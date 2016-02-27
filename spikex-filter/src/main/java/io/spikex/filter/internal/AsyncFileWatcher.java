/**
 *
 * Copyright (c) 2015 NG Modular Oy.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 *
 */
package io.spikex.filter.internal;

import static io.spikex.filter.internal.VertxFileHandler.EVENT_ADDRESS;
import static io.spikex.filter.internal.VertxFileHandler.EVENT_FIELD_CUR_SIZE;
import static io.spikex.filter.internal.VertxFileHandler.EVENT_FIELD_OLD_SIZE;
import static io.spikex.filter.internal.VertxFileHandler.EVENT_FIELD_PATH;
import static io.spikex.filter.internal.VertxFileHandler.EVENT_FIELD_ROLLOVER;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.vertx.java.core.AsyncResult;
import org.vertx.java.core.AsyncResultHandler;
import org.vertx.java.core.Handler;
import org.vertx.java.core.eventbus.EventBus;
import org.vertx.java.core.eventbus.Message;
import org.vertx.java.core.file.FileProps;
import org.vertx.java.core.file.FileSystem;
import org.vertx.java.core.json.JsonObject;

/**
 * Uses Vert.x API to watch for directory changes.
 *
 * @author cli
 */
public final class AsyncFileWatcher implements Handler<Long> {

    private final EventBus m_eventBus;
    private final FileSystem m_fileSystem;
    private final List<Path> m_paths;
    private final Map<Path, FilePointer> m_pointers;
    private final boolean m_readChunks;
    private final int m_maxReadSize;

    private final Logger m_logger = LoggerFactory.getLogger(AsyncFileWatcher.class);

    // Simple rate limiting for errors...
    private long m_tm1 = 0L;
    private long m_tm2 = 0L;
    private int m_errCount = 0;
    private static final int ERROR_SKIP_COUNT = 25;
    private static final long ERROR_INTERVAL_MS = 1000L * 60L * 5L; // 5 min

    public AsyncFileWatcher(
            final EventBus eventBus,
            final FileSystem fileSystem,
            final List<Path> paths,
            final boolean readChunks,
            final int maxReadSize) {

        m_eventBus = eventBus;
        m_fileSystem = fileSystem;
        m_paths = paths;
        m_pointers = new ConcurrentHashMap();
        m_readChunks = readChunks;
        m_maxReadSize = maxReadSize;

        // Populate pointers
        for (Path path : paths) {
            m_pointers.put(path, new FilePointer(null, 0L));
        }
    }

    @Override
    public void handle(final Long timerId) {

        final FileSystem fs = m_fileSystem;
        final List<Path> paths = m_paths; // file paths

        for (final Path path : paths) {

            m_logger.trace("Inspecting file: {}", path);

            // Check that file exists before proceeding further...
            if (Files.exists(path)) {

                fs.props(path.toString(), new AsyncResultHandler<FileProps>() {

                    @Override
                    public void handle(final AsyncResult<FileProps> ar) {
                        handleFileProps(ar, path);
                    }
                });
            }
        }
    }

    private void handleFileProps(
            final AsyncResult<FileProps> ar,
            final Path path) {

        if (ar.succeeded()) {
            final FilePointer fp = m_pointers.remove(path); // Reserve pointer
            if (fp != null) {

                //
                // Latest file properties
                //
                FileProps props = ar.result();
                Date curModDate = props.lastModifiedTime(); // Date
                long curTm = curModDate.getTime(); // Current modification time
                long curSize = props.size(); // Current file size

                if (fp.isNew()) {
                    fp.setFileProps(props);
                }

                //
                // Previous file properties
                //
                FileProps oldProps = fp.getFileProps();
                long oldBytes = fp.getFirstBytes();
                Date oldModDate = oldProps.lastModifiedTime(); // Date
                long oldTm = oldModDate.getTime();
                long oldSize = oldProps.size();

                //
                // Read in chunks
                //
                if (m_readChunks) {
                    curSize = fp.nextSize(curSize, m_maxReadSize);
                }

                // Read first 8 bytes of file and compare to last time
                //
                // We must always do this, since file props might be updated before 
                // file has actually changed...
                // ... and reading the 8 first bytes doesn't cost too much
                String pathStr = path.toString();
                ByteBuffer buf = ByteBuffer.allocate(8);
                boolean success = readFirstBytes(pathStr, buf);

                if (success) {
                    m_errCount = 0;
                    buf.rewind();
                    long curBytes = buf.getLong();

                    // Update state
                    fp.setFileProps(props);
                    fp.setFirstBytes(curBytes);

                    m_logger.trace("Current mod tm: {} size: {} - old tm: {} size: {}",
                            curTm, curSize, oldTm, oldSize);

                    // Emit handler event if file has changed
                    if (curTm != oldTm
                            || curSize != oldSize
                            || curBytes != oldBytes) {

                        JsonObject event = new JsonObject();
                        event.putString(EVENT_FIELD_PATH, path.toString());
                        event.putBoolean(EVENT_FIELD_ROLLOVER, (curBytes != oldBytes));
                        event.putNumber(EVENT_FIELD_CUR_SIZE, curSize);
                        event.putNumber(EVENT_FIELD_OLD_SIZE, oldSize);
                        m_eventBus.send(EVENT_ADDRESS, event, new Handler<Message<Void>>() {

                            @Override
                            public void handle(Message<Void> message) {
                                m_logger.trace("Release pointer (1): {}", path);
                                m_pointers.put(path, fp); // Release pointer
                            }
                        });
                    } else {
                        m_logger.trace("Release pointer (2): {}", path);
                        m_pointers.put(path, fp); // Release pointer
                    }
                } else {
                    m_logger.trace("Release pointer (3): {}", path);
                    m_pointers.put(path, fp); // Release pointer
                }
            }
        } else {
            long tm = System.currentTimeMillis();
            if ((tm - m_tm1) > ERROR_INTERVAL_MS) {
                m_logger.error("Failed to retrieve file properties", ar.cause());
                m_tm1 = tm;
            }
        }
    }

    private boolean readFirstBytes(
            final String path,
            final ByteBuffer buf) {

        boolean success = false;
        FileChannel channel = null;
        RandomAccessFile raf = null;

        try {

            raf = new RandomAccessFile(path, "r");
            channel = raf.getChannel();
            channel.read(buf);
            success = true;

        } catch (IOException e) {
            // Might be normal, since the file might be unavailable during rollover...
            long tm = System.currentTimeMillis();
            if ((m_errCount++ > ERROR_SKIP_COUNT)
                    && ((tm - m_tm2) > ERROR_INTERVAL_MS)) {
                m_logger.error("Failed to read first 8 bytes of file: {}",
                        path, e);
                m_tm2 = tm;
            }
        } finally {
            try {
                if (channel != null) {
                    channel.close();
                }
            } catch (IOException e) {
                long tm = System.currentTimeMillis();
                if ((tm - m_tm2) > ERROR_INTERVAL_MS) {
                    m_logger.error("Failed to close file channel for: {}",
                            path, e);
                    m_tm2 = tm;
                }
            }
            try {
                if (raf != null) {
                    raf.close();
                }
            } catch (IOException e) {
                long tm = System.currentTimeMillis();
                if ((tm - m_tm2) > ERROR_INTERVAL_MS) {
                    m_logger.error("Failed to close file handle for: {}",
                            path, e);
                    m_tm2 = tm;
                }
            }
        }
        return success;
    }
}
