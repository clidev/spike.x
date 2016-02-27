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

import io.spikex.core.AbstractFilter;
import io.spikex.core.helper.Events;
import static io.spikex.core.helper.Events.EVENT_PRIORITY_NORMAL;
import io.spikex.core.util.HostOs;
import io.spikex.core.util.StringTokenizer;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.vertx.java.core.AsyncResult;
import org.vertx.java.core.AsyncResultHandler;
import org.vertx.java.core.Handler;
import org.vertx.java.core.buffer.Buffer;
import org.vertx.java.core.eventbus.EventBus;
import org.vertx.java.core.eventbus.Message;
import org.vertx.java.core.file.AsyncFile;
import org.vertx.java.core.file.FileSystem;
import org.vertx.java.core.json.JsonObject;

/**
 * Asynchronous non-locking reading of file and emitting of lines.
 *
 * @author cli
 */
public final class VertxFileHandler implements Handler<Message<JsonObject>> {

    public static final String EVENT_ADDRESS = "-spikex-filter-tail-file";
    public static final String EVENT_FIELD_PATH = "path";
    public static final String EVENT_FIELD_ROLLOVER = "rollover";
    public static final String EVENT_FIELD_CUR_SIZE = "cursize";
    public static final String EVENT_FIELD_OLD_SIZE = "oldsize";

    // Minimal buffer size
    private final int m_minReadSize;

    // Maximal buffer size
    private final int m_maxReadSize;

    // Start at EOF
    private boolean m_startAtEof;

    private final AbstractFilter m_filter;
    private final EventBus m_eventBus;
    private final String m_encoding;
    private final String m_delimiter;
    private final Map<String, Long> m_offset;

    // Simple rate limiting for errors...
    private long m_tm1 = 0L;
    private long m_tm2 = 0L;
    private long m_tm3 = 0L;
    private int m_errCount1 = 0;
    private int m_errCount2 = 0;
    private int m_errCount3 = 0;
    private static final long ERROR_INTERVAL_MS = 1000L * 60L * 5L; // 5 min

    private final Logger m_logger = LoggerFactory.getLogger(getClass());

    public VertxFileHandler(
            final AbstractFilter filter,
            final EventBus eventBus,
            final String encoding,
            final String delimiter,
            final int minReadSize,
            final int maxReadSize,
            final boolean startAtEof) {

        m_filter = filter;
        m_eventBus = eventBus;
        m_encoding = encoding;
        m_delimiter = delimiter;
        m_minReadSize = minReadSize;
        m_maxReadSize = maxReadSize;
        m_startAtEof = startAtEof;
        m_offset = new ConcurrentHashMap();
    }

    @Override
    public void handle(final Message<JsonObject> message) {

        JsonObject event = message.body();
        final String path = event.getString(EVENT_FIELD_PATH);
        final boolean rollover = event.getBoolean(EVENT_FIELD_ROLLOVER);
        final long curSize = event.getLong(EVENT_FIELD_CUR_SIZE);
        final long oldSize = event.getLong(EVENT_FIELD_OLD_SIZE);

        Long offsetTmp = m_offset.get(path);
        if (offsetTmp == null) {
            if (m_startAtEof) {
                offsetTmp = curSize; // start at EOF
            } else {
                offsetTmp = 0L; // start at BOF
            }
            m_offset.put(path, offsetTmp);
        }
        long offset = offsetTmp;

        //
        // Determine if the file has actually changed
        //
        // All kinds of silly scenarios:
        //
        // 1. Lines have been appended 
        //      - modified tm changed and size has increased
        // 2. File has been rolled over to the exact same size 
        //      - offset is reset if first line has changed
        // 3. File has been rolled over - normal logging speed
        //      - modification tm changed and size is smaller
        // 4. File has been rolled over - crazy logging speed
        //      - modification tm changed and size is same or bigger
        //      - offset is reset if first line has changed
        // 5. File has changed, modification tm has not yet changed, but size has
        //      - handled by previous cases...
        //
        m_logger.trace("Current size: {} offset: {} diff: {}", curSize, offset,
                Math.abs(curSize - offset));
        // Read lines if file is big enough (>32 bytes)
        // and we can read enough bytes (>= read buffer size)
        if (curSize >= 32
                && Math.abs(curSize - offset) >= m_minReadSize) {

            if (rollover) {
                // Update state
                offset = 0;
                m_offset.put(path, offset);
                m_logger.trace("First bytes of file changed");
            } else {
                // Reset offset if offset is larger than file size
                if (offset > curSize) {
                    offset = 0;
                    m_logger.trace("Offset reset");
                }
            }

            // Yes, tail file if size or first bytes changed
            // Jump over this round if rollover since cursize might be the old size
            final long curOffset = offset;
            if (curSize > 0
                    && (curSize != oldSize || curOffset == 0)
                    && !rollover) {
                // The flush flag is required at least on Windows
                // Linux and FreeBSD work fine with or without it...
                // Internally it is translated to StandardOpenOption.DSYNC in Vert.x
                fileSystem().open(path, "r--r--r--", true, false, false, true, new AsyncResultHandler<AsyncFile>() {

                    @Override
                    public void handle(AsyncResult<AsyncFile> ar) {
                        if (ar.succeeded()) {
                            m_errCount1 = 0;
                            handleFileOpen(message, ar.result(), path, curOffset, curSize);
                        } else {
                            message.reply(); // Ready
                            long tm = System.currentTimeMillis();
                            if ((m_errCount1++ > 25)
                                    && ((tm - m_tm1) > ERROR_INTERVAL_MS)) {
                                m_logger.error("Failed to open file: {}", path, ar.cause());
                                m_tm1 = tm;
                            }
                        }
                    }
                });
            } else {
                message.reply(); // Ready
            }
        } else {
            message.reply(); // Ready
        }
    }

    private void handleFileOpen(
            final Message<JsonObject> message,
            final AsyncFile file,
            final String path,
            final long offset,
            final long size) {

        final AbstractFilter filter = m_filter;
        final int maxReadSize = m_maxReadSize;
        int len = (int) (size - offset);

        if (len > 0) {

            // Prevent uncontrolled memory consumption
            if (len > (maxReadSize * 2)) {
                m_logger.warn("Too much input data. Skipping {} bytes in file: {}",
                        (len - maxReadSize), path);
                len = maxReadSize;
            }
            m_logger.trace("Reading from offset: {} len: {} file: {}",
                    offset, len, path);

            final Buffer buf = new Buffer(len);
            // Read max 1 MB by default
            file.read(buf, 0, offset, len, new AsyncResultHandler<Buffer>() {

                @Override
                public void handle(final AsyncResult<Buffer> ar) {

                    if (ar.succeeded()) {
                        // Reset error count state 
                        // Too many consecutive errors is considered abnormal
                        m_errCount2 = 0;

                        Buffer buf = ar.result();
                        String raw = buf.toString(m_encoding);
                        m_offset.put(path, size); // Update offset

                        int len = buf.length();
                        m_logger.trace("Read {} bytes", len);

                        if (raw.length() > 0) {
                            try {
                                String[] lines = StringTokenizer.tokenize(raw, m_delimiter);
                                int remainingBytes = StringTokenizer.remainingBytes();

                                // Re-read bytes that are part of last "half" line
                                if (remainingBytes > 0) {
                                    m_offset.put(path, size - remainingBytes); // Update offset
                                }

                                for (String line : lines) {

                                    // Do not emit empty lines
                                    if (line.length() > 0) {
                                        JsonObject event = Events.createNotificationEvent(
                                                filter,
                                                HostOs.hostName(),
                                                EVENT_PRIORITY_NORMAL,
                                                "Log line event",
                                                line);
                                        m_eventBus.send(filter.getDestinationAddress(), event);
                                    }
                                }
                                m_errCount3 = 0;
                            } catch (Exception e) {
                                long tm = System.currentTimeMillis();
                                if ((m_errCount3++ > 25)
                                        && ((tm - m_tm3) > ERROR_INTERVAL_MS)) {
                                    m_logger.error("Failed to tokenize or emit line", e);
                                    m_tm3 = tm;
                                }
                            }
                        }

                        // Try to close file after read...
                        file.close(new AsyncResultHandler<Void>() {

                            @Override
                            public void handle(final AsyncResult<Void> ar) {

                                if (ar.failed()) {
                                    long tm = System.currentTimeMillis();
                                    if ((tm - m_tm2) > ERROR_INTERVAL_MS) {
                                        m_logger.error("Failed to close file: {}",
                                                path, ar.cause());
                                        m_tm2 = tm;
                                    }
                                } else {
                                    m_logger.trace("Closed: {}", path);
                                }
                                message.reply(); // Ready
                            }
                        });

                    } else {
                        message.reply(); // Ready
                        long tm = System.currentTimeMillis();
                        if ((m_errCount2++ > 25)
                                && ((tm - m_tm2) > ERROR_INTERVAL_MS)) {
                            m_logger.error("Failed to read file: {}", path,
                                    ar.cause());
                            m_tm2 = tm;
                        }
                    }
                }
            });
        } else {
            m_logger.warn("Negative or zero input size. Resetting file offset pointer to: {}",
                    size);
            m_offset.put(path, size); // Update offset
            message.reply(); // Ready
        }
    }

    /**
     * Returns the Vert.x file system object.
     *
     * @return Vert.x file system
     */
    private FileSystem fileSystem() {
        return m_filter.getVertx().fileSystem();
    }
}
