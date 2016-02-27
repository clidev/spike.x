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
package io.spikex.core.util;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardWatchEventKinds;
import java.nio.file.WatchEvent.Kind;
import java.nio.file.attribute.BasicFileAttributes;
import java.nio.file.attribute.FileTime;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.vertx.java.core.Handler;
import org.vertx.java.core.eventbus.EventBus;
import org.vertx.java.core.json.JsonObject;
import org.vertx.java.platform.Verticle;

/**
 *
 * @author cli
 */
public final class VertxDirWatcher extends Verticle implements Handler<Long> {

    private Path m_dir;
    private String m_glob;
    private String m_address;
    private long m_timerId;
    private long m_iteration;
    private EventBus m_eventBus;
    private Map<Path, DirEntry> m_entries;

    public static final String CONFIG_FIELD_WATCH_DIR = "watch-dir";
    public static final String CONFIG_FIELD_WATCHER_EVENT_ADDRESS = "event-address";
    public static final String CONFIG_FIELD_GLOB_PATTERN = "glob-pattern";

    private static final String DEF_GLOB_PATTERN = "*.*";

    private final Logger m_logger = LoggerFactory.getLogger(VertxDirWatcher.class);

    @Override
    public void start() {

        m_entries = new ConcurrentHashMap();
        m_eventBus = vertx.eventBus();
        JsonObject config = container.config();

        // Sanity check
        Preconditions.checkState(config != null
                && !config.getFieldNames().isEmpty(),
                "No verticle configuration defined. Stopping.");

        m_dir = Paths.get(config.getString(CONFIG_FIELD_WATCH_DIR));
        m_address = config.getString(CONFIG_FIELD_WATCHER_EVENT_ADDRESS);
        m_glob = config.getString(CONFIG_FIELD_GLOB_PATTERN, DEF_GLOB_PATTERN);

        Preconditions.checkArgument(!Strings.isNullOrEmpty(m_address),
                "address is null or empty");

        // Pre-build entry map (if dir and files exist)
        buildEntries();

        // Start watcher timer
        m_timerId = vertx.setPeriodic(500L, this); // 500 ms
    }

    @Override
    public void stop() {
        // Stop timer
        vertx.cancelTimer(m_timerId);
    }

    @Override
    public void handle(final Long timerId) {
        //
        // Iterate through directory every n ms
        //
        long iteration = m_iteration + 1L;
        Path dir = m_dir;
        if (dir != null
                && !Strings.isNullOrEmpty(m_glob)) {
            //
            // New or modified files
            //
            try (DirectoryStream<Path> stream
                    = java.nio.file.Files.newDirectoryStream(dir, m_glob)) {

                for (Path file : stream) {
                    //
                    // Compare timestamps and hashes
                    //
                    DirEntry entry = m_entries.get(file);
                    BasicFileAttributes attrs
                            = java.nio.file.Files.readAttributes(file, BasicFileAttributes.class);
                    FileTime modtm = attrs.lastModifiedTime();
                    int hash = Files.hashOfFile(file);

                    if (entry == null) {

                        // New entry
                        entry = new DirEntry(dir, file, modtm, hash);
                        entry.created();
                        emitEvent(entry);

                    } else if (!modtm.equals(entry.getLastModifiedTime())
                            || hash != entry.getHash()) {

                        // Modified entry
                        entry.setLastModifiedTime(modtm);
                        entry.setHash(hash);
                        entry.modified();
                        emitEvent(entry);
                    }

                    entry.setIteration(iteration);
                    m_entries.put(file, entry);
                }

                //
                // Removed files
                //
                Iterator<Entry<Path, DirEntry>> iterator = m_entries.entrySet().iterator();
                while (iterator.hasNext()) {
                    Entry<Path, DirEntry> entry = iterator.next();
                    DirEntry dirEntry = entry.getValue();
                    if (dirEntry.getIteration() != iteration) {
                        dirEntry.deleted();
                        iterator.remove();
                        emitEvent(dirEntry);
                    }
                }

                m_iteration = iteration;

            } catch (IOException e) {
                m_logger.error("Failed to iterate through directory: {}", m_dir, e);
            }
        }
    }

    private void buildEntries() {
        Path dir = m_dir;
        try (DirectoryStream<Path> stream
                = java.nio.file.Files.newDirectoryStream(dir, m_glob)) {

            for (Path file : stream) {
                BasicFileAttributes attrs
                        = java.nio.file.Files.readAttributes(file, BasicFileAttributes.class);
                FileTime modtm = attrs.lastModifiedTime();
                int hash = Files.hashOfFile(file);
                m_entries.put(file, new DirEntry(dir, file, modtm, hash));
            }
        } catch (IOException e) {
            m_logger.error("Failed to build directory entry map", e);
        }
    }

    private void emitEvent(final DirEntry entry) {
        m_eventBus.publish(m_address, entry.toJson());
    }

    private static final class DirEntry {

        private final Path m_base;
        private final Path m_file;
        private Kind m_kind;
        private int m_hash;
        private FileTime m_modtm;
        private long m_iteration;

        private DirEntry(
                final Path base,
                final Path file,
                final FileTime modtm,
                final int hash) {

            m_base = base;
            m_file = file;
            m_modtm = modtm;
            m_hash = hash;
        }

        private FileTime getLastModifiedTime() {
            return m_modtm;
        }

        private int getHash() {
            return m_hash;
        }

        private Kind getKind() {
            return m_kind;
        }

        private Path getBase() {
            return m_base;
        }

        private Path getFile() {
            return m_file;
        }

        private long getIteration() {
            return m_iteration;
        }

        private void setLastModifiedTime(final FileTime modtm) {
            m_modtm = modtm;
            m_kind = StandardWatchEventKinds.ENTRY_MODIFY;
        }

        private void setHash(final int hash) {
            m_hash = hash;
        }

        private void setIteration(final long iteration) {
            m_iteration = iteration;
        }

        private void created() {
            m_kind = StandardWatchEventKinds.ENTRY_CREATE;
        }

        private void deleted() {
            m_kind = StandardWatchEventKinds.ENTRY_DELETE;
        }

        private void modified() {
            m_kind = StandardWatchEventKinds.ENTRY_MODIFY;
        }

        private JsonObject toJson() {
            DirEvent event = new DirEvent(
                    getBase(),
                    getFile(),
                    getKind(),
                    getHash(),
                    getLastModifiedTime()
            );
            return event.toJson();
        }
    }
}
