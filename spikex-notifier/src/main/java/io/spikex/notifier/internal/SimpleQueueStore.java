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
package io.spikex.notifier.internal;

import com.hazelcast.core.QueueStore;
import static io.spikex.core.util.Files.Permission.OWNER_FULL_GROUP_EXEC;
import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.vertx.java.core.json.JsonObject;

/**
 *
 * @author cli
 */
public final class SimpleQueueStore implements QueueStore<JsonObject> {

    private final Path m_dataPath;
    private final String m_user;

    private static final String STORE_DIR = "notification-queue";
    private static final String FILE_SUFFIX = ".json";
    private static final String FILE_SUFFIX_GLOB = "*" + FILE_SUFFIX;

    private final Logger m_logger = LoggerFactory.getLogger(SimpleQueueStore.class);

    public SimpleQueueStore(
            final Path dataPath,
            final String user) {

        m_dataPath = dataPath;
        m_user = user;
    }

    @Override
    public void store(
            final Long key,
            final JsonObject event) {

        saveToFile(key, event);
    }

    @Override
    public void storeAll(final Map<Long, JsonObject> events) {
        Iterator iterator = events.entrySet().iterator();
        while (iterator.hasNext()) {
            Map.Entry<Long, JsonObject> entry = (Map.Entry) iterator.next();
            store(entry.getKey(), entry.getValue());
        }
    }

    @Override
    public void delete(final Long key) {
        try {
            Path path = m_dataPath.resolve(STORE_DIR);
            m_logger.debug("Deleting notification: {}", path);
            Files.deleteIfExists(path.resolve(resolveFilename(key)));
        } catch (IOException e) {
            m_logger.error("Failed to remove notification (key: {})", key, e);
        }
    }

    @Override
    public void deleteAll(final Collection<Long> keys) {
        for (Long key : keys) {
            delete(key);
        }
    }

    @Override
    public JsonObject load(final Long key) {
        return loadFromFile(key);
    }

    @Override
    public Map<Long, JsonObject> loadAll(final Collection<Long> keys) {
        Map<Long, JsonObject> events = new HashMap();
        for (Long key : keys) {
            events.put(key, loadFromFile(key));
        }
        return events;
    }

    @Override
    public Set<Long> loadAllKeys() {
        Set<Long> keys = new HashSet();
        Path path = m_dataPath.resolve(STORE_DIR);
        try (DirectoryStream<Path> stream
                = java.nio.file.Files.newDirectoryStream(path, FILE_SUFFIX_GLOB)) {
            for (Path file : stream) {
                String filename = file.getFileName().toString();
                String key = filename.substring(0, filename.indexOf('.'));
                loadFromFile(Long.valueOf(key));
            }
        } catch (IOException e) {
            throw new RuntimeException("Failed to load notification", e);
        }
        return keys;
    }

    private JsonObject loadFromFile(final Long key) {

        JsonObject value = null;

        try {
            Path path = m_dataPath.resolve(STORE_DIR);
            if (path.toFile().exists()) {
                Path file = path.resolve(resolveFilename(key));
                byte[] data = Files.readAllBytes(file);
                m_logger.debug("Loaded notification: {}", file);
                String json = new String(data); // UTF-8
                value = new JsonObject(json);
            }
        } catch (IOException e) {
            m_logger.error("Failed to load notification (key: {})", key, e);
        }

        return value;
    }

    private void saveToFile(
            final Long key,
            final JsonObject event) {

        try {
            // Create dir if it doesn't exist
            Path path = m_dataPath.resolve(STORE_DIR);
            if (!path.toFile().exists()) {
                io.spikex.core.util.Files.createDirectories(
                        m_user,
                        OWNER_FULL_GROUP_EXEC,
                        path);
            }
            //
            Path file = path.resolve(resolveFilename(key));
            Files.write(file, event.encode().getBytes()); // UTF-8
            m_logger.debug("Saved notification: {}", file);
        } catch (IOException e) {
            m_logger.error("Failed to save \"{}\" notification (key: {})",
                    event.encode(), key, e);
        }
    }

    private String resolveFilename(final Long key) {
        StringBuilder filename = new StringBuilder();
        filename.append(key); // Number
        filename.append(FILE_SUFFIX);
        return filename.toString();
    }
}
