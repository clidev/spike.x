/**
 *
 * Copyright (c) 2015 NG Modular Oy.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */
package io.spikex.core.integration;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardWatchEventKinds;
import java.nio.file.WatchEvent.Kind;
import java.util.concurrent.atomic.AtomicInteger;
import junit.framework.Assert;
import io.spikex.core.util.DirEvent;
import io.spikex.core.util.VertxDirWatcher;
import static io.spikex.core.util.VertxDirWatcher.CONFIG_FIELD_GLOB_PATTERN;
import static io.spikex.core.util.VertxDirWatcher.CONFIG_FIELD_WATCHER_EVENT_ADDRESS;
import static io.spikex.core.util.VertxDirWatcher.CONFIG_FIELD_WATCH_DIR;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.vertx.java.core.AsyncResult;
import org.vertx.java.core.AsyncResultHandler;
import org.vertx.java.core.Handler;
import org.vertx.java.core.eventbus.Message;
import org.vertx.java.core.json.JsonObject;
import org.vertx.testtools.TestVerticle;
import org.vertx.testtools.VertxAssert;

/**
 *
 * @author cli
 */
public class VertxDirWatcherTest extends TestVerticle {

    private static final String EVENT_ADDRESS = "FILE_WATCH_EVENTS";
    private static final String BASE_DIR = "build/files";

    private final Logger m_logger = LoggerFactory.getLogger(VertxDirWatcherTest.class);

    @Test
    public void testWatcher() throws IOException {

        final int FILE_COUNT = 20;
        final AtomicInteger counter = new AtomicInteger(FILE_COUNT * 3);

        // Receive watch events
        vertx.eventBus().registerLocalHandler(EVENT_ADDRESS, new Handler<Message<JsonObject>>() {

            @Override
            public void handle(final Message<JsonObject> event) {

                JsonObject evn = event.body();
                m_logger.info("Received event: {}", evn);

                try {
                    DirEvent dirEvent = new DirEvent(evn);
                    Kind kind = dirEvent.getKind();
                    Path file = dirEvent.getFile();
                    if (StandardWatchEventKinds.ENTRY_CREATE.equals(kind)) {
                        modifyFile(file);
                    } else if (StandardWatchEventKinds.ENTRY_MODIFY.equals(kind)) {
                        Files.deleteIfExists(file);
                    }
                } catch (IOException e) {
                    m_logger.error("Failed to remove file", e);
                }

                if (counter.decrementAndGet() <= 0) {
                    // Stop test
                    VertxAssert.testComplete();
                }
            }
        });

        // Start watching directory
        final Path base = Paths.get(BASE_DIR).toAbsolutePath();
        Files.createDirectories(base);

        JsonObject config = new JsonObject();
        config.putString(CONFIG_FIELD_WATCH_DIR, base.toString());
        config.putString(CONFIG_FIELD_WATCHER_EVENT_ADDRESS, EVENT_ADDRESS);
        config.putString(CONFIG_FIELD_GLOB_PATTERN, "*.txt");

        container.deployWorkerVerticle(VertxDirWatcher.class.getName(), config, 1, false,
                new AsyncResultHandler<String>() {
                    @Override
                    public void handle(final AsyncResult<String> ar) {
                        if (ar.failed()) {
                            m_logger.error("Failed to deploy verticle", ar.cause());
                            Assert.fail();
                        } else {
                            try {
                                // Create new files
                                for (int i = 0; i < FILE_COUNT; i++) {
                                    Path file1 = base.resolve("test-" + i + ".txt");
                                    Path file2 = base.resolve("test-" + i + ".dat");
                                    java.nio.file.Files.deleteIfExists(file1);
                                    java.nio.file.Files.deleteIfExists(file2);
                                    Files.createFile(file1);
                                    Files.createFile(file2);
                                }
                            } catch (IOException e) {
                                m_logger.error("Failed to create or remove files", e);
                            }
                        }
                    }
                });

    }

    private void modifyFile(final Path file) {
        try (PrintWriter out = new PrintWriter(new BufferedWriter(new FileWriter(file.toFile(), true)))) {
            out.println("modified");
        } catch (IOException e) {
            m_logger.error("Failed to modify file", e);
        }
    }
}
