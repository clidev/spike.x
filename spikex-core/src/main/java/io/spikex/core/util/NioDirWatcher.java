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

import com.google.common.eventbus.EventBus;
import com.sun.nio.file.SensitivityWatchEventModifier;
import java.io.IOException;
import java.nio.file.FileSystems;
import java.nio.file.Path;
import static java.nio.file.StandardWatchEventKinds.OVERFLOW;
import java.nio.file.WatchEvent;
import java.nio.file.WatchEvent.Kind;
import java.nio.file.WatchKey;
import java.nio.file.WatchService;
import java.util.concurrent.atomic.AtomicInteger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author cli
 */
public final class NioDirWatcher {

    private final EventBus m_eventBus; // Used for resource events
    private WatcherThread m_watcher;

    private static final AtomicInteger m_count = new AtomicInteger();

    private final Logger m_logger = LoggerFactory.getLogger(NioDirWatcher.class);

    public NioDirWatcher() {
        m_eventBus = new EventBus(NioDirWatcher.class.getSimpleName()
                + "-" + m_count.getAndIncrement());
    }

    public void watch(
            final Path dir,
            final Kind<Path>[] kinds) throws IOException {

        WatchService service = FileSystems.getDefault().newWatchService();
        // Rationale for SensitivityWatchEventModifier.HIGH: 
        // http://stackoverflow.com/questions/9588737/is-java-7-watchservice-slow-for-anyone-else
        dir.register(
                service,
                kinds,
                SensitivityWatchEventModifier.HIGH);

        m_logger.info("Watching directory: {}", dir);
        m_watcher = new WatcherThread(service, m_eventBus);
        m_watcher.start(); // Start watcher thread
    }

    public void close() {
        m_watcher.close();
    }

    public void register(final Object listener) {
        m_eventBus.register(listener);
    }

    public void unregister(final Object listener) {
        m_eventBus.unregister(listener);
    }

    private static final class WatcherThread extends Thread {

        private final WatchService m_service;
        private final EventBus m_eventBus;

        private final Logger m_logger = LoggerFactory.getLogger(WatcherThread.class);

        public WatcherThread(
                final WatchService service,
                final EventBus eventBus) {

            super("WatcherThread-" + m_count.getAndIncrement());
            m_service = service;
            m_eventBus = eventBus;
        }

        @Override
        public void run() {
            while (true) {

                WatchKey key;
                try {
                    key = m_service.take();
                } catch (InterruptedException e) {
                    m_logger.warn("Directory watcher interrupted", e);
                    break;
                }

                for (final WatchEvent<?> event : key.pollEvents()) {
                    Kind kind = event.kind();
                    if (kind != OVERFLOW) {
                        m_eventBus.post(event);
                    } else {
                        m_logger.trace("Overflow");
                    }
                }

                // Reset key and stop watching if dir removed (or other problem)
                if (!key.reset()) {
                    break;
                }
            }
            m_logger.debug("Directory watcher stopped");
        }

        public void close() {
            try {
                m_service.close();
            } catch (IOException e) {
                m_logger.error("Failed to close directory watch service", e);
            }
        }
    }
}
