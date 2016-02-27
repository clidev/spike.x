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
package io.spikex.notifier;

import com.google.common.base.Strings;
import com.google.common.eventbus.Subscribe;
import io.spikex.core.AbstractActivator;
import io.spikex.core.helper.Commands;
import io.spikex.core.util.NioDirWatcher;
import io.spikex.notifier.NotifierConfig.DatabaseDef;
import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.StandardWatchEventKinds;
import java.nio.file.WatchEvent;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import org.mapdb.DB;
import org.mapdb.DBMaker;
import org.mapdb.Serializer;
import org.vertx.java.core.Handler;
import org.vertx.java.core.eventbus.Message;
import org.vertx.java.core.json.JsonArray;
import org.vertx.java.core.json.JsonObject;

/**
 *
 * @author cli
 */
public final class Activator extends AbstractActivator {

    // Last time we did a re-init
    private long m_lastInitTm;

    // Notifier and its configuration
    private Notifier m_notifier;
    private NotifierConfig m_config;

    // Deployment ID of dispatcher
    private String m_deploymentId;

    // Notification database
    private DB m_db;

    // Dispatching timer
    private long m_timerId;

    // Dispatcher address
    private static final String DISPATCHER_LOCAL_ADDRESS = Dispatcher.class.getName();

    public Activator() {
        m_deploymentId = "";
        m_timerId = -1;
    }

    @Override
    protected void startVerticle() {

        m_config = new NotifierConfig(confPath());

        try {
            m_config.load();
        } catch (Exception e) {
            throw new IllegalStateException("Failed to load notifier configuration", e);
        }

        //
        // Initialize local database
        //
        try {
            DatabaseDef dbDef = m_config.getDatabaseDef();
            String dbPassword = dbDef.getPassword();
            File dbFile = new File(dataPath().toFile(), dbDef.getName());
            m_db = openDatabase(dbFile, dbPassword);

            // Create queue (if not created already)
            if (!m_db.exists(NotifierConfig.queueName())) {
                m_db.createQueue(
                        NotifierConfig.queueName(),
                        Serializer.STRING,
                        false);
                m_db.commit();
            }

            // Compact on startup by default
            if (m_config.getDatabaseDef().getCompact()) {
                compactDatabase(m_db);
            }

        } catch (Exception e) {
            throw new IllegalStateException("Failed to initialize notification database", e);
        }

        //
        // Create and initialize notifier
        //
        createAndInitNotifier();

        try {

            //
            // Watch configuration
            //
            m_lastInitTm = System.currentTimeMillis();
            NioDirWatcher watcher = new NioDirWatcher();
            watcher.register(this);
            watcher.watch(confPath(),
                    new WatchEvent.Kind[]{
                        StandardWatchEventKinds.ENTRY_CREATE,
                        StandardWatchEventKinds.ENTRY_MODIFY
                    });

        } catch (IOException e) {
            throw new IllegalStateException("Failed to watch directory: "
                    + confPath(), e);
        }
    }

    @Override
    protected void stopVerticle() {
        // Close database
        closeDatabase(m_db);
    }

    @Override
    protected void initVerticle() {

        try {
            // Load configuration, deploy dispatcher and create notifier
            if (m_config.hasChanged()) {
                m_config.load();
                if (m_config.isEmpty()) {
                    logger().error("Empty configuration file: {}",
                            m_config.getYaml().getQualifiedName());
                } else {
                    //
                    // Deploy dispatcher
                    //
                    final String verticle = Dispatcher.class.getName();
                    if (!Strings.isNullOrEmpty(m_deploymentId)) {
                        undeployDispatcher(
                                m_deploymentId,
                                new UndeploymentHandler(verticle) {
                                    @Override
                                    public void handleSuccess() {
                                        deployDispatcher(
                                                m_config,
                                                new DeploymentHandler(verticle) {
                                                    @Override
                                                    public void handleSuccess(final String deploymentId) {
                                                        m_deploymentId = deploymentId;
                                                    }
                                                });
                                    }
                                });
                    } else {
                        deployDispatcher(
                                m_config,
                                new DeploymentHandler(verticle) {
                                    @Override
                                    public void handleSuccess(final String deploymentId) {
                                        m_deploymentId = deploymentId;
                                    }
                                });
                    }
                    //
                    // Create and initialize notifier
                    //
                    createAndInitNotifier();
                }
            }
        } catch (Exception e) {
            logger().error("Failed to initialize notifier", e);
        }
    }

    @Subscribe
    public void handleConfChanged(final WatchEvent event) {

        try {

            // Throttle re-init (5 sec)
            long nowTm = System.currentTimeMillis();
            long lastTm = m_lastInitTm;
            if ((nowTm - lastTm) > 5000L) {
                m_lastInitTm = nowTm;
                Path filePath = (Path) event.context();
                logger().trace("Got file changed event: {}", filePath);
                if (filePath != null
                        && filePath.getFileName().toString().startsWith(m_config.getName())) {
                    String filename = filePath.getFileName().toString();
                    logger().info("Configuration file modified: {}", filename);
                    Commands.call(
                            eventBus(),
                            localAddress(),
                            Commands.cmdInitVerticle());
                }
            }
        } catch (Exception e) {
            logger().error("Failed to handle configuration change", e);
        }
    }

    private void dispatchEvents() {
        //
        // Read next batch of events from queue (if any)
        //
        if (!m_db.isClosed()) {

            BlockingQueue<String> queue = m_db.getQueue(NotifierConfig.queueName());
            List<String> events = new ArrayList();
            queue.drainTo(events, m_config.getDispatcherBatchSize());

            JsonArray jsonEvents = new JsonArray();
            for (String json : events) {

                JsonObject event = new JsonObject(json);
                jsonEvents.add(event);
            }

            // Send events to dispatcher
            eventBus().send(DISPATCHER_LOCAL_ADDRESS, jsonEvents);
        }
    }

    private void undeployDispatcher(
            final String deploymentId,
            final UndeploymentHandler handler) {

        logger().info("Undeploying dispatcher ({})", deploymentId);

        Commands.call(
                eventBus(),
                localAddress(),
                Commands.cmdUndeployVerticle(deploymentId),
                handler);
    }

    private void deployDispatcher(
            final NotifierConfig config,
            final Handler<Message<JsonObject>> handler) {

        logger().info("Deploying dispatcher");

        JsonObject dispatcherConfig = new JsonObject();

        // Dispatcher local address
        dispatcherConfig.putString(CONF_KEY_LOCAL_ADDRESS, DISPATCHER_LOCAL_ADDRESS);

        Commands.call(
                eventBus(),
                localAddress(),
                Commands.cmdDeployVerticle(
                        Dispatcher.class.getName(),
                        dispatcherConfig,
                        1,
                        false,
                        true), // Dispatcher is a worker
                handler);
    }

    private DB openDatabase(
            final File file,
            final String password) {

        return DBMaker.newFileDB(file)
                .closeOnJvmShutdown()
                .checksumEnable()
                .compressionEnable()
                .encryptionEnable(password)
                .transactionDisable()
                .make();
    }

    private void closeDatabase(final DB database) {
        // Close database
        if (database != null
                && !database.isClosed()) {
            database.commit();
            database.close();
        }
    }

    private void compactDatabase(final DB database) {
        if (database != null
                && !database.isClosed()) {
            logger().debug("Compacting notifier buffer database");
            database.compact();
        }
    }

    private void createAndInitNotifier() {
        //
        // Unregister local listener and stop notifier
        //
        if (m_notifier != null) {
            m_notifier.stop(eventBus());
            //
            // Compact database...
            //
            compactDatabase(m_db);
        }
        //
        // Create notifier (and possibly listen on another address)
        //
        m_notifier = new Notifier(
                m_db,
                m_config,
                variables(),
                confPath(),
                config().getString(CONF_KEY_USER));

        m_notifier.start(eventBus());
        //
        // Re-start timer
        //
        if (m_timerId != -1) {
            vertx.cancelTimer(m_timerId);
        }
        long interval = m_config.getDispatcherInterval();
        logger().info("Starting dispatcher timer with interval: {}", interval);
        m_timerId = vertx.setPeriodic(
                interval,
                new Handler<Long>() {

                    @Override
                    public void handle(final Long timerId) {
                        dispatchEvents();
                    }
                });
    }
}
