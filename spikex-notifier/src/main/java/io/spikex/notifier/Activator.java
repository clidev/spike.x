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
import com.hazelcast.config.Config;
import com.hazelcast.config.EvictionPolicy;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.config.MapConfig;
import com.hazelcast.config.MaxSizeConfig;
import com.hazelcast.config.MaxSizeConfig.MaxSizePolicy;
import com.hazelcast.config.QueueConfig;
import com.hazelcast.config.QueueStoreConfig;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.core.IQueue;
import io.spikex.core.AbstractActivator;
import io.spikex.core.helper.Commands;
import io.spikex.core.util.NioDirWatcher;
import static io.spikex.notifier.NotifierConfig.CONF_KEY_QUEUE_BULK_LOAD;
import static io.spikex.notifier.NotifierConfig.CONF_KEY_QUEUE_MEMORY_LIMIT;
import io.spikex.notifier.internal.HzEventListener;
import io.spikex.notifier.internal.SimpleQueueStore;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.StandardWatchEventKinds;
import java.nio.file.WatchEvent;
import java.util.ArrayList;
import java.util.List;
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

    // Distributed notification queue (persisted to disk)
    private IQueue<JsonObject> m_queueNotifs;

    // Distributed map to prevent duplicate handling (in-memory only)
    private IMap<String, JsonObject> m_handledNotifs;

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
        // Initialize distributed notification queue and map
        //
        HazelcastInstance hzInstance = hazelcastInstance();
        logger().info("Initializing distributed notification map");
        Config hzConfig = new Config();
        if (hzInstance != null) {
            hzConfig = hzInstance.getConfig(); // Grab config template
        }

        MapConfig mapConfig = new MapConfig("notifier");
        mapConfig.setInMemoryFormat(InMemoryFormat.BINARY);
        mapConfig.setBackupCount(1);
        mapConfig.setEvictionPolicy(EvictionPolicy.LRU);
        mapConfig.setTimeToLiveSeconds(m_config.getEntryTimeToLive());
        mapConfig.setMaxSizeConfig(new MaxSizeConfig(m_config.getMaxMapSize(), MaxSizePolicy.PER_NODE));
        hzConfig.addMapConfig(mapConfig);

        logger().info("Initializing distributed and persistent notification queue");
        QueueConfig queueConfig = new QueueConfig("notifier");
        queueConfig.setMaxSize(m_config.getMaxQueueSize());
        queueConfig.setBackupCount(m_config.getQueueBackupCount());
        QueueStoreConfig queueStoreConfig = new QueueStoreConfig();
        queueStoreConfig.setEnabled(true);
        queueStoreConfig.setProperty(CONF_KEY_QUEUE_MEMORY_LIMIT,
                String.valueOf(m_config.getQueueMemoryLimit()));
        queueStoreConfig.setProperty(CONF_KEY_QUEUE_BULK_LOAD,
                String.valueOf(m_config.getQueueBulkLoad()));
        queueStoreConfig.setStoreImplementation(
                new SimpleQueueStore(dataPath(), config().getString(CONF_KEY_USER)));
        queueConfig.setQueueStoreConfig(queueStoreConfig);
        hzConfig.addQueueConfig(queueConfig);

        // Use available or create new instance
        HzEventListener listener = new HzEventListener();
        hzInstance = Hazelcast.newHazelcastInstance(hzConfig);
        hzInstance.getCluster().addMembershipListener(listener);

        m_handledNotifs = hzInstance.getMap("notifier");
        m_handledNotifs.addEntryListener(listener, false); // Listen to entry events

        m_queueNotifs = hzInstance.getQueue("notifier");
        m_queueNotifs.addItemListener(listener, true); // Listen to item events

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
        // Read next batch of events from map (if any)
        //
        if (m_handledNotifs != null
                && m_queueNotifs != null) {

            List<JsonObject> events = new ArrayList();
            m_queueNotifs.drainTo(events, m_config.getDispatcherBatchSize());

            JsonArray jsonEvents = new JsonArray();
            for (JsonObject event : events) {
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

    private void createAndInitNotifier() {
        //
        // Unregister local listener and stop notifier
        //
        if (m_notifier != null) {
            m_notifier.stop(eventBus());
        }
        //
        // Create notifier (and possibly listen on another address)
        //
        m_notifier = new Notifier(
                m_queueNotifs,
                m_handledNotifs,
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
