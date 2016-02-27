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
package io.spikex.filter;

import com.google.common.eventbus.Subscribe;
import io.spikex.core.AbstractActivator;
import static io.spikex.core.AbstractFilter.CONF_KEY_DEST_ADDRESS;
import static io.spikex.core.AbstractFilter.CONF_KEY_SOURCE_ADDRESS;
import io.spikex.core.helper.Commands;
import static io.spikex.core.helper.Commands.RET_DEPLOYMENT_ID;
import io.spikex.core.helper.Commands.Result;
import io.spikex.core.util.NioDirWatcher;
import io.spikex.filter.internal.FiltersConfig;
import io.spikex.filter.internal.FiltersConfig.ChainDef;
import io.spikex.filter.internal.FiltersConfig.FilterDef;
import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.StandardWatchEventKinds;
import java.nio.file.WatchEvent;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.vertx.java.core.Handler;
import org.vertx.java.core.eventbus.Message;
import org.vertx.java.core.json.JsonObject;

/**
 *
 * @author cli
 */
public final class Activator extends AbstractActivator {

    // Last time we did a re-init
    private long m_lastInitTm;

    // Changed condiguration files
    private final Queue<String> m_changedFilenames;

    // Deployed filter configurations
    private final Map<String, FiltersConfig> m_configs;

    // Configuration file name prefix
    private static final String CONFIG_PREFIX = "filters";

    // Max latch await time (ms)
    private static final long MAX_AWAIT_MS = 25000;

    public Activator() {

        m_changedFilenames = new ConcurrentLinkedQueue();
        m_configs = new ConcurrentHashMap();
    }

    @Override
    protected void startVerticle() {

        try {

            //
            // Force initial load of configuration files
            //
            File[] files = confPath().toFile().listFiles(new FilenameFilter() {

                @Override
                public boolean accept(
                        final File dir,
                        final String name) {

                    return name.startsWith(CONFIG_PREFIX);
                }
            });
            for (File file : files) {
                String filename = file.getName();
                int n = filename.lastIndexOf("."); // Drop suffix
                m_changedFilenames.offer(filename.substring(0, n));
            }

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

        while (true) {

            // Retrieve next changed filename
            String filename = m_changedFilenames.poll();
            if (filename == null) {
                break; // Done
            }

            try {
                FiltersConfig config = m_configs.get(filename);
                if (config == null) {
                    config = new FiltersConfig(filename, confPath());
                    m_configs.put(filename, config);
                }

                if (config.hasChanged()) {

                    //
                    // Load chain configuration and deploy filters
                    //
                    config.load();
                    if (config.isEmpty()) {
                        logger().error("Empty configuration file: {}",
                                config.getYaml().getQualifiedName());
                    } else {

                        config.logInputOutputDef();

                        for (ChainDef chain : config.getChains()) {
                            logger().info("Building chain: {}", chain.getName());

                            for (final FilterDef filter : chain.getFilters()) {

                                // Deploy verticle and initialize it
                                deployFilter(
                                        filter,
                                        new DeployResult(filter));
                            }
                        }
                    }
                }
            } catch (Exception e) {
                logger().error("Failed to initialize filters", e);
            }
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
                        && filePath.getFileName().toString().startsWith(CONFIG_PREFIX)) {
                    String filename = filePath.getFileName().toString();
                    logger().info("Configuration file modified: {}", filename);

                    //
                    // Undeploy current chain first 
                    // (we can use a latch since we're not on the Vert.x event thread)
                    //
                    int n = filename.lastIndexOf("."); // Drop suffix
                    filename = filename.substring(0, n);
                    FiltersConfig config = m_configs.get(filename);
                    if (config != null) {
                        int count = getDeployedFilterCount(config);
                        if (count > 0) {
                            CountDownLatch latch = new CountDownLatch(count);
                            undeployChain(
                                    config,
                                    latch);
                            if (latch.await(MAX_AWAIT_MS, TimeUnit.MILLISECONDS)) {
                                m_configs.remove(filename); // Successfully undeployed
                            }
                        }
                    }

                    m_changedFilenames.offer(filename);
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

    private int getDeployedFilterCount(final FiltersConfig config) {
        int count = 0;
        for (ChainDef chain : config.getChains()) {
            for (final FilterDef filter : chain.getFilters()) {
                if (filter.isDeployed()) {
                    count++;
                }
            }
        }
        return count;
    }

    private void deployFilter(
            final FilterDef filter,
            final Handler<Message<JsonObject>> handler) {

        String address = filter.getModule(); // Address of filter activator
        String verticle = filter.getVerticle();
        JsonObject config = filter.getJsonConfig();

        // Input / output address of filter
        config.putString(CONF_KEY_SOURCE_ADDRESS, filter.getInputAddress());
        config.putString(CONF_KEY_DEST_ADDRESS, filter.getOutputAddress());

        // Filter local address
        config.putString(CONF_KEY_LOCAL_ADDRESS, verticle);

        logger().info("Deploying {} (activator: {})",
                verticle, address);

        Commands.call(
                eventBus(),
                address,
                Commands.cmdDeployVerticle(
                        verticle,
                        config,
                        filter.getInstances(),
                        filter.isMultiThreaded(),
                        filter.isWorker()),
                handler);
    }

    private void undeployChain(
            final FiltersConfig config,
            final CountDownLatch latch) {

        for (ChainDef chain : config.getChains()) {
            logger().info("Undeploying chain: {}", chain.getName());

            for (final FilterDef filter : chain.getFilters()) {

                // Undeploy, if already deployed
                if (filter.isDeployed()) {
                    undeployFilter(
                            filter,
                            new Handler<Message<JsonObject>>() {

                                @Override
                                public void handle(final Message<JsonObject> message) {
                                    Result result = new Result(message.body());
                                    if (!result.isOk()) {
                                        throw new IllegalStateException(result.getReason());
                                    } else {
                                        latch.countDown();
                                    }
                                }
                            });
                }
            }
        }
    }

    private void undeployFilter(
            final FilterDef filter,
            final Handler<Message<JsonObject>> handler) {

        String address = filter.getModule(); // Address of filter activator
        String verticle = filter.getVerticle();
        String deploymentId = filter.getDeploymentId();

        logger().info("Undeploying {} (activator: {} deploymentId: {})",
                verticle,
                address,
                deploymentId);

        Commands.call(
                eventBus(),
                address,
                Commands.cmdUndeployVerticle(deploymentId),
                handler);
    }

    private static final class DeployResult
            implements Handler<Message<JsonObject>> {

        private final FilterDef m_filter;

        private DeployResult(final FilterDef filter) {
            m_filter = filter;
        }

        @Override
        public void handle(final Message<JsonObject> message) {
            Result result = new Result(message.body());
            if (!result.isReturnCode(RET_DEPLOYMENT_ID)) {
                throw new IllegalStateException(result.getReason());
            } else {
                m_filter.setDeploymentId(result.getReason());
            }
        }
    }
}
