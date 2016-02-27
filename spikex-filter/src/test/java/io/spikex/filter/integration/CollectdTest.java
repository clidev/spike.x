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
package io.spikex.filter.integration;

import com.eaio.uuid.UUID;
import com.google.common.io.Files;
import static io.spikex.core.AbstractFilter.CONF_KEY_CHAIN_NAME;
import static io.spikex.core.AbstractFilter.CONF_KEY_DEST_ADDRESS;
import static io.spikex.core.AbstractFilter.CONF_KEY_SOURCE_ADDRESS;
import static io.spikex.core.AbstractVerticle.CONF_KEY_CLUSTER_NAME;
import static io.spikex.core.AbstractVerticle.CONF_KEY_CONF_PATH;
import static io.spikex.core.AbstractVerticle.CONF_KEY_DATA_PATH;
import static io.spikex.core.AbstractVerticle.CONF_KEY_HOME_PATH;
import static io.spikex.core.AbstractVerticle.CONF_KEY_LOCAL_ADDRESS;
import static io.spikex.core.AbstractVerticle.CONF_KEY_NODE_NAME;
import static io.spikex.core.AbstractVerticle.CONF_KEY_TMP_PATH;
import static io.spikex.core.AbstractVerticle.CONF_KEY_USER;
import io.spikex.core.util.HostOs;
import io.spikex.core.util.process.ChildProcess;
import io.spikex.core.util.process.DefaultProcessHandler;
import io.spikex.core.util.process.ProcessExecutor;
import io.spikex.core.util.resource.ResourceException;
import io.spikex.filter.input.Collectd;
import static io.spikex.filter.integration.TestUtils.deleteFolder;
import io.spikex.filter.internal.FiltersConfig;
import java.io.IOException;
import java.nio.file.FileSystems;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.concurrent.TimeUnit;
import junit.framework.Assert;
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
public class CollectdTest extends TestVerticle implements Handler<Message<JsonObject>> {

    private int m_counter;
    private static ChildProcess m_cmd;

    private static final Path TMP_PATH = Paths.get("/tmp/collectd");

    private static final String FILTER_COLLECTD_NAME = "Collectd.in";
    private static final String FILTER_DEST_ADDRESS = "CollectdFilter." + new UUID().toString();
    
    private static final String CONF_NAME = "filters";

    private static final Logger m_logger = LoggerFactory.getLogger(CollectdTest.class);

    @Test
    public void testHttpWriter() throws Exception {

        // We test collectd support on OS X and Linux
        if (HostOs.isMac() || HostOs.isLinux()) {

            // Start collectd
            new CollectdThread("collectd-thread", "collectd.conf").start();

            // Collectd listener configuration
            JsonObject config = createBaseConfig();
            config.mergeIn(loadCollectdConfig(config));

            vertx.eventBus().registerLocalHandler(FILTER_DEST_ADDRESS, this);

            // Start collectd listener
            container.deployWorkerVerticle(Collectd.class.getName(), config, 1, false,
                    new AsyncResultHandler<String>() {
                        @Override
                        public void handle(final AsyncResult<String> ar) {
                            if (ar.failed()) {
                                m_logger.error("Failed to deploy verticle", ar.cause());
                                Assert.fail();
                            }
                        }
                    });
        } else {
            // Stop test
            VertxAssert.testComplete();
        }
    }

    @Override
    public void handle(final Message<JsonObject> message) {
        m_logger.info("Received event: {}", message.body().toString());

        if (++m_counter > 10) {
            
            // Stop collectd
            m_cmd.destroy(true);

            // Stop test
            VertxAssert.testComplete();
        }
    }

    private JsonObject createBaseConfig() {
        JsonObject config = new JsonObject();
        config.putString(CONF_KEY_CHAIN_NAME, "collectd-test");
        config.putString(CONF_KEY_LOCAL_ADDRESS, "my-local-address");
        config.putString(CONF_KEY_SOURCE_ADDRESS, "");
        config.putString(CONF_KEY_DEST_ADDRESS, FILTER_DEST_ADDRESS);
        config.putString(CONF_KEY_NODE_NAME, "node-name");
        config.putString(CONF_KEY_CLUSTER_NAME, "cluster-name");
        config.putString(CONF_KEY_HOME_PATH, "build");
        config.putString(CONF_KEY_CONF_PATH, "build/resources/test");
        config.putString(CONF_KEY_DATA_PATH, "build");
        config.putString(CONF_KEY_TMP_PATH, "build");
        config.putString(CONF_KEY_USER, "spikex");
        m_logger.info("Host operating system: {}", HostOs.operatingSystemFull());
        return config;
    }

    private JsonObject loadCollectdConfig(final JsonObject baseConfig)
            throws ResourceException {

        Path confPath = FileSystems.getDefault().getPath(
                baseConfig.getString(CONF_KEY_CONF_PATH));

        FiltersConfig.FilterDef filterDef = null;
        FiltersConfig config = new FiltersConfig(CONF_NAME, confPath);
        config.load();
        config.logInputOutputDef();

        for (FiltersConfig.ChainDef chain : config.getChains()) {
            for (FiltersConfig.FilterDef filter : chain.getFilters()) {
                if (FILTER_COLLECTD_NAME.equalsIgnoreCase(filter.getAlias())) {
                    filterDef = filter;
                    break;
                }
            }
        }

        junit.framework.Assert.assertNotNull("Could not find Collectd.in filter from " + confPath,
                filterDef);

        String verticle = filterDef.getVerticle();
        JsonObject collectdConfig = filterDef.getJsonConfig();

        // Output address of filter
        collectdConfig.putString(CONF_KEY_DEST_ADDRESS, FILTER_DEST_ADDRESS);

        // Filter local address
        collectdConfig.putString(CONF_KEY_LOCAL_ADDRESS, verticle);

        return collectdConfig;
    }
    
    
    public static class CollectdThread extends Thread {

        private final String m_configName;

        public CollectdThread(
                final String threadName,
                final String configName) {

            super(threadName);
            m_configName = configName;
        }

        @Override
        public void run() {
            final Path basePath = Paths.get("build/resources/test").toAbsolutePath();
            final Path baseCollectdPath = basePath.resolve("collectd");
            try {
                Path collectdPath = baseCollectdPath.resolve("collectd-5.5.0-linux");
                if (HostOs.isMac()) {
                    collectdPath = baseCollectdPath.resolve("collectd-5.5.0-osx");
                }

                m_cmd = new ProcessExecutor()
                        .command(collectdPath.toString(), "-f", "-C", basePath.resolve(m_configName).toString())
                        .timeout(3000L, TimeUnit.MILLISECONDS)
                        .handler(new DefaultProcessHandler() {
                            @Override
                            public void onPreStart(final ChildProcess process) {
                                // Cleanup and create empty dir
                                cleanupFiles(TMP_PATH);

                                // Copy file to tmp dir
                                copyFiles(
                                        baseCollectdPath,
                                        basePath,
                                        TMP_PATH);

                                // Always call cuper method
                                super.onPreStart(process);
                            }

                            @Override
                            public void onExit(final int statusCode) {
                                // Cleanup
                                deleteFolder(TMP_PATH.toFile());
                            }

                            @Override
                            protected void handleStderr(final String txt) {
                                handleStdout(txt); // Reroute to stdout handler
                            }
                        })
                        .start();
                m_cmd.waitForExit();
            } catch (InterruptedException e) {
                m_logger.error("Failed to start InfluxDB service", e);
                Assert.fail();
            }
        }

        private void cleanupFiles(final Path tmpPath) {
            try {
                deleteFolder(tmpPath.toFile());
                java.nio.file.Files.createDirectories(tmpPath);
            } catch (IOException e) {
                org.junit.Assert.fail("Failed to create path:" + e.getMessage());
            }
        }

        private void copyFiles(
                final Path baseCollectdPath,
                final Path basePath,
                final Path tmpPath) {

            try {
                Path libPath = tmpPath.resolve("lib");
                java.nio.file.Files.createDirectories(libPath);
                // Copy types file to temp dir
                Files.copy(basePath.resolve("types.db").toFile(),
                        tmpPath.resolve("types.db").toFile());
                // Copy library files to temp dir
                Path baseLibPath = Paths.get(baseCollectdPath.toString(), "lib", "linux");
                if (HostOs.isMac()) {
                    baseLibPath = Paths.get(baseCollectdPath.toString(), "lib", "osx");
                }
                copyLibs(baseLibPath, libPath);
            } catch (IOException e) {
                org.junit.Assert.fail("Failed to copy files:" + e.getMessage());
            }
        }

        private void copyLibs(
                final Path basePath,
                final Path destPath) throws IOException {

            // LogFile
            Files.copy(basePath.resolve("logfile.so").toFile(),
                    destPath.resolve("logfile.so").toFile());
            // Write HTTP
            Files.copy(basePath.resolve("write_http.so").toFile(),
                    destPath.resolve("write_http.so").toFile());
            // CPU - This doesn't seem to work on OSX (https://github.com/collectd/collectd/issues/22)
            Files.copy(basePath.resolve("cpu.so").toFile(),
                    destPath.resolve("cpu.so").toFile());
            // Interface
            Files.copy(basePath.resolve("interface.so").toFile(),
                    destPath.resolve("interface.so").toFile());
            // Load
            Files.copy(basePath.resolve("load.so").toFile(),
                    destPath.resolve("load.so").toFile());
            // Memory
            Files.copy(basePath.resolve("memory.so").toFile(),
                    destPath.resolve("memory.so").toFile());
            // Network
            Files.copy(basePath.resolve("network.so").toFile(),
                    destPath.resolve("network.so").toFile());
        }
    }
}
