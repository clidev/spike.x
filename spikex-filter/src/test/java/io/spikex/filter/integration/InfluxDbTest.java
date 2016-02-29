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
import static io.spikex.core.AbstractFilter.CONF_KEY_CHAIN_NAME;
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
import io.spikex.filter.internal.FiltersConfig;
import io.spikex.filter.output.InfluxDb;
import java.io.IOException;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.file.DirectoryStream;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import junit.framework.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.vertx.java.core.AsyncResult;
import org.vertx.java.core.AsyncResultHandler;
import org.vertx.java.core.Handler;
import org.vertx.java.core.json.JsonObject;
import org.vertx.testtools.TestVerticle;
import org.vertx.testtools.VertxAssert;

/**
 *
 * @author cli
 */
public class InfluxDbTest extends TestVerticle implements Handler<Long> {

    private int m_counter;
    private static final CountDownLatch m_latch = new CountDownLatch(17);
    private static final List<JsonObject> m_metrics = new ArrayList();

    private static final String FILTER_INFLUXDB_NAME = "InfluxDB.out";
    private static final String FILTER_SOURCE_ADDRESS = "InputInfluxDbFilter." + new UUID().toString();

    private static final String PLUGIN_KEY = "plugin";
    private static final String PLUGIN_VALUE = "cpu";
    private static final String PLUGIN_INSTANCE_KEY = "plugin-instance";
    private static final String PLUGIN_INSTANCE_VALUE = "cpu-0";
    private static final String TYPE_KEY = "type";
    private static final String TYPE_VALUE = "";
    private static final String TYPE_INSTANCE_KEY = "type-instance";
    private static final String TYPE_INSTANCE_VALUE = "-";

    private static final String CONF_NAME = "filters";

    private static final Logger m_logger = LoggerFactory.getLogger(InfluxDbTest.class);

    @Test
    public void testGenerateMessages() throws Exception {

        // InfluxDB supports OS X and Linux
        if (HostOs.isMac() || HostOs.isLinux()) {
            ClassLoader cl = ClassLoader.getSystemClassLoader();
            URL[] urls = ((URLClassLoader) cl).getURLs();
            m_logger.debug("CLASSPATH:");
            for (URL url : urls) {
                m_logger.debug(url.getFile());
            }

            JsonObject config = createBaseConfig();
            config.mergeIn(loadInfluxDbConfig(config));

            Path metricsPath = Paths.get("build/resources/test/metrics").toAbsolutePath();
            try (DirectoryStream<Path> dirStream = Files.newDirectoryStream(metricsPath, "*.json")) {
                for (Path file : dirStream) {
                    m_logger.debug("Loading file: {}", file);
                    JsonObject json = new JsonObject(loadFileContents(file));
                    m_metrics.add(json);
                }
            } catch (IOException e) {
                m_logger.error("Failed to read metric file(s)", e);
            }

            // Start two InfluxDB servers
            new InfluxDbThread("influxdb-thread1", "influxdb1.toml", "influxdb1").start();
            new InfluxDbThread("influxdb-thread2", "influxdb2.toml", "influxdb2").start();

            // Wait until InfluxDB has started
            m_latch.await();

            container.deployWorkerVerticle(InfluxDb.class.getName(), config, 1, false,
                    new AsyncResultHandler<String>() {
                        @Override
                        public void handle(final AsyncResult<String> ar) {
                            if (ar.failed()) {
                                m_logger.error("Failed to deploy verticle", ar.cause());
                                Assert.fail();
                            }
                        }
                    });

            vertx.setPeriodic(150L, this); // Generate events once every 150 ms
        } else {
            // Stop test immediately
            VertxAssert.testComplete();
        }
    }

    @Override
    public void handle(final Long timerId) {
        if (m_latch.getCount() == 0) {
            if (m_counter++ > 80) {
                // Stop test
                VertxAssert.testComplete();
            } else {
                //
                // Generate test events
                //
                for (JsonObject json : m_metrics) {
                    JsonObject event = EventCreator.createBatch("metrics", json.toMap(), 5);
                    vertx.eventBus().publish(FILTER_SOURCE_ADDRESS, event);
                }
            }
        }
    }

    private JsonObject loadInfluxDbConfig(final JsonObject baseConfig)
            throws ResourceException {

        Path confPath = FileSystems.getDefault().getPath(
                baseConfig.getString(CONF_KEY_CONF_PATH));

        FiltersConfig.FilterDef filterDef = null;
        FiltersConfig config = new FiltersConfig(CONF_NAME, confPath);
        config.load();
        config.logInputOutputDef();

        for (FiltersConfig.ChainDef chain : config.getChains()) {
            for (FiltersConfig.FilterDef filter : chain.getFilters()) {
                if (FILTER_INFLUXDB_NAME.equalsIgnoreCase(filter.getAlias())) {
                    filterDef = filter;
                    break;
                }
            }
        }

        junit.framework.Assert.assertNotNull("Could not find InfluxDB.out filter from " + confPath,
                filterDef);

        String verticle = filterDef.getVerticle();
        JsonObject influxDbConfig = filterDef.getJsonConfig();

        // Input address of filter
        influxDbConfig.putString(CONF_KEY_SOURCE_ADDRESS, FILTER_SOURCE_ADDRESS);

        // Filter local address
        influxDbConfig.putString(CONF_KEY_LOCAL_ADDRESS, verticle);

        return influxDbConfig;
    }

    private JsonObject createBaseConfig() {
        JsonObject config = new JsonObject();
        config.putString(CONF_KEY_CHAIN_NAME, "influxdb-test");
        config.putString(CONF_KEY_LOCAL_ADDRESS, "my-local-address");
        config.putString(CONF_KEY_NODE_NAME, "node-name");
        config.putString(CONF_KEY_CLUSTER_NAME, "cluster-name");
        config.putString(CONF_KEY_HOME_PATH, "build/influxdb");
        config.putString(CONF_KEY_CONF_PATH, "build/resources/test");
        config.putString(CONF_KEY_DATA_PATH, "build/influxdb/data");
        config.putString(CONF_KEY_TMP_PATH, "build/influxdb/tmp");
        config.putString(CONF_KEY_USER, "spikex");
        m_logger.info("Host operating system: {}", HostOs.operatingSystemFull());
        return config;
    }

    private String loadFileContents(final Path filePath) {
        try {
            return new String(Files.readAllBytes(filePath));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public static class InfluxDbThread extends Thread {

        private final String m_configName;
        private final String m_tmpName;

        public InfluxDbThread(
                final String threadName,
                final String configName,
                final String tmpName) {

            super(threadName);
            m_configName = configName;
            m_tmpName = tmpName;
        }

        @Override
        public void run() {
            final Path tmpPath = Paths.get("/tmp", m_tmpName);
            Path basePath = Paths.get("build/resources/test").toAbsolutePath();
            try {
                Path influxDbPath = basePath;
                if (HostOs.isMac()) {
                    influxDbPath = Paths.get(basePath.toString(), "influxdb", "influxd-0.9.5.1-osx");
                }
                if (HostOs.isLinux()) {
                    influxDbPath = Paths.get(basePath.toString(), "influxdb", "influxd-0.9.5.1-linux");
                }
                ChildProcess cmd = new ProcessExecutor()
                        .command(influxDbPath.toString(), "-config=" + basePath.resolve(m_configName).toString())
                        .timeout(3000L, TimeUnit.MILLISECONDS)
                        .handler(new DefaultProcessHandler() {
                            @Override
                            public void onPreStart(final ChildProcess process) {
                                // Cleanup and create empty dir
//                                deleteFolder(tmpPath.toFile());
                                try {
                                    java.nio.file.Files.createDirectories(tmpPath);
                                } catch (IOException e) {
                                    org.junit.Assert.fail("Failed to create " + tmpPath + ":" + e.getMessage());
                                }
                                // Always call super method
                                super.onPreStart(process);
                            }

                            @Override
                            public void onExit(final int statusCode) {
                                // Cleanup
//                                deleteFolder(tmpPath.toFile());
                            }

                            @Override
                            protected void handleStderr(final String txt) {
                                m_latch.countDown();
                                handleStdout(txt); // Reroute to stdout handler
                            }
                        })
                        .start();
                cmd.waitForExit();
            } catch (InterruptedException e) {
                m_logger.error("Failed to start InfluxDB service", e);
                Assert.fail();
            }
        }
    }
}
