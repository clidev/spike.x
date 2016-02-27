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
import static io.spikex.filter.integration.TestUtils.deleteFolder;
import io.spikex.filter.internal.FiltersConfig;
import io.spikex.filter.output.NsqHttp;
import java.io.IOException;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.file.FileSystems;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.junit.Assert;
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
public class NsqHttpOutTest extends TestVerticle implements Handler<Long> {

    private int m_counter;
    private long m_timer;
    private static ChildProcess m_cmd;

    private static final String FILTER_NSQ_NAME = "NsqHttp.out";
    private static final String FILTER_NSQ_TOPIC = "tokyo913.metrics.host.zeus";
    private static final String FILTER_SOURCE_ADDRESS = "InputNsqFilter." + new UUID().toString();

    private static final String TOPIC_KEY = "@nsq.topic";
    private static final String CONF_NAME = "filters";

    private static final Logger m_logger = LoggerFactory.getLogger(NsqHttpOutTest.class);

    @Test
    public void testGenerateMessages() throws Exception {

        // NSQ supports OS X and Linux
        if (HostOs.isMac() || HostOs.isLinux()) {
            ClassLoader cl = ClassLoader.getSystemClassLoader();
            URL[] urls = ((URLClassLoader) cl).getURLs();
            m_logger.debug("CLASSPATH:");
            for (URL url : urls) {
                m_logger.debug(url.getFile());
            }

            JsonObject config = createBaseConfig();
            config.mergeIn(loadNsqConfig(config));

            new NsqThread().start();

            container.deployWorkerVerticle(NsqHttp.class.getName(), config, 1, false,
                    new AsyncResultHandler<String>() {
                        @Override
                        public void handle(final AsyncResult<String> ar) {
                            if (ar.failed()) {
                                m_logger.error("Failed to deploy verticle", ar.cause());
                                Assert.fail();
                            }
                        }
                    });

            m_timer = vertx.setPeriodic(150L, this); // Generate events once every 150 ms
        } else {
            // Stop test immediately
            VertxAssert.testComplete();
        }
    }

    @Override
    public void handle(final Long timerId) {
        if (m_counter++ > 50) {
            // Stop test
            vertx.cancelTimer(m_timer);
            VertxAssert.testComplete();
            m_cmd.destroy(true);
        } else {
            //
            // Generate test events
            //                            
            Map<String, Object> nsqFields = new HashMap();
            nsqFields.put(TOPIC_KEY, FILTER_NSQ_TOPIC);
            JsonObject event = EventCreator.createBatch("NSQ", nsqFields);
            vertx.eventBus().publish(FILTER_SOURCE_ADDRESS, event);
        }
    }

    private JsonObject loadNsqConfig(final JsonObject baseConfig)
            throws ResourceException {

        Path confPath = FileSystems.getDefault().getPath(
                baseConfig.getString(CONF_KEY_CONF_PATH));

        FiltersConfig.FilterDef filterDef = null;
        FiltersConfig config = new FiltersConfig(CONF_NAME, confPath);
        config.load();
        config.logInputOutputDef();

        for (FiltersConfig.ChainDef chain : config.getChains()) {
            for (FiltersConfig.FilterDef filter : chain.getFilters()) {
                if (FILTER_NSQ_NAME.equalsIgnoreCase(filter.getAlias())) {
                    filterDef = filter;
                    break;
                }
            }
        }

        junit.framework.Assert.assertNotNull("Could not find Nsq.out filter from " + confPath,
                filterDef);

        String verticle = filterDef.getVerticle();
        JsonObject nsqConfig = filterDef.getJsonConfig();

        // Input address of filter
        nsqConfig.putString(CONF_KEY_SOURCE_ADDRESS, FILTER_SOURCE_ADDRESS);

        // Filter local address
        nsqConfig.putString(CONF_KEY_LOCAL_ADDRESS, verticle);

        return nsqConfig;
    }

    private JsonObject createBaseConfig() {
        JsonObject config = new JsonObject();
        config.putString(CONF_KEY_CHAIN_NAME, "nsq-test");
        config.putString(CONF_KEY_LOCAL_ADDRESS, "my-local-address");
        config.putString(CONF_KEY_NODE_NAME, "node-name");
        config.putString(CONF_KEY_CLUSTER_NAME, "cluster-name");
        config.putString(CONF_KEY_HOME_PATH, "build/nsq");
        config.putString(CONF_KEY_CONF_PATH, "build/resources/test");
        config.putString(CONF_KEY_DATA_PATH, "build/nsq/data");
        config.putString(CONF_KEY_TMP_PATH, "build/nsq/tmp");
        config.putString(CONF_KEY_USER, "spikex");
        m_logger.info("Host operating system: {}", HostOs.operatingSystemFull());
        return config;
    }

    public static class NsqThread extends Thread {

        public NsqThread() {
            super("nsq-thread");
        }

        @Override
        public void run() {
            final Path tmpPath = Paths.get("/tmp/nsq_ssl");
            Path basePath = Paths.get("build/resources/test").toAbsolutePath();
            try {
                Path nsqPath = basePath;
                if (HostOs.isMac()) {
                    nsqPath = Paths.get(basePath.toString(), "nsq-0.3.6.darwin-amd64.go1.5.1", "bin");
                }
                if (HostOs.isLinux()) {
                    nsqPath = Paths.get(basePath.toString(), "nsq-0.3.6.linux-amd64.go1.5.1", "bin");
                }
                m_cmd = new ProcessExecutor()
                        .command(nsqPath.resolve("nsqd").toString(), "-config=" + basePath.resolve("nsqd_ssl.conf").toString())
                        .timeout(3000L, TimeUnit.MILLISECONDS)
                        .handler(new DefaultProcessHandler() {
                            @Override
                            public void onPreStart(final ChildProcess process) {
                                // Cleanup and create empty dir
                                deleteFolder(tmpPath.toFile());
                                try {
                                    java.nio.file.Files.createDirectories(tmpPath);
                                } catch (IOException e) {
                                    Assert.fail("Failed to create " + tmpPath + ":" + e.getMessage());
                                }
                                // Always call super method
                                super.onPreStart(process);
                            }

                            @Override
                            public void onExit(final int statusCode) {
                                // Cleanup
                                deleteFolder(tmpPath.toFile());
                            }

                            @Override
                            protected void handleStderr(final String txt) {
                                handleStdout(txt); // Reroute to stdout handler
                            }
                        })
                        .start();
                m_cmd.waitForExit();
            } catch (InterruptedException e) {
                m_logger.error("Failed to start NSQ service", e);
                Assert.fail();
            }
        }
    }
}
