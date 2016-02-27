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

import static io.spikex.core.AbstractFilter.CONF_KEY_CHAIN_NAME;
import static io.spikex.core.AbstractFilter.CONF_KEY_DEST_ADDRESS;
import static io.spikex.core.AbstractVerticle.CONF_KEY_CLUSTER_NAME;
import static io.spikex.core.AbstractVerticle.CONF_KEY_CONF_PATH;
import static io.spikex.core.AbstractVerticle.CONF_KEY_DATA_PATH;
import static io.spikex.core.AbstractVerticle.CONF_KEY_HOME_PATH;
import static io.spikex.core.AbstractVerticle.CONF_KEY_LOCAL_ADDRESS;
import static io.spikex.core.AbstractVerticle.CONF_KEY_NODE_NAME;
import static io.spikex.core.AbstractVerticle.CONF_KEY_TMP_PATH;
import static io.spikex.core.AbstractVerticle.CONF_KEY_USER;
import io.spikex.core.util.HostOs;
import io.spikex.core.util.connection.HttpClientAdapter;
import io.spikex.core.util.connection.HttpClientResponseAdapter;
import io.spikex.core.util.connection.HttpConnection;
import io.spikex.core.util.process.ChildProcess;
import io.spikex.core.util.process.DefaultProcessHandler;
import io.spikex.core.util.process.ProcessExecutor;
import io.spikex.core.util.resource.ResourceException;
import io.spikex.filter.input.Nsq;
import static io.spikex.filter.integration.TestUtils.deleteFolder;
import io.spikex.filter.internal.FiltersConfig;
import java.io.IOException;
import java.net.URI;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.ByteBuffer;
import java.nio.file.FileSystems;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.vertx.java.core.AsyncResult;
import org.vertx.java.core.AsyncResultHandler;
import org.vertx.java.core.Handler;
import org.vertx.java.core.Vertx;
import org.vertx.java.core.buffer.Buffer;
import org.vertx.java.core.eventbus.Message;
import org.vertx.java.core.http.HttpClient;
import org.vertx.java.core.http.HttpClientRequest;
import org.vertx.java.core.http.HttpClientResponse;
import static org.vertx.java.core.http.HttpHeaders.CONTENT_LENGTH;
import org.vertx.java.core.json.JsonObject;
import org.vertx.testtools.TestVerticle;
import org.vertx.testtools.VertxAssert;

/**
 *
 * @author cli
 */
public class NsqInTest extends TestVerticle implements Handler<Message<JsonObject>> {

    private int m_counter;

    private static ChildProcess m_cmd1;
    private static ChildProcess m_cmd2;
    private static final CountDownLatch m_nsqLatch = new CountDownLatch(5);

    private static final String FILTER_NSQ_NAME = "Nsq.in";
    private static final String FILTER_NSQ_TOPIC = "tampere812.metrics.host.urho";
    private static final String FILTER_DEST_ADDRESS = "nsq-topic-tampere812";
    private static final String NSQ_PUBLISH_URI = "/pub?topic=" + FILTER_NSQ_TOPIC;
    private static final int MAX_MESSAGE_COUNT = 110;

    private static final String CONF_NAME = "filters";

    private static final Logger m_logger = LoggerFactory.getLogger(NsqInTest.class);

    @Test
    public void testConsumeTopicChannel() throws Exception {

        // NSQ supports OS X and Linux
        if (HostOs.isMac() || HostOs.isLinux()) {
            ClassLoader cl = ClassLoader.getSystemClassLoader();
            URL[] urls = ((URLClassLoader) cl).getURLs();
            m_logger.debug("CLASSPATH:");
            for (URL url : urls) {
                m_logger.debug(url.getFile());
            }

            vertx.eventBus().registerLocalHandler(FILTER_DEST_ADDRESS, this);

            JsonObject config = createBaseConfig();
            config.mergeIn(loadNsqConfig(config));

            new NsqLookupThread().start();
            new NsqThread().start();

            // Wait until NSQ has started
            m_nsqLatch.await();

            new NsqCreateTopicAndMessages(vertx).start();

            Thread.sleep(1000);

            container.deployWorkerVerticle(Nsq.class.getName(), config, 1, false,
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
            // Stop test immediately
            VertxAssert.testComplete();
        }
    }

    @Override
    public void handle(final Message<JsonObject> message) {
        m_logger.info("Received message {}: {}", m_counter, message.body().toString());

        if (++m_counter >= 100) {

            // Forcefully stop processes
            m_cmd1.destroy(true);
            m_cmd2.destroy(true);

            // Stop test
            VertxAssert.testComplete();
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

        junit.framework.Assert.assertNotNull("Could not find Nsq filter from " + confPath,
                filterDef);

        String verticle = filterDef.getVerticle();
        JsonObject nsqConfig = filterDef.getJsonConfig();

        // Output address of filter
        nsqConfig.putString(CONF_KEY_DEST_ADDRESS, FILTER_DEST_ADDRESS);

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

    public static class NsqLookupThread extends Thread {

        public NsqLookupThread() {
            super("nsqlookup-thread");
        }

        @Override
        public void run() {
            Path basePath = Paths.get("build/resources/test").toAbsolutePath();
            try {
                Path nsqPath = basePath;
                if (HostOs.isMac()) {
                    nsqPath = Paths.get(basePath.toString(), "nsq-0.3.6.darwin-amd64.go1.5.1", "bin");
                }
                if (HostOs.isLinux()) {
                    nsqPath = Paths.get(basePath.toString(), "nsq-0.3.6.linux-amd64.go1.5.1", "bin");
                }
                m_cmd1 = new ProcessExecutor()
                        .command(nsqPath.resolve("nsqlookupd").toString(), "-config=" + basePath.resolve("nsqlookupd.conf").toString())
                        .timeout(2000L, TimeUnit.MILLISECONDS)
                        .start();
                m_cmd1.waitForExit();
            } catch (InterruptedException e) {
                m_logger.error("Failed to start NSQ lookup service", e);
                Assert.fail();
            }
        }
    }

    public static class NsqThread extends Thread {

        public NsqThread() {
            super("nsq-thread");
        }

        @Override
        public void run() {
            final Path tmpPath = Paths.get("/tmp/nsq");
            Path basePath = Paths.get("build/resources/test").toAbsolutePath();
            try {
                Path nsqPath = basePath;
                if (HostOs.isMac()) {
                    nsqPath = Paths.get(basePath.toString(), "nsq-0.3.6.darwin-amd64.go1.5.1", "bin");
                }
                if (HostOs.isLinux()) {
                    nsqPath = Paths.get(basePath.toString(), "nsq-0.3.6.linux-amd64.go1.5.1", "bin");
                }
                m_cmd2 = new ProcessExecutor()
                        .command(nsqPath.resolve("nsqd").toString(), "-config=" + basePath.resolve("nsqd.conf").toString())
                        .timeout(2000L, TimeUnit.MILLISECONDS)
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
                            public void onStdout(
                                    final ByteBuffer buffer,
                                    final boolean closed) {

                                        super.onStdout(buffer, closed);
                                        m_nsqLatch.countDown();
                            }

                            @Override
                            public void onStderr(
                                    final ByteBuffer buffer,
                                    final boolean closed) {

                                        super.onStderr(buffer, closed);
                                        m_nsqLatch.countDown();
                            }
                        })
                        .start();
                m_cmd2.waitForExit();
            } catch (InterruptedException e) {
                m_logger.error("Failed to start NSQ service", e);
                Assert.fail();
            }
        }
    }

    public static class NsqCreateTopicAndMessages extends Thread {

        private final Vertx m_vertx;

        public NsqCreateTopicAndMessages(Vertx vertx) {
            super("nsq-topic-messages-thread");
            m_vertx = vertx;
        }

        @Override
        public void run() {
            URI address = URI.create("http://localhost:4151");
            HttpConnection nsqConnection
                    = HttpConnection.builder(address, m_vertx)
                    .connectTimeout(500L)
                    .build();

            for (int i = 0; i < MAX_MESSAGE_COUNT; i++) {
                JsonObject json = new JsonObject();
                json.putString("caption", "Test Message");
                json.putNumber("random", (int) (Math.random() * 100.0d));
                json.putNumber("index", i);
                doPost(nsqConnection, json.toString(), i);
            }
        }

        private void doPost(
                final HttpConnection connection,
                final String message,
                final int index) {

            HttpClientAdapter adapter = new HttpClientAdapter(connection) {

                private int m_counter;

                @Override
                protected void doRequest(final HttpClient client) {

                    HttpClientRequest request = client.post(NSQ_PUBLISH_URI, new HttpClientResponseAdapter() {

                        @Override
                        protected void handleFailure(final HttpClientResponse response) {
                            m_logger.error("Failed to post message {}: {}/{}",
                                    index, response.statusCode(), response.statusMessage());
                            Assert.fail();
                            VertxAssert.testComplete();
                        }
                    });

                    byte[] body = message.getBytes();
                    request.putHeader(CONTENT_LENGTH, String.valueOf(body.length));
                    request.write(new Buffer(body));
                    request.end();
                }
            };

            connection.doRequest(adapter);
        }
    }
}
