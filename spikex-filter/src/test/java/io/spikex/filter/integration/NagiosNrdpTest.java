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
import io.spikex.core.util.connection.DefaultConnectionExceptionHandler;
import io.spikex.core.util.connection.HttpClientAdapter;
import io.spikex.core.util.connection.HttpClientResponseAdapter;
import io.spikex.core.util.connection.HttpConnection;
import io.spikex.core.util.connection.IConnection;
import io.spikex.filter.input.HttpServer;
import java.io.IOException;
import java.net.URI;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import junit.framework.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.vertx.java.core.AsyncResult;
import org.vertx.java.core.AsyncResultHandler;
import org.vertx.java.core.Handler;
import org.vertx.java.core.buffer.Buffer;
import org.vertx.java.core.eventbus.Message;
import org.vertx.java.core.http.HttpClient;
import org.vertx.java.core.http.HttpClientRequest;
import org.vertx.java.core.http.HttpClientResponse;
import static org.vertx.java.core.http.HttpHeaders.CONTENT_LENGTH;
import org.vertx.java.core.json.JsonArray;
import org.vertx.java.core.json.JsonObject;
import org.vertx.testtools.TestVerticle;
import org.vertx.testtools.VertxAssert;

/**
 *
 * @author cli
 */
public class NagiosNrdpTest extends TestVerticle implements Handler<Message<JsonObject>> {

    private int m_counter;

    private static final String FILTER_SRC_ADDRESS = "nrdp-data-src";
    private static final String FILTER_DEST_ADDRESS = "nrdp-data-dest";

    private static final int SERVER_PORT = 44120;
    private static final String SERVER_HOST = "localhost";

    private static final Logger m_logger = LoggerFactory.getLogger(NagiosNrdpTest.class);

    @Test
    public void testNrdpResults() throws Exception {

        // Http server configuration
        JsonObject config = createBaseConfig();
        config.putArray("add-tags", new JsonArray(new String[]{"nrdp"}));
        config.putString("input-format", "nagios-nrdp");

        JsonObject nrdpConfig = new JsonObject();
        JsonArray services = new JsonArray();

        // files_count
        JsonObject srvConfig1 = new JsonObject();
        srvConfig1.putString("name", "files_count");
        srvConfig1.putString("type", "metric");
        services.add(srvConfig1);

        // files_count
        JsonObject srvConfig2 = new JsonObject();
        srvConfig2.putString("name", "files_old");
        srvConfig2.putString("type", "event");
        services.add(srvConfig2);

        // the rest
        JsonObject srvConfig3 = new JsonObject();
        srvConfig3.putString("name", "*");
        JsonArray patterns = new JsonArray();
        patterns.add("(?:values=)?(\\[)?([@\\w\\_\\.]+)=([ #:\\w\\.\\\\/]+)([\\]]?)?");
        srvConfig3.putArray("output-patterns", patterns);
        services.add(srvConfig3);

        JsonArray tokens = new JsonArray(new String[]{"1234secret!"});
        nrdpConfig.putArray("accepted-tokens", tokens);
        nrdpConfig.putBoolean("fix-units", true);
        nrdpConfig.putArray("event-tags", new JsonArray(new String[]{"event"}));
        nrdpConfig.putArray("metric-tags", new JsonArray(new String[]{"spikex-metric"}));
        nrdpConfig.putArray("services", services);
        config.putObject("nrdp-config", nrdpConfig);

        vertx.eventBus().registerLocalHandler(FILTER_DEST_ADDRESS, this);

        // Start http server
        container.deployWorkerVerticle(HttpServer.class.getName(), config, 1, false,
                new AsyncResultHandler<String>() {
                    @Override
                    public void handle(final AsyncResult<String> ar) {
                        if (ar.failed()) {
                            m_logger.error("Failed to deploy verticle", ar.cause());
                            Assert.fail();
                        } else {

                            URI address = URI.create("http://" + SERVER_HOST + ":" + SERVER_PORT);
                            HttpConnection connection = HttpConnection.builder(address, vertx).build();

                            try {
                                Thread.sleep(2000L); // Wait 2 seconds for http server to start
                            } catch (InterruptedException e) {
                                m_logger.error("Waiting for HTTP server was interrupted");
                            }

                            Path nrdpPath = Paths.get("build/resources/test/nrdp").toAbsolutePath();
                            try (DirectoryStream<Path> dirStream = Files.newDirectoryStream(nrdpPath, "*.txt")) {
                                for (Path file : dirStream) {
                                    NrdpWriter handler = new NrdpWriter(connection, file);
                                    connection.doRequest(handler);
                                }
                            } catch (IOException e) {
                                m_logger.error("Failed to read NRDP file(s)", e);
                            }
                        }
                    }
                });
    }

    @Override
    public void handle(final Message<JsonObject> message) {
        m_logger.info("Received event: {}", message.body().encodePrettily());
        m_logger.info("======================================== {} =========================================", m_counter);

        if (++m_counter > 26) {

            // Stop test
            VertxAssert.testComplete();
        }
    }

    private JsonObject createBaseConfig() {
        JsonObject config = new JsonObject();
        config.putString(CONF_KEY_CHAIN_NAME, "nrdp-test");
        config.putString(CONF_KEY_LOCAL_ADDRESS, "my-local-address");
        config.putString(CONF_KEY_SOURCE_ADDRESS, FILTER_SRC_ADDRESS);
        config.putString(CONF_KEY_DEST_ADDRESS, FILTER_DEST_ADDRESS);
        config.putString(CONF_KEY_NODE_NAME, "node-name");
        config.putString(CONF_KEY_CLUSTER_NAME, "cluster-name");
        config.putString(CONF_KEY_HOME_PATH, "build");
        config.putString(CONF_KEY_CONF_PATH, "build");
        config.putString(CONF_KEY_DATA_PATH, "build");
        config.putString(CONF_KEY_TMP_PATH, "build");
        config.putString(CONF_KEY_USER, "spikex");
        m_logger.info("Host operating system: {}", HostOs.operatingSystemFull());
        return config;
    }

    private static class NrdpWriter extends HttpClientAdapter {

        private final Path m_filePath;
        private final Logger m_logger = LoggerFactory.getLogger(NrdpWriter.class);

        private NrdpWriter(
                final IConnection<HttpClient> connection,
                final Path filePath) {

            super(connection);
            m_filePath = filePath;
        }

        @Override
        protected void doRequest(final HttpClient client) {

            // Build form params
            final StringBuilder dataUri = new StringBuilder("XMLDATA=");
            dataUri.append(loadFileContents());

            HttpClientRequest request = doPost("/", new HttpClientResponseAdapter() {

                @Override
                protected void handleFailure(final HttpClientResponse response) {
                    if (response.statusCode() != 200) {
                        m_logger.error("Failed to send data to NRDP server: {} (host: {})",
                                dataUri.toString(),
                                getConnection().getAddress(),
                                new IllegalStateException("HTTP post failure: "
                                        + response.statusCode()
                                        + "/"
                                        + response.statusMessage()));
                    }
                }
            });

            byte[] body = dataUri.toString().getBytes();
            request.putHeader(CONTENT_LENGTH, String.valueOf(body.length));
            request.exceptionHandler(new DefaultConnectionExceptionHandler(
                    getConnection()));
            request.write(new Buffer(body));
            request.end();
        }

        private String loadFileContents() {
            try {
                String contents = new String(Files.readAllBytes(m_filePath));
                return URLEncoder.encode(contents, StandardCharsets.UTF_8.name());
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
    }
}
