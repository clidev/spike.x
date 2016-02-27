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
package io.spikex.core.integration;

import static io.spikex.core.AbstractFilter.CONF_KEY_CHAIN_NAME;
import static io.spikex.core.AbstractFilter.CONF_KEY_SOURCE_ADDRESS;
import static io.spikex.core.AbstractVerticle.CONF_KEY_CLUSTER_NAME;
import static io.spikex.core.AbstractVerticle.CONF_KEY_CONF_PATH;
import static io.spikex.core.AbstractVerticle.CONF_KEY_DATA_PATH;
import static io.spikex.core.AbstractVerticle.CONF_KEY_HOME_PATH;
import static io.spikex.core.AbstractVerticle.CONF_KEY_LOCAL_ADDRESS;
import static io.spikex.core.AbstractVerticle.CONF_KEY_NODE_NAME;
import static io.spikex.core.AbstractVerticle.CONF_KEY_TMP_PATH;
import static io.spikex.core.AbstractVerticle.CONF_KEY_UPDATE_INTERVAL;
import static io.spikex.core.AbstractVerticle.CONF_KEY_USER;
import io.spikex.core.util.connection.ConnectionConfig;
import static io.spikex.core.util.connection.ConnectionConfig.LB_STRATEGY_BROADCAST;
import static io.spikex.core.util.connection.ConnectionConfig.LB_STRATEGY_ROUND_ROBIN;
import io.spikex.core.util.connection.ConnectionConfig.NodesDef;
import java.io.IOException;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.vertx.java.core.Handler;
import org.vertx.java.core.buffer.Buffer;
import org.vertx.java.core.http.HttpServer;
import org.vertx.java.core.http.HttpServerRequest;
import org.vertx.java.core.json.JsonObject;
import org.vertx.java.core.net.NetServer;
import org.vertx.java.core.net.NetSocket;
import org.vertx.testtools.TestVerticle;

/**
 *
 * @author cli
 */
public class ConnectionsTest extends TestVerticle {

    private final Logger m_logger = LoggerFactory.getLogger(ConnectionsTest.class);

    @Test
    public void testTcpConnection() throws IOException {

        // Start local TCP servers
        URI address1 = URI.create("telnet://localhost:52210");
        startLocalTcpServer(address1);

        URI address2 = URI.create("telnet://localhost:52214");
        startLocalTcpServer(address2);

        URI address3 = URI.create("telnet://localhost:52220");
        startLocalTcpServer(address3);

        // Non-existent server
        URI address4 = URI.create("telnet://localhost:52230");

        // Create connection configuration
        List<URI> addresses = new ArrayList();
        addresses.add(address1);
        addresses.add(address2);
        addresses.add(address3);
        addresses.add(address4);
        NodesDef nodes = ConnectionConfig.builder(addresses)
                .loadBalancing(LB_STRATEGY_ROUND_ROBIN, 1000L)
                .build();

        // Start worker verticle that communicates with TCP echo servers
        JsonObject config = new JsonObject();
        config.mergeIn(nodes.toJson());
        config.putString(CONF_KEY_CHAIN_NAME, "tcpconnection-test");
        config.putString(CONF_KEY_LOCAL_ADDRESS, "my-local-address");
        config.putString(CONF_KEY_SOURCE_ADDRESS, "my-global-address");
        config.putString(CONF_KEY_NODE_NAME, "node-name");
        config.putString(CONF_KEY_CLUSTER_NAME, "cluster-name");
        config.putString(CONF_KEY_HOME_PATH, "build");
        config.putString(CONF_KEY_CONF_PATH, "build/resources/test");
        config.putString(CONF_KEY_DATA_PATH, "build");
        config.putString(CONF_KEY_TMP_PATH, "build");
        config.putString(CONF_KEY_USER, "spikex");
        getContainer().deployWorkerVerticle(TestEchoClient.class.getName(), config);
    }

    @Test
    public void testHttpConnection() throws IOException {

        // Start local HTTP servers
        URI address1 = URI.create("http://localhost:52410");
        startLocalHttpServer(address1);

        URI address2 = URI.create("http://localhost:52414");
        startLocalHttpServer(address2);

        URI address3 = URI.create("http://localhost:52420");
        startLocalHttpServer(address3);

        // Non-existent server
        URI address4 = URI.create("http://localhost:52429");

        // Create connection configuration
        List<URI> addresses = new ArrayList();
        addresses.add(address1);
        addresses.add(address2);
        addresses.add(address3);
        addresses.add(address4);
        NodesDef nodes = ConnectionConfig.builder(addresses)
                .loadBalancing(LB_STRATEGY_BROADCAST, "/status", 1000L)
                .build();

        // Start worker verticle that communicates with TCP echo servers
        JsonObject config = new JsonObject();
        config.mergeIn(nodes.toJson());
        config.putString(CONF_KEY_CHAIN_NAME, "httpconnection-test");
        config.putString(CONF_KEY_LOCAL_ADDRESS, "my-local-address");
        config.putString(CONF_KEY_SOURCE_ADDRESS, "my-global-address");
        config.putString(CONF_KEY_NODE_NAME, "node-name");
        config.putString(CONF_KEY_CLUSTER_NAME, "cluster-name");
        config.putString(CONF_KEY_HOME_PATH, "build");
        config.putString(CONF_KEY_CONF_PATH, "build/resources/test");
        config.putString(CONF_KEY_DATA_PATH, "build");
        config.putString(CONF_KEY_TMP_PATH, "build");
        config.putString(CONF_KEY_USER, "spikex");
        config.putNumber(CONF_KEY_UPDATE_INTERVAL, 1000L);
        getContainer().deployWorkerVerticle(TestHttpClient.class.getName(), config);
    }

    private void startLocalTcpServer(final URI address) {
        m_logger.info("Listening on {}:{}", address.getHost(), address.getPort());
        NetServer server = vertx.createNetServer();
        server.connectHandler(new Handler<NetSocket>() {
            @Override
            public void handle(final NetSocket socket) {
                m_logger.debug("{} received connection from {}",
                        socket.localAddress(),
                        socket.remoteAddress());
                socket.dataHandler(new Handler<Buffer>() {
                    @Override
                    public void handle(Buffer buffer) {
                        socket.write(buffer);
                    }
                });
            }
        }).listen(address.getPort(), address.getHost());
    }

    private void startLocalHttpServer(final URI address) {

        String host = address.getHost();
        int port = address.getPort();
        m_logger.info("Listening on {}:{}", host, port);
        HttpServer server = vertx.createHttpServer();

        server.requestHandler(new Handler<HttpServerRequest>() {
            @Override
            public void handle(final HttpServerRequest request) {
                m_logger.debug("{} received request from {}",
                        request.localAddress(),
                        request.remoteAddress());
                request.bodyHandler(new Handler<Buffer>() {
                    @Override
                    public void handle(final Buffer body) {
                        m_logger.info("{} client data: {}",
                                request.localAddress(),
                                body.toString(StandardCharsets.UTF_8.name()));
                    }
                });
                request.response().setStatusCode(200).end();
            }
        }).listen(port, host);
    }
}
