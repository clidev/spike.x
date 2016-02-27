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

import io.spikex.core.util.connection.AbstractConnectionHealthChecker;
import io.spikex.core.util.connection.AsbtractTcpClient;
import io.spikex.core.util.connection.ConnectionConfig.LoadBalancingDef;
import io.spikex.core.util.connection.ConnectionException;
import io.spikex.core.util.connection.IConnection;
import io.spikex.core.util.connection.IConnectionHealthChecker;
import io.spikex.core.util.connection.NetClientAdapter;
import io.spikex.core.util.connection.TcpConnection;
import static java.lang.Boolean.FALSE;
import static java.lang.Boolean.TRUE;
import java.nio.charset.StandardCharsets;
import org.vertx.java.core.Handler;
import org.vertx.java.core.buffer.Buffer;
import org.vertx.java.core.net.NetClient;
import org.vertx.java.core.net.NetSocket;
import org.vertx.testtools.VertxAssert;

/**
 *
 * @author cli
 */
public final class TestEchoClient extends AsbtractTcpClient {

    private int m_counter;
    private long m_timerId;

    @Override
    protected void startClient() {
        logger().info("Configuration: {}", config());
        vertx.setPeriodic(300L, new Handler<Long>() {

            @Override
            public void handle(final Long timerId) {
                try {
                    m_counter++;
                    sendData("Hello again " + m_counter, connections().next(), null);
                } catch (ConnectionException e) {
                    logger().error("Failed to send data to echo server", e);
                }
            }
        });
    }

    @Override
    protected void stopClient() {
        vertx.cancelTimer(m_timerId);
    }

    @Override
    protected IConnectionHealthChecker<TcpConnection> healthChecker(final LoadBalancingDef lbDef) {
        return new AbstractConnectionHealthChecker<TcpConnection>(nodes().getLoadBalancing()) {

            @Override
            protected void healthCheck(
                    final TcpConnection connection,
                    final Handler<Boolean> handler) {

                connection.setConnected(true); // Assume OK
                sendData("Health check", connection, handler);
            }
        };
    }

    private void sendData(
            final String message,
            final IConnection<NetClient> connection,
            final Handler<Boolean> handler) {

        NetClientAdapter adapter = new NetClientAdapter(connection) {

            @Override
            protected void handleReceivedData(
                    final NetSocket socket,
                    final Buffer buffer) {

                connection.setConnected(true); // Update state
                if (handler != null) {
                    handler.handle(TRUE);
                }

                logger().info("{} answered: {}",
                        socket.remoteAddress(),
                        buffer.toString(StandardCharsets.UTF_8.name()));

                if (m_counter > 30) {
                    // Stop test
                    disconnect();
                    VertxAssert.testComplete();
                }
            }

            @Override
            protected void handleConnectFailure(final Throwable cause) {
                logger().error("Failed to connect to: {} (marking as disconnected)", connection);
                connection.setConnected(false); // Update state
                disconnect();
                if (handler != null) {
                    handler.handle(FALSE);
                }
            }

            @Override
            protected void sendData(final NetSocket socket) {
                socket.write(message + " from " + socket.localAddress());
            }
        };

        connection.doRequest(adapter);
    }
}
