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
package io.spikex.core.util.connection;

import org.vertx.java.core.AsyncResult;
import org.vertx.java.core.AsyncResultHandler;
import org.vertx.java.core.Handler;
import org.vertx.java.core.buffer.Buffer;
import org.vertx.java.core.net.NetClient;
import org.vertx.java.core.net.NetSocket;

/**
 *
 * @author cli
 */
public class NetClientAdapter implements AsyncResultHandler<NetSocket> {

    private final IConnection<NetClient> m_connection;

    public NetClientAdapter(final IConnection<NetClient> connection) {
        m_connection = connection;
    }

    public final IConnection<NetClient> getConnection() {
        return m_connection;
    }

    @Override
    public final void handle(AsyncResult<NetSocket> result) {
        if (result.succeeded()) {
            handleConnectSuccess(result.result());
        } else {
            handleConnectFailure(result.cause());
        }
    }

    protected void handleConnectSuccess(final NetSocket socket) {
        socket.dataHandler(new Handler<Buffer>() {

            @Override
            public void handle(final Buffer buffer) {
                handleReceivedData(socket, buffer);
            }
        });
        sendData(socket);
    }

    protected void handleConnectFailure(final Throwable cause) {
        // Do nothing by default
    }

    protected void handleReceivedData(
            final NetSocket socket,
            final Buffer buffer) {
        // Do nothing by default
    }

    protected void sendData(final NetSocket socket) {
        // Do nothing by default
    }

    protected void disconnect() {
        m_connection.disconnect();
    }
}
