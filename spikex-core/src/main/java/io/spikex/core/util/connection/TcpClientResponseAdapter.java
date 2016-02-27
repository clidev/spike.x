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

import java.util.concurrent.atomic.AtomicReference;
import org.vertx.java.core.AsyncResult;
import org.vertx.java.core.Handler;
import org.vertx.java.core.net.NetSocket;

/**
 *
 * @author cli
 */
public class TcpClientResponseAdapter implements Handler<AsyncResult<NetSocket>> {

    private final AtomicReference<TcpConnection> m_connection;

    public TcpClientResponseAdapter() {
        m_connection = new AtomicReference();
    }

    public final void setConnection(final TcpConnection connection) {
        m_connection.set(connection);
    }

    @Override
    public void handle(final AsyncResult<NetSocket> response) {
        if (response.failed()) {
            handleFailure(response);
        } else {
            handleSuccess(response);
        }
    }

    protected void handleSuccess(final AsyncResult<NetSocket> response) {
        // Do nothing by default...
    }

    protected void handleFailure(final AsyncResult<NetSocket> response) {
        // Do nothing by default...
    }

    protected final TcpConnection connection() {
        return m_connection.get();
    }
}
