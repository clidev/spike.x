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

import java.net.URI;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.vertx.java.core.Handler;

/**
 *
 * @param <E>
 * @author cli
 */
public class DefaultConnectionExceptionHandler<E extends IConnection>
        implements Handler<Throwable> {

    private final URI m_address;
    private final E m_connection;

    private final Logger m_logger = LoggerFactory.getLogger(DefaultConnectionExceptionHandler.class);

    public DefaultConnectionExceptionHandler(final URI address) {
        m_address = address;
        m_connection = null;
    }

    public DefaultConnectionExceptionHandler(final E connection) {
        m_address = connection.getAddress();
        m_connection = connection;
    }

    public final URI getAddress() {
        return m_address;
    }

    public final E getConnection() {
        return m_connection;
    }

    @Override
    public void handle(Throwable e) {
        //
        // Mark connection as unhealthy...
        //
        E connection = getConnection();
        if (connection != null) {
            connection.disconnect();
            logger().error("Client error for address: {} (marked as disconnected)",
                    getAddress(), e);
        } else {
            logger().error("Client error for address: {}", getAddress(), e);
        }
    }

    protected final Logger logger() {
        return m_logger;
    }
}
