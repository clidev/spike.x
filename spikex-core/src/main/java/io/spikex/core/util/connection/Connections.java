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

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.vertx.java.core.Handler;

/**
 *
 * @param <E>
 * @author cli
 */
public final class Connections<E extends IConnection> {

    private final ILoadBalancingStrategy m_strategy;
    private final IConnectionHealthChecker m_checker;
    private final List<E> m_connections;

    private final Logger m_logger = LoggerFactory.getLogger(Connections.class);

    public Connections() {
        this(new RoundRobinStrategy(), null);
    }

    public Connections(final IConnectionHealthChecker checker) {
        this(new RoundRobinStrategy(), checker);
    }

    public Connections(
            final ILoadBalancingStrategy strategy,
            final IConnectionHealthChecker checker) {

        m_strategy = strategy;
        m_checker = checker;
        m_connections = new CopyOnWriteArrayList();
    }

    public int getAvailableCount() {
        int count = 0;
        for (IConnection connection : m_connections) {
            if (connection.isConnected()) {
                count++;
            }
        }
        return count;
    }

    public List<E> getConnections() {
        return m_connections;
    }

    public E next() throws ConnectionException {
        //
        // Skip unhealthy connections...
        //
        List<E> connections = m_connections;
        ILoadBalancingStrategy<E> strategy = m_strategy;
        E connection = null;
        int i = connections.size();

        while ((connection = strategy.next()) != null
                && (i > 0)) {

            if (connection.isConnected()) {
                break; // Found healthy connection
            }

            i--;
        }

        //
        // Uh, oh, no connection found
        //
        if (connection == null || i <= 0) {
            throw new ConnectionException("No healthy connections found: "
                    + connections);
        }

        return connection;
    }

    public void addConnection(final E connection) {
        m_connections.add(connection);
        m_strategy.setConnections(m_connections);
    }

    public void removeConnection(final E connection) {
        m_connections.remove(connection);
        m_strategy.setConnections(m_connections);
    }

    public void doHealthChecks() {
        doHealthChecks(null);
    }

    public void doHealthChecks(final Handler<Boolean> handler) {
        m_logger.trace("Performing health check of connections: {}", m_connections);
        for (E connection : m_connections) {
            m_checker.doHealthCheck(connection, handler);
        }
    }

    public void disconnectAll() {
        for (E connection : m_connections) {
            if (connection.isConnected()) {
                m_logger.trace("Disconnecting from: {}", connection.getAddress());
                connection.disconnect();
            }
        }
    }
}
