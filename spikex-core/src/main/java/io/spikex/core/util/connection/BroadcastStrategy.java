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

import com.google.common.base.Preconditions;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import org.vertx.java.core.Handler;

/**
 *
 * @param <E>
 * @author cli
 */
public final class BroadcastStrategy<E extends IConnection> extends AbstractLoadBalancingStrategy<E> {

    @Override
    public void setConnections(final List<E> connections) {
        //
        // Sanity checks
        //
        Preconditions.checkNotNull(connections);
        Preconditions.checkArgument(connections.size() > 0, "Empty connection list");
        //
        // Wrap all connections with our broadcaster connection
        //
        List broadcastConnections = new ArrayList();
        broadcastConnections.add(new BroadcastConnection(connections));
        super.setConnections(broadcastConnections);
    }

    @Override
    public E next() {
        //
        // Simply pick the first and only connection from the list
        //
        List<E> connections = getConnections();
        return connections.get(0); // This is the broadcast connection
    }

    private static class BroadcastConnection<E extends IConnection, V> implements IConnection<V> {

        private final List<E> m_connections;

        private BroadcastConnection(final List<E> connections) {
            m_connections = connections;
        }

        @Override
        public boolean isConnected() {
            // true, if we have one connected connection
            boolean connected = false;
            for (IConnection connection : m_connections) {
                if (connection.isConnected()) {
                    connected = true;
                    break;
                }
            }
            return connected;
        }

        @Override
        public URI getAddress() {
            // Return the first connected address
            URI address = null;
            for (IConnection connection : m_connections) {
                if (connection.isConnected()) {
                    address = connection.getAddress();
                    break;
                }
            }
            return address;
        }

        @Override
        public long getConnectTimeout() {
            return m_connections.get(0).getConnectTimeout();
        }

        @Override
        public long getLastActivity() {
            // Return the activity of the "oldest" connection
            long activity = Long.MAX_VALUE;
            for (IConnection connection : m_connections) {
                long tmp = connection.getLastActivity();
                if (tmp < activity) {
                    activity = tmp;
                }
            }
            return activity;
        }

        @Override
        public V getClient() {
            // Return the client of the first connected connection
            Object client = null;
            for (IConnection connection : m_connections) {
                if (connection.isConnected()) {
                    client = connection.getClient();
                    break;
                }
            }
            return (V) client;
        }

        @Override
        public void doRequest(final Handler handler) {
            // Broadcast doRequest to all connections
            for (IConnection connection : m_connections) {
                connection.doRequest(handler);
            }
        }

        @Override
        public void disconnect() {
            // Do nothing...
        }

        @Override
        public void setConnected(boolean connected) {
            // Do nothing...
        }

        @Override
        public void updateActivity() {
            // Do nothing...
        }
    }
}
