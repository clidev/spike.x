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

import io.spikex.core.AbstractFilter;
import io.spikex.core.util.connection.ConnectionConfig.LoadBalancingDef;
import io.spikex.core.util.connection.ConnectionConfig.NodesDef;
import org.vertx.java.core.Handler;

/**
 *
 * @param <E>
 * @author cli
 */
public abstract class AsbtractClient<E extends IConnection> extends AbstractFilter {

    private NodesDef m_nodes;
    private Connections<E> m_connections;
    private long m_timerId;
    private boolean m_started;

    @Override
    protected void startFilter() {
        m_nodes = new ConnectionConfig().parse(config(), variables());
        m_connections = buildConnections();
        m_started = false;
        //
        // Start client immediately if health checks are disabled
        //
        LoadBalancingDef lb = m_nodes.getLoadBalancing();
        if (!lb.isCheckEnabled()) {
            logger().trace("Starting {} client", getClass().getName());
            startClient();
            m_started = true;
        } else {
            // Perform initial health check
            doHealthChecks();
        }
        //
        // Start periodic health checks if enabled
        //
        m_timerId = 0L;
        if (lb.isCheckEnabled()) {
            vertx.setPeriodic(lb.getCheckInterval(), new Handler<Long>() {

                @Override
                public void handle(final Long timerId) {
                    doHealthChecks();
                }
            });
        }
    }

    @Override
    protected void stopFilter() {
        //
        // Stop client and disconnect connections
        //
        vertx.cancelTimer(m_timerId);
        stopClient();
        m_connections.disconnectAll();
    }

    protected final boolean isStarted() {
        return m_started;
    }

    protected final NodesDef nodes() {
        return m_nodes;
    }

    protected final Connections<E> connections() {
        return m_connections;
    }

    protected abstract Connections<E> buildConnections();

    protected abstract IConnectionHealthChecker<E> healthChecker(LoadBalancingDef lbDef);

    protected void startClient() {
        // Do nothing by default;
    }

    protected void stopClient() {
        // Do nothing by default;
    }

    private void doHealthChecks() {
        m_connections.doHealthChecks(new Handler<Boolean>() {

            @Override
            public void handle(Boolean success) {
                if (success && !m_started) {
                    //
                    // Start client after successful health check
                    //
                    logger().trace("Starting client");
                    startClient();
                    m_started = true;
                }
            }
        });
    }
}
