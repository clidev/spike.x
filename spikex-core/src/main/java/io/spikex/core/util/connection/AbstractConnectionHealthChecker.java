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

import io.spikex.core.util.connection.ConnectionConfig.LoadBalancingDef;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.vertx.java.core.Handler;

/**
 *
 * @param <E>
 * @author cli
 */
public abstract class AbstractConnectionHealthChecker<E extends IConnection>
        implements IConnectionHealthChecker<E> {

    private Handler<Throwable> m_exceptionHandler;

    private final LoadBalancingDef m_lbDef;

    private final Logger m_logger = LoggerFactory.getLogger(getClass());

    public AbstractConnectionHealthChecker(
            final String loadBalancingStrategyName,
            final String statusUri,
            final long checkInterval) {

        m_lbDef = LoadBalancingDef.create(
                loadBalancingStrategyName,
                statusUri,
                checkInterval);
    }

    public AbstractConnectionHealthChecker(final LoadBalancingDef lbDef) {
        m_lbDef = lbDef;
    }

    public void setExceptionHandler(final Handler<Throwable> exceptionHandler) {
        m_exceptionHandler = exceptionHandler;
    }

    @Override
    public void doHealthCheck(
            final E connection,
            final Handler<Boolean> handler) {

        long currentTime = System.currentTimeMillis();
        long lastActivityTime = connection.getLastActivity();
        long checkInterval = m_lbDef.getCheckInterval();
        boolean checkEnabled = m_lbDef.isCheckEnabled();

        if (checkEnabled
                && (lastActivityTime + checkInterval) < currentTime) {

            healthCheck(connection, handler);
        }
    }

    protected abstract void healthCheck(
            E connection,
            Handler<Boolean> handler);

    protected Handler<Throwable> exceptionHandler() {
        return m_exceptionHandler;
    }

    protected LoadBalancingDef loadBalancingDef() {
        return m_lbDef;
    }

    protected Logger logger() {
        return m_logger;
    }
}
