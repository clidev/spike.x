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
import io.spikex.core.util.IBuilder;
import java.net.URI;
import java.util.concurrent.atomic.AtomicLong;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.vertx.java.core.Handler;

/**
 * Abstract base class for connection implementations.
 *
 * TODO fix javadocs TODO add sanity checks
 *
 *
 * @param <V>
 *
 * @author cli
 */
public abstract class AbstractConnection<V> implements IConnection<V> {

    private final URI m_address; // The remote host address
    private final int m_reconnectAttempts;
    private final long m_connectTimeout; // Connection timeout
    private final long m_reconnectInterval;
    private final AtomicLong m_connectCount; // Total amount of connects (successful & failed)
    //
    private volatile boolean m_connected;
    private volatile long m_lastActivity; // ms since epoch
    private volatile int m_hashCode;
    //
    private final Logger m_logger = LoggerFactory.getLogger(getClass());

    private AbstractConnection(
            final URI address,
            final long connectTimeout,
            final int reconnectAttempts,
            final long reconnectInterval) {
        //
        // Sanity checks
        //
        Preconditions.checkNotNull(address, "Connection addrerss is null");
        Preconditions.checkArgument(connectTimeout >= 0,
                "Connect timeout is less than zero");
        Preconditions.checkArgument(reconnectAttempts >= 0,
                "Connection reconnect attempts is less than zero");
        Preconditions.checkArgument(reconnectInterval >= 0,
                "Connection reconnect interval is less than zero");
        //
        m_address = address;
        m_connectTimeout = connectTimeout;
        m_reconnectAttempts = reconnectAttempts;
        m_reconnectInterval = reconnectInterval;
        m_connected = false;
        m_lastActivity = System.currentTimeMillis();
        m_connectCount = new AtomicLong(0L);
    }

    /**
     * Please see {@link java.lang.Object#equals} for documentation.
     */
    @Override
    public boolean equals(final Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null) {
            return false;
        }
        if (obj.getClass() != getClass()) {
            return false;
        }
        return (hashCode() == obj.hashCode());
    }

    /**
     * Please see {@link java.lang.Object#hashCode} for documentation.
     */
    @Override
    public int hashCode() {
        // Racy single-check is acceptable for hashCode
        // [Effective Java, Joshua Block, Item 71]
        int hashCode = m_hashCode;
        if (hashCode == 0) {
            hashCode = 21;
            hashCode = 3 * hashCode + m_address.hashCode();
            m_hashCode = hashCode;
        }
        return hashCode;
    }

    @Override
    public final boolean isConnected() {
        return m_connected;
    }

    @Override
    public final URI getAddress() {
        return m_address;
    }

    @Override
    public final long getConnectTimeout() {
        return m_connectTimeout;
    }

    @Override
    public final long getLastActivity() {
        return m_lastActivity;
    }

    public final int getReconnectAttempts() {
        return m_reconnectAttempts;
    }

    public final long getReconnectInterval() {
        return m_reconnectInterval;
    }

    public long getConnectCount() {
        return m_connectCount.get();
    }

    @Override
    public abstract V getClient();

    @Override
    public abstract void doRequest(Handler handler);

    @Override
    public abstract void disconnect();

    @Override
    public final void setConnected(final boolean connected) {
        m_connected = connected;
        updateActivity();
        if (connected) {
            m_connectCount.incrementAndGet();
        }
    }

    @Override
    public void updateActivity() {
        m_lastActivity = System.currentTimeMillis();
    }

    @Override
    public String toString() {
        return getAddress().toString();
    }

    protected Logger logger() {
        return m_logger;
    }

    protected abstract V copyClient();

    protected abstract static class Builder<E extends Builder, T extends AbstractConnection>
            implements IBuilder<T> {

        protected URI m_address;
        protected int m_reconnectAttempts;
        protected long m_connectTimeout;
        protected long m_reconnectInterval;

        protected Builder(final URI address) {
            m_address = address;
            m_connectTimeout = 0L;
            m_reconnectInterval = 0L;
            m_reconnectAttempts = 0;
        }

        public final E connectTimeout(final long timeout) {
            m_connectTimeout = timeout;
            return (E) this;
        }

        public final E reconnectAttempts(final int reconnectAttempts) {
            m_reconnectAttempts = reconnectAttempts;
            return (E) this;
        }

        public final E reconnectInterval(final long reconnectInterval) {
            m_reconnectInterval = reconnectInterval;
            return (E) this;
        }
    }

    protected AbstractConnection(final Builder builder) {
        this(builder.m_address,
                builder.m_connectTimeout,
                builder.m_reconnectAttempts,
                builder.m_reconnectInterval);
    }
}
