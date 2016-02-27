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

import com.google.common.base.Strings;
import io.spikex.core.util.connection.ConnectionConfig.NodesDef;
import java.net.URI;
import org.vertx.java.core.Handler;
import org.vertx.java.core.Vertx;
import org.vertx.java.core.net.NetClient;

/**
 *
 * TODO fix javadocs TODO add sanity checks
 *
 * @author cli
 */
public final class TcpConnection extends AbstractConnection<NetClient> {

    private volatile NetClient m_client;
    private final Vertx m_vertx;

    @Override
    public NetClient getClient() {
        return m_client;
    }

    @Override
    public void doRequest(final Handler handler) {
        URI address = getAddress();
        try {
            logger().trace("Sending request to {}:{}", address.getHost(), address.getPort());
            m_client.connect(address.getPort(), address.getHost(), handler);
        } catch (Exception e) {
            logger().error("Failed to communicate with {}:{}",
                    address.getHost(), address.getPort(), e);
        }
    }

    @Override
    public void disconnect() {
        URI address = getAddress();
        if (isConnected()) {
            try {
                NetClient client = copyClient(); // Make a copy just before close
                m_client.close();
                m_client = client;
                setConnected(false);
                logger().debug("Disconnected from {}:{}",
                        address.getHost(), address.getPort());
            } catch (Exception e) {
                logger().error("Failed to disconnect from {}:{}",
                        address.getHost(), address.getPort(), e);
            }
        }
    }

    @Override
    protected NetClient copyClient() {
        return builder(getAddress(), m_vertx)
                .connectTimeout(getConnectTimeout())
                .reconnectAttempts(m_client.getReconnectAttempts())
                .reconnectInterval(m_client.getReconnectInterval())
                .keepAlive(m_client.isTCPKeepAlive())
                .sslEnabled(m_client.isSSL())
                .keystorePath(m_client.getKeyStorePath())
                .keystorePassword(m_client.getKeyStorePassword())
                .truststorePath(m_client.getTrustStorePath())
                .truststorePassword(m_client.getTrustStorePassword())
                .buildClient();
    }

    public static Builder builder(
            final URI address,
            final Vertx vertx) {

        return new Builder(address, vertx);
    }

    public static final class Builder extends AbstractConnection.Builder<Builder, TcpConnection> {

        private final Vertx m_vertx;
        private boolean m_keepAlive;
        private boolean m_sslEnabled;
        private String m_keystorePath;
        private String m_keystorePassword;
//        private String m_keystoreType;
        private String m_truststorePath;
        private String m_truststorePassword;
//        private String m_truststoreType;

        private Builder(
                final URI address,
                final Vertx vertx) {

            super(address);
            m_vertx = vertx;
        }

        public Builder keepAlive(final boolean keepAlive) {
            m_keepAlive = keepAlive;
            return this;
        }

        public Builder sslEnabled(final boolean sslEnabled) {
            m_sslEnabled = sslEnabled;
            return this;
        }

        public Builder keystorePath(final String keystorePath) {
            m_keystorePath = keystorePath;
            return this;
        }

        public Builder keystorePassword(final String keystorePassword) {
            m_keystorePassword = keystorePassword;
            return this;
        }

        public Builder truststorePath(final String truststorePath) {
            m_truststorePath = truststorePath;
            return this;
        }

        public Builder truststorePassword(final String truststorePassword) {
            m_truststorePassword = truststorePassword;
            return this;
        }

        public Builder config(final NodesDef def) {
            m_keepAlive = def.isKeepAlive();
            m_sslEnabled = def.isSslEnabled();
            m_keystorePath = def.getKeystorePath();
            m_keystorePassword = def.getKeystorePassword();
            m_truststorePath = def.getTruststorePath();
            m_truststorePassword = def.getTruststorePassword();
            return this;
        }

        @Override
        public TcpConnection build() {
            return new TcpConnection(this);
        }

        public NetClient buildClient() {
            return newClient(this);
        }
    }

    private TcpConnection(final Builder builder) {
        super(builder);
        m_vertx = builder.m_vertx;
        m_client = newClient(builder);
    }

    private static NetClient newClient(final Builder builder) {
        NetClient client = builder.m_vertx.createNetClient();
        client.setConnectTimeout((int) builder.m_connectTimeout);
        client.setTCPKeepAlive(builder.m_keepAlive);
        client.setSSL(builder.m_sslEnabled);
        if (builder.m_reconnectAttempts > 0) {
            client.setReconnectAttempts(builder.m_reconnectAttempts);
        }
        if (builder.m_reconnectInterval > 0L) {
            client.setReconnectInterval(builder.m_reconnectInterval);
        }
        if (!Strings.isNullOrEmpty(builder.m_keystorePath)) {
            client.setKeyStorePath(builder.m_keystorePath);
        }
        if (!Strings.isNullOrEmpty(builder.m_keystorePassword)) {
            client.setKeyStorePassword(builder.m_keystorePassword);
        }
        if (!Strings.isNullOrEmpty(builder.m_truststorePath)) {
            client.setTrustStorePath(builder.m_truststorePath);
        }
        if (!Strings.isNullOrEmpty(builder.m_truststorePassword)) {
            client.setTrustStorePassword(builder.m_truststorePassword);
        }
        return client;
    }
}
