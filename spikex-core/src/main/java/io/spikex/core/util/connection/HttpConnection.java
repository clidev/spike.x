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
import io.spikex.core.util.Base64;
import io.spikex.core.util.connection.ConnectionConfig.NodesDef;
import java.net.URI;
import java.net.URISyntaxException;
import org.vertx.java.core.Handler;
import org.vertx.java.core.Vertx;
import org.vertx.java.core.http.HttpClient;

/**
 *
 * TODO fix javadocs TODO add sanity checks
 *
 * @author cli
 */
public final class HttpConnection extends AbstractConnection<HttpClient> {

    private volatile HttpClient m_client;
    private final Vertx m_vertx;
    private final Handler<Throwable> m_exceptionHandler;
    private final String m_authBase64;

    public String getBase64UserAndPassword() {
        return m_authBase64;
    }

    @Override
    public HttpClient getClient() {
        return m_client;
    }

    @Override
    public void doRequest(final Handler handler) {
        URI address = getAddress();
        try {
            logger().trace("Sending request to {}:{}", address.getHost(), address.getPort());
            //
            // Trigger handler to do a HTTP request
            //
            handler.handle(m_client);
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
                HttpClient client = copyClient(); // Make a copy just before close
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
    protected HttpClient copyClient() {
        return builder(getAddress(), m_vertx)
                .connectTimeout(getConnectTimeout())
                .keepAlive(m_client.isKeepAlive())
                .useCompression(m_client.getTryUseCompression())
                .verifyHost(m_client.isVerifyHost())
                .sslEnabled(m_client.isSSL())
                .pipelining(m_client.isPipelining())
                .maxPoolSize(m_client.getMaxPoolSize())
                .exceptionHandler(m_exceptionHandler)
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

    public static final class Builder extends AbstractConnection.Builder<Builder, HttpConnection> {

        private final Vertx m_vertx;

        private boolean m_keepAlive;
        private boolean m_useCompression;
        private boolean m_verifyHost;
        private boolean m_sslEnabled;
        private boolean m_pipelining;
        private int m_maxPoolSize;
        private Handler<Throwable> m_exceptionHandler;
        private String m_keystorePath;
        private String m_keystorePassword;
//        private String m_keystoreType;
        private String m_truststorePath;
        private String m_truststorePassword;
//        private String m_truststoreType;

        private Builder(
                final URI address,
                final Vertx vertx) {

            super(HttpConnection.initAddress(address));
            m_vertx = vertx;
            m_maxPoolSize = 1; // The default

            //
            // Turn on SSL if HTTPS
            //
            if ("https".equalsIgnoreCase(address.getScheme())) {
                m_sslEnabled = true;
            }
        }

        public Builder keepAlive(final boolean keepAlive) {
            m_keepAlive = keepAlive;
            return this;
        }

        public Builder useCompression(final boolean useCompression) {
            m_useCompression = useCompression;
            return this;
        }

        public Builder verifyHost(final boolean verifyHost) {
            m_verifyHost = verifyHost;
            return this;
        }

        public Builder sslEnabled(final boolean sslEnabled) {
            m_sslEnabled = sslEnabled;
            return this;
        }

        public Builder pipelining(final boolean pipelining) {
            m_pipelining = pipelining;
            return this;
        }

        public Builder maxPoolSize(final int maxPoolSize) {
            m_maxPoolSize = maxPoolSize;
            return this;
        }

        public Builder exceptionHandler(final Handler<Throwable> exceptionHandler) {
            m_exceptionHandler = exceptionHandler;
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
            m_useCompression = def.isUseCompression();
            m_verifyHost = def.isVerifyHost();
            m_sslEnabled = def.isSslEnabled();
            m_pipelining = def.isPipelining();
            m_maxPoolSize = def.getMaxPoolSize();
            m_keystorePath = def.getKeystorePath();
            m_keystorePassword = def.getKeystorePassword();
            m_truststorePath = def.getTruststorePath();
            m_truststorePassword = def.getTruststorePassword();
            return this;
        }

        @Override
        public HttpConnection build() {
            return new HttpConnection(this);
        }

        public HttpClient buildClient() {
            return newClient(this);
        }
    }

    private HttpConnection(final Builder builder) {
        super(builder);
        m_vertx = builder.m_vertx;
        m_client = newClient(builder);
        m_exceptionHandler = builder.m_exceptionHandler;
        //
        // Basic authentication (calculate in advance)
        //
        m_authBase64 = base64UserAndPassword(getAddress());
        //
        // Output some SSL info
        //
        if (m_client.isSSL()) {
            logger().trace("Using key store: {}", m_client.getKeyStorePath());
            logger().trace("Using trust store: {}", m_client.getTrustStorePath());
        }
    }

    private String base64UserAndPassword(final URI address) {

        String encoded = "";
        String userInfo = address.getUserInfo(); // joe:pwd
        if (!Strings.isNullOrEmpty(userInfo)) {
            encoded = Base64.encodeBytes(userInfo.getBytes());
        }
        return encoded;
    }

    private static HttpClient newClient(final Builder builder) {
        HttpClient client = builder.m_vertx.createHttpClient();
        URI address = builder.m_address;
        client.setHost(address.getHost());
        client.setPort(address.getPort());
        client.setConnectTimeout((int) builder.m_connectTimeout);
        client.setKeepAlive(builder.m_keepAlive);
        client.setTryUseCompression(builder.m_useCompression);
        client.setVerifyHost(builder.m_verifyHost);
        client.setSSL(builder.m_sslEnabled);
        client.setPipelining(builder.m_pipelining);
        client.setMaxPoolSize(builder.m_maxPoolSize);
        if (builder.m_exceptionHandler != null) {
            client.exceptionHandler(builder.m_exceptionHandler);
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

    private static URI initAddress(URI address) {
        //
        // Use port 80 or 443 if not defined
        //
        if (address.getPort() == -1) {
            try {
                if ("http".equalsIgnoreCase(address.getScheme())) {
                    address = new URI(
                            address.getScheme(),
                            address.getUserInfo(),
                            address.getHost(),
                            80,
                            address.getPath(),
                            address.getQuery(),
                            address.getFragment());
                } else if ("https".equalsIgnoreCase(address.getScheme())) {
                    address = new URI(
                            address.getScheme(),
                            address.getUserInfo(),
                            address.getHost(),
                            443,
                            address.getPath(),
                            address.getQuery(),
                            address.getFragment());
                }
            } catch (URISyntaxException e) {
                throw new RuntimeException("Could not assign port 80 or 443 "
                        + "to address: " + address, e);
            }
        }
        return address;
    }
}
