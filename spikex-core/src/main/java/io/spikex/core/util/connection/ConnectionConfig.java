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
import com.google.common.base.Strings;
import io.spikex.core.helper.Variables;
import io.spikex.core.util.IBuilder;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import org.vertx.java.core.json.JsonArray;
import org.vertx.java.core.json.JsonObject;

/**
 *
 * @author cli
 */
public final class ConnectionConfig {

    //
    // Load balancing strategies
    //
    public static final String LB_STRATEGY_ROUND_ROBIN = "round-robin";
    public static final String LB_STRATEGY_BROADCAST = "broadcast";

    private static final String CONF_NODES = "nodes";
    private static final String CONF_KEEP_ALIVE = "keep-alive";
    private static final String CONF_CONNECT_TIMEOUT = "connect-timeout";
    private static final String CONF_PIPELINING = "pipelining";
    private static final String CONF_SSL_ENABLED = "ssl-enabled";
    private static final String CONF_KEYSTORE_PATH = "keystore-path";
    private static final String CONF_KEYSTORE_PASSWORD = "keystore-password";
    private static final String CONF_KEYSTORE_TYPE = "keystore-type";
    private static final String CONF_TRUSTSTORE_PATH = "truststore-path";
    private static final String CONF_TRUSTSTORE_PASSWORD = "truststore-password";
    private static final String CONF_TRUSTSTORE_TYPE = "truststore-type";
    private static final String CONF_USE_COMPRESSION = "use-compression";
    private static final String CONF_VERIFY_HOST = "verify-host";
    private static final String CONF_MAX_POOL_SIZE = "max-pool-size";
    private static final String CONF_LOAD_BALANCING = "load-balancing";
    private static final String CONF_STRATEGY = "strategy";
    private static final String CONF_CHECK_INTERVAL = "check-interval";
    private static final String CONF_STATUS_URI = "status-uri";
    private static final String CONF_RECONNECT_ATTEMPTS = "reconnect-attempts";
    private static final String CONF_RECONNECT_INTERVAL = "reconnect-interval";

    //
    // Configuration defaults
    //
    private static final boolean DEF_KEEP_ALIVE = true;
    private static final boolean DEF_USE_COMPRESSION = true;
    private static final boolean DEF_PIPELINING = false;
    private static final boolean DEF_SSL_ENABLED = false;
    private static final boolean DEF_VERIFY_HOST = true;
    private static final int DEF_CONNECT_TIMEOUT = 250;
    private static final int DEF_MAX_POOL_SIZE = 1;
    private static final int DEF_RECONNECT_ATTEMPTS = 0;
    private static final long DEF_RECONNECT_INTERVAL = 1000L;
    private static final long DEF_CHECK_INTERVAL = 5000L;
    private static final String DEF_LOAD_BALANCING_STRATEGY = LB_STRATEGY_ROUND_ROBIN;
    private static final String DEF_KEYSTORE_TYPE = "JKS";
    private static final String DEF_TRUSTSTORE_TYPE = "JKS";
    private static final String DEF_STATUS_URI = "/";

    public NodesDef parse(
            final JsonObject config,
            final Variables variables) {
        return parse(config, variables, new String[0]);
    }

    public NodesDef parse(
            final JsonObject config,
            final Variables variables,
            final String[] defNodes) {

        List<URI> nodes = new ArrayList();
        JsonArray jsonNodes = config.getArray(CONF_NODES, new JsonArray(defNodes));
        for (int i = 0; i < jsonNodes.size(); i++) {
            String address = jsonNodes.get(i);
            URI uri = URI.create(address);
            // Sanity checks
            Preconditions.checkArgument(!Strings.isNullOrEmpty(uri.getHost()),
                    "Host is missing from address: " + address);
            Preconditions.checkArgument(!Strings.isNullOrEmpty(uri.getScheme()),
                    "Scheme is missing from address: " + address);
            nodes.add(uri);
        }

        NodesDef connection = new NodesDef(
                nodes,
                config.getBoolean(CONF_KEEP_ALIVE, DEF_KEEP_ALIVE),
                config.getBoolean(CONF_USE_COMPRESSION, DEF_USE_COMPRESSION),
                config.getBoolean(CONF_VERIFY_HOST, DEF_VERIFY_HOST),
                config.getBoolean(CONF_SSL_ENABLED, DEF_SSL_ENABLED),
                config.getBoolean(CONF_PIPELINING, DEF_PIPELINING),
                config.getInteger(CONF_CONNECT_TIMEOUT, DEF_CONNECT_TIMEOUT),
                config.getInteger(CONF_MAX_POOL_SIZE, DEF_MAX_POOL_SIZE),
                config.getInteger(CONF_RECONNECT_ATTEMPTS, DEF_RECONNECT_ATTEMPTS),
                config.getLong(CONF_RECONNECT_INTERVAL, DEF_RECONNECT_INTERVAL));

        LoadBalancingDef loadBalancing;

        if (config.containsField(CONF_LOAD_BALANCING)) {

            JsonObject jsonLoadBalancing = config.getObject(CONF_LOAD_BALANCING);

            loadBalancing = new LoadBalancingDef(
                    jsonLoadBalancing.getString(CONF_STRATEGY, DEF_LOAD_BALANCING_STRATEGY),
                    jsonLoadBalancing.getString(CONF_STATUS_URI, DEF_STATUS_URI),
                    jsonLoadBalancing.getLong(CONF_CHECK_INTERVAL, DEF_CHECK_INTERVAL));

        } else {
            loadBalancing = new LoadBalancingDef(
                    DEF_LOAD_BALANCING_STRATEGY,
                    DEF_STATUS_URI,
                    0L); // No health checks
        }

        connection.setLoadBalancingDef(loadBalancing);

        //
        // Key and trust store
        //
        if (config.containsField(CONF_KEYSTORE_PATH)) {
            String path = variables.translate(config.getString(CONF_KEYSTORE_PATH, ""));
            connection.setKeystorePath(path);
            connection.setKeystorePassword(config.getString(CONF_KEYSTORE_PASSWORD));
            connection.setKeystoreType(config.getString(CONF_KEYSTORE_TYPE, DEF_KEYSTORE_TYPE));
        }
        if (config.containsField(CONF_TRUSTSTORE_PATH)) {
            String path = variables.translate(config.getString(CONF_TRUSTSTORE_PATH, ""));
            connection.setTruststorePath(path);
            connection.setTruststorePassword(config.getString(CONF_TRUSTSTORE_PASSWORD));
            connection.setTruststoreType(config.getString(CONF_TRUSTSTORE_TYPE, DEF_TRUSTSTORE_TYPE));
        }

        return connection;
    }

    public static final class NodesDef {

        private final List<URI> m_nodes;
        private final boolean m_keepAlive;
        private final boolean m_useCompression;
        private final boolean m_verifyHost;
        private final boolean m_sslEnabled;
        private final boolean m_pipelining;
        private final int m_connectTimeout;
        private final int m_maxPoolSize;
        private final int m_reconnectAttempts;
        private final long m_reconnectInterval;

        private LoadBalancingDef m_loadBalancingDef;

        private String m_keystorePath;
        private String m_keystorePassword;
        private String m_keystoreType;

        private String m_truststorePath;
        private String m_truststorePassword;
        private String m_truststoreType;

        private NodesDef(
                final List<URI> nodes,
                final boolean keepAlive,
                final boolean useCompression,
                final boolean verifyHost,
                final boolean sslEnabled,
                final boolean pipelining,
                final int connectTimeout,
                final int maxPoolSize,
                final int reconnectAttempts,
                final long reconnectInterval) {

            m_nodes = nodes;
            m_keepAlive = keepAlive;
            m_useCompression = useCompression;
            m_verifyHost = verifyHost;
            m_sslEnabled = sslEnabled;
            m_pipelining = pipelining;
            m_connectTimeout = connectTimeout;
            m_maxPoolSize = maxPoolSize;
            m_reconnectAttempts = reconnectAttempts;
            m_reconnectInterval = reconnectInterval;
        }

        public boolean isKeepAlive() {
            return m_keepAlive;
        }

        public boolean isUseCompression() {
            return m_useCompression;
        }

        public boolean isVerifyHost() {
            return m_verifyHost;
        }

        public boolean isSslEnabled() {
            return m_sslEnabled;
        }

        public boolean isPipelining() {
            return m_pipelining;
        }

        public boolean isLoadBalanced() {
            return (m_loadBalancingDef != null);
        }

        public List<URI> getNodes() {
            return m_nodes;
        }

        public int getConnectTimeout() {
            return m_connectTimeout;
        }

        public int getMaxPoolSize() {
            return m_maxPoolSize;
        }

        public int getReconnectAttempts() {
            return m_reconnectAttempts;
        }

        public long getReconnectInterval() {
            return m_reconnectInterval;
        }

        public LoadBalancingDef getLoadBalancing() {
            return m_loadBalancingDef;
        }

        public String getKeystorePath() {
            return m_keystorePath;
        }

        public String getKeystorePassword() {
            return m_keystorePassword;
        }

        public String getKeystoreType() {
            return m_keystoreType;
        }

        public String getTruststorePath() {
            return m_truststorePath;
        }

        public String getTruststorePassword() {
            return m_truststorePassword;
        }

        public String getTruststoreType() {
            return m_truststoreType;
        }

        public JsonObject toJson() {

            JsonObject json = new JsonObject();

            // Nodes
            JsonArray nodesArray = new JsonArray();
            for (URI node : m_nodes) {
                nodesArray.addString(node.toASCIIString());
            }
            json.putArray(CONF_NODES, nodesArray);

            // Common
            json.putBoolean(CONF_KEEP_ALIVE, m_keepAlive);
            json.putBoolean(CONF_USE_COMPRESSION, m_useCompression);
            json.putBoolean(CONF_VERIFY_HOST, m_verifyHost);
            json.putBoolean(CONF_SSL_ENABLED, m_sslEnabled);
            json.putBoolean(CONF_PIPELINING, m_pipelining);
            json.putNumber(CONF_CONNECT_TIMEOUT, m_connectTimeout);
            json.putNumber(CONF_MAX_POOL_SIZE, m_maxPoolSize);
            json.putNumber(CONF_RECONNECT_ATTEMPTS, m_reconnectAttempts);
            json.putNumber(CONF_RECONNECT_INTERVAL, m_reconnectInterval);

            if (!Strings.isNullOrEmpty(m_keystorePath)) {
                json.putString(CONF_KEYSTORE_PATH, m_keystorePath);
                json.putString(CONF_KEYSTORE_PASSWORD, m_keystorePassword);
                json.putString(CONF_KEYSTORE_TYPE, m_keystoreType);
            }
            if (!Strings.isNullOrEmpty(m_truststorePath)) {
                json.putString(CONF_TRUSTSTORE_PATH, m_truststorePath);
                json.putString(CONF_TRUSTSTORE_PASSWORD, m_truststorePassword);
                json.putString(CONF_TRUSTSTORE_TYPE, m_truststoreType);

            }

            // Load balancing
            JsonObject loadBalancing = new JsonObject();
            loadBalancing.putString(CONF_STRATEGY, m_loadBalancingDef.m_strategyName);
            loadBalancing.putString(CONF_STATUS_URI, m_loadBalancingDef.m_statusUri);
            loadBalancing.putNumber(CONF_CHECK_INTERVAL, m_loadBalancingDef.m_checkInterval);
            json.putObject(CONF_LOAD_BALANCING, loadBalancing);

            return json;
        }

        private void setLoadBalancingDef(final LoadBalancingDef loadBalancingDef) {
            m_loadBalancingDef = loadBalancingDef;
        }

        private void setKeystorePath(final String path) {
            m_keystorePath = path;
        }

        private void setKeystorePassword(final String password) {
            m_keystorePassword = password;
        }

        private void setKeystoreType(final String type) {
            m_keystoreType = type;
        }

        private void setTruststorePath(final String path) {
            m_truststorePath = path;
        }

        private void setTruststorePassword(final String password) {
            m_truststorePassword = password;
        }

        private void setTruststoreType(final String type) {
            m_truststoreType = type;
        }

        private NodesDef(final Builder builder) {
            m_nodes = builder.m_nodes;
            m_keepAlive = builder.m_keepAlive;
            m_useCompression = builder.m_useCompression;
            m_verifyHost = builder.m_verifyHost;
            m_sslEnabled = builder.m_sslEnabled;
            m_pipelining = builder.m_pipelining;
            m_connectTimeout = builder.m_connectTimeout;
            m_maxPoolSize = builder.m_maxPoolSize;
            m_reconnectAttempts = builder.m_reconnectAttempts;
            m_reconnectInterval = builder.m_reconnectInterval;
            m_loadBalancingDef = builder.m_loadBalancingDef;

            m_keystorePath = builder.m_keystorePath;
            m_keystorePassword = builder.m_keystorePassword;
            m_keystoreType = builder.m_keystoreType;

            m_truststorePath = builder.m_truststorePath;
            m_truststorePassword = builder.m_truststorePassword;
            m_truststoreType = builder.m_truststoreType;
        }
    }

    public static final class LoadBalancingDef {

        private final String m_strategyName;
        private final String m_statusUri;
        private final long m_checkInterval;

        private LoadBalancingDef(
                final String strategyName,
                final String statusUri,
                final long checkInterval) {

            m_strategyName = strategyName;
            m_statusUri = statusUri;
            m_checkInterval = checkInterval;
        }

        public boolean isCheckEnabled() {
            return (m_checkInterval > 0L);
        }

        public String getStrategyName() {
            return m_strategyName;
        }

        public String getStatusUri() {
            return m_statusUri;
        }

        public long getCheckInterval() {
            return m_checkInterval;
        }

        public ILoadBalancingStrategy getStrategy() {

            ILoadBalancingStrategy strategy;

            switch (getStrategyName()) {
                case LB_STRATEGY_ROUND_ROBIN:
                    strategy = new RoundRobinStrategy();
                    break;
                case LB_STRATEGY_BROADCAST:
                    strategy = new BroadcastStrategy();
                    break;
                default:
                    strategy = new RoundRobinStrategy();
                    break;
            }

            return strategy;
        }

        public static LoadBalancingDef create(
                final String loadBalancingStrategyName,
                final String statusUri,
                final long checkInterval) {

            return new LoadBalancingDef(
                    loadBalancingStrategyName,
                    statusUri,
                    checkInterval);
        }
    }

    public static Builder builder(final List<URI> nodes) {
        return new Builder(nodes);
    }

    public static final class Builder implements IBuilder<NodesDef> {

        private final List<URI> m_nodes;
        private boolean m_keepAlive;
        private boolean m_useCompression;
        private boolean m_verifyHost;
        private boolean m_sslEnabled;
        private boolean m_pipelining;
        private int m_connectTimeout;
        private int m_maxPoolSize;
        private int m_reconnectAttempts;
        private long m_reconnectInterval;

        private LoadBalancingDef m_loadBalancingDef;

        private String m_keystorePath;
        private String m_keystorePassword;
        private String m_keystoreType;

        private String m_truststorePath;
        private String m_truststorePassword;
        private String m_truststoreType;

        private Builder(final List<URI> nodes) {

            m_nodes = nodes;
            m_keepAlive = DEF_KEEP_ALIVE;
            m_useCompression = DEF_USE_COMPRESSION;
            m_pipelining = DEF_PIPELINING;
            m_sslEnabled = DEF_SSL_ENABLED;
            m_verifyHost = DEF_VERIFY_HOST;
            m_connectTimeout = DEF_CONNECT_TIMEOUT;
            m_maxPoolSize = DEF_MAX_POOL_SIZE;
            m_reconnectAttempts = DEF_RECONNECT_ATTEMPTS;
            m_reconnectInterval = DEF_RECONNECT_INTERVAL;

            m_loadBalancingDef = new LoadBalancingDef(
                    DEF_LOAD_BALANCING_STRATEGY,
                    DEF_STATUS_URI,
                    DEF_CHECK_INTERVAL);

            m_keystoreType = DEF_KEYSTORE_TYPE;
            m_truststoreType = DEF_TRUSTSTORE_TYPE;
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

        public Builder connectTimeout(final int connectTimeout) {
            m_connectTimeout = connectTimeout;
            return this;
        }

        public Builder maxPoolSize(final int maxPoolSize) {
            m_maxPoolSize = maxPoolSize;
            return this;
        }

        public Builder reconnectAttempts(final int reconnectAttempts) {
            m_reconnectAttempts = reconnectAttempts;
            return this;
        }

        public Builder reconnectInterval(final long reconnectInterval) {
            m_reconnectInterval = reconnectInterval;
            return this;
        }

        public Builder loadBalancing(
                final String strategyName,
                final long checkInterval) {

            return loadBalancing(
                    strategyName,
                    DEF_STATUS_URI,
                    checkInterval);
        }

        public Builder loadBalancing(
                final String strategyName,
                final String statusUri,
                final long checkInterval) {

            m_loadBalancingDef = new LoadBalancingDef(
                    strategyName,
                    statusUri,
                    checkInterval);

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

        public Builder keystoreType(final String keystoreType) {
            m_keystoreType = keystoreType;
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

        public Builder truststoreType(final String truststoreType) {
            m_truststoreType = truststoreType;
            return this;
        }

        @Override
        public NodesDef build() {
            return new NodesDef(this);
        }
    }
}
