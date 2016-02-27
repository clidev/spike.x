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
package io.spikex.filter.internal;

import com.eaio.uuid.UUID;
import com.github.brainlag.nsq.NSQConfig;
import com.github.brainlag.nsq.lookup.DefaultNSQLookup;
import com.github.brainlag.nsq.lookup.NSQLookup;
import io.spikex.core.SpikexInfo;
import io.spikex.core.helper.Variables;
import io.spikex.core.util.HostOs;
import io.spikex.core.util.IBuilder;
import io.spikex.core.util.connection.KeyStoreHelper;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.KeyStoreException;
import java.util.ArrayList;
import java.util.List;
import org.vertx.java.core.json.JsonArray;
import org.vertx.java.core.json.JsonObject;

/**
 *
 * @author cli
 */
public final class NsqClientConfig {

    private final List<String> m_topics;
    private final List<String> m_nodes;
    private final List<String> m_lookupNodes;

    private final boolean m_featureNegotiation;
    private final boolean m_sslEnabled;
    private final boolean m_sslClientAuth;
    private final int m_heartbeatInterval;
    private final int m_lookupPeriod;
    private final int m_deflateLevel;
    private final int m_corePoolSize;
    private final int m_maxPoolSize;
    private final int m_msgPerBatch;
    private final int m_msgTimeout;
    private final int m_msgQueueSize;
    private final int m_outputBufferTimeout;
    private final int m_outputBufferSize;
    private final int m_sampleRate;
    private final long m_idleThreadKeepAliveTime;

    private final String m_clientId;
    private final String m_userAgent;

    private final String m_trustStorePath;
    private final String m_trustStorePassword;
    private final String m_keyStorePath;
    private final String m_keyStorePassword;

    private final String m_trustCertChainPath;
    private final String m_clientCertPath;
    private final String m_clientKeyPath;
    private final String m_sslProvider;

    private final NSQConfig.Compression m_compression;

    public static final String NSQ_COMPRESSION_NONE = "no-compression";
    public static final String NSQ_COMPRESSION_DEFLATE = "deflate";
    public static final String NSQ_COMPRESSION_SNAPPY = "snappy";

    public static final int NSQ_HTTP_PORT = 4151;
    public static final int NSQ_LOOKUP_PORT = 4161;

    public static final String SSL_PROVIDER_JDK = "jdk";
    public static final String SSL_PROVIDER_OPENSSL = "openssl";

    private static final String CONF_KEY_CLIENT_ID = "client-id";
    private static final String CONF_KEY_COMPRESSION = "compression";
    private static final String CONF_KEY_DEFLATE_LEVEL = "deflate-level";
    private static final String CONF_KEY_SSL_ENABLED = "ssl-enabled";
    private static final String CONF_KEY_SSL_PROVIDER = "ssl-provider";
    private static final String CONF_KEY_SSL_CLIENT_AUTH = "ssl-client-auth";
    private static final String CONF_KEY_FEATURE_NEGOTIATION = "feature-negotiation";
    private static final String CONF_KEY_HEARTBEAT_INTERVAL = "heartbeat-interval";
    private static final String CONF_KEY_MSG_TIMEOUT = "msg-timeout";
    private static final String CONF_KEY_OUTPUT_BUFFER_SIZE = "output-buffer-size";
    private static final String CONF_KEY_OUTPUT_BUFFER_TIMEOUT = "output-buffer-timeout";
    private static final String CONF_KEY_SAMPLE_RATE = "sample-rate";
    private static final String CONF_KEY_USER_AGENT = "user-agent";
    private static final String CONF_KEY_NODES = "nodes";
    private static final String CONF_KEY_LOOKUP_NODES = "lookup-nodes";
    private static final String CONF_KEY_TOPICS = "topics";
    private static final String CONF_KEY_MESSAGES_PER_BATCH = "messages-per-batch";
    private static final String CONF_KEY_LOOKUP_PERIOD = "lookup-period";
    private static final String CONF_KEY_CORE_POOL_SIZE = "core-pool-size";
    private static final String CONF_KEY_MAX_POOL_SIZE = "max-pool-size";
    private static final String CONF_KEY_IDLE_THREAD_KEEP_ALIVE_TIME = "idle-thread-keep-alive-time";
    private static final String CONF_KEY_MESSAGE_QUEUE_SIZE = "message-queue-size";
    private static final String CONF_KEY_KEYSTORE_PATH = "keystore-path";
    private static final String CONF_KEY_KEYSTORE_PASSWORD = "keystore-password";
    private static final String CONF_KEY_KEYSTORE_TYPE = "keystore-type";
    private static final String CONF_KEY_TRUSTSTORE_PATH = "truststore-path";
    private static final String CONF_KEY_TRUSTSTORE_PASSWORD = "truststore-password";
    private static final String CONF_KEY_TRUSTSTORE_TYPE = "truststore-type";
    private static final String CONF_KEY_TRUST_CERT_CHAIN_PATH = "trust-cert-chain-path";
    private static final String CONF_KEY_CLIENT_CERT_PATH = "client-cert-path";
    private static final String CONF_KEY_CLIENT_KEY_PATH = "client-key-path";

    //
    // Configuration defaults
    //
    private static final String[] DEF_NODES = new String[]{"localhost:4151"};
    private static final String[] DEF_LOOKUP_NODES = new String[]{"localhost:4161"};
    private static final String DEF_USER_AGENT = "spikex/" + SpikexInfo.version();
    private static final String DEF_KEYSTORE_PATH = "%{#spikex.conf}/keystore.jks";
    private static final String DEF_KEYSTORE_PASSWORD = "1234secret!";
    private static final String DEF_TRUSTSTORE_PATH = "%{#spikex.conf}/truststore.jks";
    private static final String DEF_TRUSTSTORE_PASSWORD = "1234secret!";
    private static final String DEF_KEYSTORE_TYPE = "JKS";
    private static final String DEF_TRUSTSTORE_TYPE = "JKS";
    private static final String DEF_TRUST_CERT_CHAIN_PATH = "%{#spikex.conf}/trusted.pem";
    private static final String DEF_CLIENT_CERT_PATH = "%{#spikex.conf}/client.crt";
    private static final String DEF_CLIENT_KEY_PATH = "%{#spikex.conf}/client.key";
    private static final int DEF_DEFLATE_LEVEL = 4;
    private static final int DEF_HEARTBEAT_INTERVAL = 30000; // ms
    private static final int DEF_MSG_TIMEOUT = 60000; // ms
    private static final int DEF_OUTPUT_BUFFER_SIZE = 16000; // bytes
    private static final int DEF_OUTPUT_BUFFER_TIMEOUT = 250; // ms
    private static final int DEF_SAMPLE_RATE = 0;
    private static final int DEF_MESSAGES_PER_BATCH = 200;
    private static final int DEF_LOOKUP_PERIOD = 60 * 1000;
    private static final int DEF_CORE_POOL_SIZE = 1;
    private static final int DEF_MAX_POOL_SIZE = 1;
    private static final int DEF_MESSAGE_QUEUE_SIZE = 1000;
    private static final long DEF_IDLE_THREAD_KEEP_ALIVE_TIME = 1000L; // ms

    private static final boolean DEF_FEATURE_NEGOTIATION = true;
    private static final boolean DEF_SSL_ENABLED = false;
    private static final boolean DEF_SSL_CLIENT_AUTH = false;
    private static final String DEF_SSL_PROVIDER = SSL_PROVIDER_JDK;

    private static final NSQConfig.Compression DEF_COMPRESSION
            = NSQConfig.Compression.NO_COMPRESSION;

    private static final String NSQ_CLIENT_PREFIX = "NSQ-CLIENT-";

    private NsqClientConfig(
            final List<String> topics,
            final List<String> nodes,
            final List<String> lookupNodes,
            final boolean featureNegotiation,
            final boolean sslEnabled,
            final boolean sslClientAuth,
            final int heartbeatInterval,
            final int lookupPeriod,
            final int deflateLevel,
            final int corePoolSize,
            final int maxPoolSize,
            final int msgPerBatch,
            final int msgTimeout,
            final int msgQueueSize,
            final int outputBufferTimeout,
            final int outputBufferSize,
            final int sampleRate,
            final long idleThreadKeepAliveTime,
            final String clientId,
            final String userAgent,
            final String trustStorePath,
            final String trustStorePassword,
            final String keyStorePath,
            final String keyStorePassword,
            final String trustCertChainPath,
            final String clientCertPath,
            final String clientKeyPath,
            final String sslProvider,
            final NSQConfig.Compression compression) {

        m_topics = topics;
        m_nodes = nodes;
        m_lookupNodes = lookupNodes;
        m_featureNegotiation = featureNegotiation;
        m_sslEnabled = sslEnabled;
        m_sslClientAuth = sslClientAuth;
        m_heartbeatInterval = heartbeatInterval;
        m_lookupPeriod = lookupPeriod;
        m_deflateLevel = deflateLevel;
        m_corePoolSize = corePoolSize;
        m_maxPoolSize = maxPoolSize;
        m_msgPerBatch = msgPerBatch;
        m_msgTimeout = msgTimeout;
        m_msgQueueSize = msgQueueSize;
        m_outputBufferTimeout = outputBufferTimeout;
        m_outputBufferSize = outputBufferSize;
        m_sampleRate = sampleRate;
        m_idleThreadKeepAliveTime = idleThreadKeepAliveTime;
        m_clientId = clientId;
        m_userAgent = userAgent;
        m_trustStorePath = trustStorePath;
        m_trustStorePassword = trustStorePassword;
        m_keyStorePath = keyStorePath;
        m_keyStorePassword = keyStorePassword;
        m_compression = compression;
        m_trustCertChainPath = trustCertChainPath;
        m_clientCertPath = clientCertPath;
        m_clientKeyPath = clientKeyPath;
        m_sslProvider = sslProvider;
    }

    public boolean isSslEnabled() {
        return m_sslEnabled;
    }

    public boolean isSslClientAuth() {
        return m_sslClientAuth;
    }

    public String getSslProvider() {
        return m_sslProvider;
    }

    public List<String> getTopics() {
        return m_topics;
    }

    public List<String> getNodes() {
        return m_nodes;
    }

    public int getLookupPeriod() {
        return m_lookupPeriod;
    }

    public int getCorePoolSize() {
        return m_corePoolSize;
    }

    public int getMaxPoolSize() {
        return m_maxPoolSize;
    }

    public int getMessagesPerBatch() {
        return m_msgPerBatch;
    }

    public int getMessagesQueueSize() {
        return m_msgQueueSize;
    }

    public long getIdleThreadKeepAliveTime() {
        return m_idleThreadKeepAliveTime;
    }

    public NSQConfig buildNSQConfig(final Variables variables) {

        NSQConfig config = new NSQConfig();
        config.setClientId(m_clientId);
        config.setCompression(m_compression);
        config.setDeflateLevel(m_deflateLevel);
        config.setFeatureNegotiation(m_featureNegotiation);
        config.setHeartbeatInterval(m_heartbeatInterval);
        config.setMsgTimeout(m_msgTimeout);
        config.setOutputBufferSize(m_outputBufferSize);
        config.setOutputBufferTimeout(m_outputBufferTimeout);
        config.setSampleRate(m_sampleRate);
        config.setUserAgent(m_userAgent);

        // SSL
        if (isSslEnabled()) {

            String sslProvider = getSslProvider();
            if (sslProvider != null) {
                switch (sslProvider) {
                    // Provider: JDK
                    case SSL_PROVIDER_JDK:
                        buildSslContextJdk(variables, config);
                        break;
                    // Provider: OPENSSL
                    case SSL_PROVIDER_OPENSSL:
                        buildSslContextOpenSsl(variables, config);
                        break;
                    // Provider: JDK
                    default:
                        buildSslContextJdk(variables, config);
                        break;
                }
            }
        }

        return config;
    }

    public NSQLookup buildNSQLookup() {
        NSQLookup lookup = new DefaultNSQLookup();
        for (String host : m_lookupNodes) {

            int port = NSQ_LOOKUP_PORT;
            int pos = host.lastIndexOf(":");

            if (pos != -1) {
                port = Integer.parseInt(host.substring(pos + 1));
                host = host.substring(0, pos);
            }

            lookup.addLookupAddress(host, port);
        }
        return lookup;
    }

    private void buildSslContextJdk(
            final Variables variables,
            final NSQConfig config) {

        try {
            Path trustStorePath = Paths.get(String.valueOf(variables.translate(m_trustStorePath)));
            Path keyStorePath = Paths.get(String.valueOf(variables.translate(m_keyStorePath)));

            KeyStoreHelper helper = new KeyStoreHelper(
                    trustStorePath,
                    m_trustStorePassword,
                    keyStorePath,
                    m_keyStorePassword);

            config.setSslContext(helper.buildClientContext(isSslClientAuth()));

        } catch (KeyStoreException e) {
            throw new IllegalStateException("Failed to create SSL context", e);
        }
    }

    private void buildSslContextOpenSsl(
            final Variables variables,
            final NSQConfig config) {

        try {
            Path trustCertChainPath = Paths.get(String.valueOf(variables.translate(m_trustCertChainPath)));
            Path clientCertPath = Paths.get(String.valueOf(variables.translate(m_clientCertPath)));
            Path clientKeyPath = Paths.get(String.valueOf(variables.translate(m_clientKeyPath)));

            KeyStoreHelper helper = new KeyStoreHelper(
                    trustCertChainPath,
                    clientCertPath,
                    clientKeyPath);

            config.setSslContext(helper.buildOpenSslClientContext(isSslClientAuth()));

        } catch (IOException e) {
            throw new IllegalStateException("Failed to create SSL context", e);
        }
    }

    public static Builder builder(final List<String> topics) {
        return new Builder(topics);
    }

    public static Builder builder(final JsonObject config) {
        return new Builder(config);
    }

    public static final class Builder implements IBuilder<NsqClientConfig> {

        private final List<String> m_topics;
        private final List<String> m_nodes;
        private final List<String> m_lookupNodes;

        private boolean m_featureNegotiation;
        private boolean m_sslEnabled;
        private boolean m_sslClientAuth;
        private int m_heartbeatInterval;
        private int m_lookupPeriod;
        private int m_deflateLevel;
        private int m_corePoolSize;
        private int m_maxPoolSize;
        private int m_msgPerBatch;
        private int m_msgTimeout;
        private int m_msgQueueSize;
        private int m_outputBufferTimeout;
        private int m_outputBufferSize;
        private int m_sampleRate;
        private long m_idleThreadKeepAliveTime;

        private String m_clientId;
        private String m_userAgent;

        private String m_trustStorePath;
        private String m_trustStorePassword;
        private String m_keyStorePath;
        private String m_keyStorePassword;

        private String m_trustCertChainPath;
        private String m_clientCertPath;
        private String m_clientKeyPath;
        private String m_sslProvider;

        private NSQConfig.Compression m_compression;

        private Builder(final List<String> topics) {

            m_topics = new ArrayList(topics);
            m_nodes = new ArrayList();
            m_lookupNodes = new ArrayList();

            m_featureNegotiation = DEF_FEATURE_NEGOTIATION;
            m_sslEnabled = DEF_SSL_ENABLED;
            m_sslClientAuth = DEF_SSL_CLIENT_AUTH;
            m_heartbeatInterval = DEF_HEARTBEAT_INTERVAL;
            m_lookupPeriod = DEF_LOOKUP_PERIOD;
            m_deflateLevel = DEF_DEFLATE_LEVEL;
            m_corePoolSize = DEF_CORE_POOL_SIZE;
            m_maxPoolSize = DEF_MAX_POOL_SIZE;
            m_msgPerBatch = DEF_MESSAGES_PER_BATCH;
            m_msgTimeout = DEF_MSG_TIMEOUT;
            m_msgQueueSize = DEF_MESSAGE_QUEUE_SIZE;
            m_outputBufferTimeout = DEF_OUTPUT_BUFFER_TIMEOUT;
            m_outputBufferSize = DEF_OUTPUT_BUFFER_SIZE;
            m_sampleRate = DEF_SAMPLE_RATE;
            m_idleThreadKeepAliveTime = DEF_IDLE_THREAD_KEEP_ALIVE_TIME;
            m_userAgent = DEF_USER_AGENT;
            m_clientId = newClientId();

            m_trustStorePath = DEF_TRUSTSTORE_PATH;
            m_trustStorePassword = DEF_TRUSTSTORE_PASSWORD;
            m_keyStorePath = DEF_KEYSTORE_PATH;
            m_keyStorePassword = DEF_KEYSTORE_PASSWORD;
            m_compression = DEF_COMPRESSION;

            m_trustCertChainPath = DEF_TRUST_CERT_CHAIN_PATH;
            m_clientCertPath = DEF_CLIENT_CERT_PATH;
            m_clientKeyPath = DEF_CLIENT_KEY_PATH;
            m_sslProvider = DEF_SSL_PROVIDER;
        }

        private Builder(final JsonObject config) {

            m_topics = config.getArray(CONF_KEY_TOPICS, new JsonArray()).toList();
            m_nodes = config.getArray(CONF_KEY_NODES,
                    new JsonArray(DEF_NODES)).toList();
            m_lookupNodes = config.getArray(CONF_KEY_LOOKUP_NODES,
                    new JsonArray(DEF_LOOKUP_NODES)).toList();

            clientId(config.getString(CONF_KEY_CLIENT_ID, newClientId()));
            compression(config.getString(CONF_KEY_COMPRESSION, NSQ_COMPRESSION_NONE));
            deflateLevel(config.getInteger(CONF_KEY_DEFLATE_LEVEL, DEF_DEFLATE_LEVEL));
            featureNegotiation(config.getBoolean(CONF_KEY_FEATURE_NEGOTIATION,
                    DEF_FEATURE_NEGOTIATION));
            sslEnabled(config.getBoolean(CONF_KEY_SSL_ENABLED,
                    DEF_SSL_ENABLED));
            sslClientAuth(config.getBoolean(CONF_KEY_SSL_CLIENT_AUTH,
                    DEF_SSL_CLIENT_AUTH));
            heartbeatInterval(config.getInteger(CONF_KEY_HEARTBEAT_INTERVAL,
                    DEF_HEARTBEAT_INTERVAL));
            messageTimeout(config.getInteger(CONF_KEY_MSG_TIMEOUT, DEF_MSG_TIMEOUT));
            outputBufferSize(config.getInteger(CONF_KEY_OUTPUT_BUFFER_SIZE,
                    DEF_OUTPUT_BUFFER_SIZE));
            outputBufferTimeout(config.getInteger(CONF_KEY_OUTPUT_BUFFER_TIMEOUT,
                    DEF_OUTPUT_BUFFER_TIMEOUT));
            sampleRate(config.getInteger(CONF_KEY_SAMPLE_RATE, DEF_SAMPLE_RATE));
            userAgent(config.getString(CONF_KEY_USER_AGENT, DEF_USER_AGENT));
            trustStorePath(config.getString(CONF_KEY_TRUSTSTORE_PATH, DEF_TRUSTSTORE_PATH));
            trustStorePassword(config.getString(CONF_KEY_TRUSTSTORE_PASSWORD, DEF_TRUSTSTORE_PASSWORD));
            keyStorePath(config.getString(CONF_KEY_KEYSTORE_PATH, DEF_KEYSTORE_PATH));
            keyStorePassword(config.getString(CONF_KEY_KEYSTORE_PASSWORD, DEF_KEYSTORE_PASSWORD));

            trustCertChainPath(config.getString(CONF_KEY_TRUST_CERT_CHAIN_PATH, DEF_TRUST_CERT_CHAIN_PATH));
            clientCertPath(config.getString(CONF_KEY_CLIENT_CERT_PATH, DEF_CLIENT_CERT_PATH));
            clientKeyPath(config.getString(CONF_KEY_CLIENT_KEY_PATH, DEF_CLIENT_KEY_PATH));
            sslProvider(config.getString(CONF_KEY_SSL_PROVIDER, DEF_SSL_PROVIDER));

            messagesPerBatch(config.getInteger(CONF_KEY_MESSAGES_PER_BATCH,
                    DEF_MESSAGES_PER_BATCH));
            lookupPeriod(config.getInteger(CONF_KEY_LOOKUP_PERIOD,
                    DEF_LOOKUP_PERIOD));

            messageQueueSize(config.getInteger(CONF_KEY_MESSAGE_QUEUE_SIZE, DEF_MESSAGE_QUEUE_SIZE));
            corePoolSize(config.getInteger(CONF_KEY_CORE_POOL_SIZE, DEF_CORE_POOL_SIZE));
            maxPoolSize(config.getInteger(CONF_KEY_MAX_POOL_SIZE, DEF_MAX_POOL_SIZE));
            idleThreadKeepAliveTime(config.getLong(CONF_KEY_IDLE_THREAD_KEEP_ALIVE_TIME, DEF_IDLE_THREAD_KEEP_ALIVE_TIME));
        }

        public void clientId(final String clientId) {
            m_clientId = clientId;
        }

        public void compression(final String compression) {
            switch (compression) {
                case NSQ_COMPRESSION_NONE:
                    m_compression = NSQConfig.Compression.NO_COMPRESSION;
                    break;
                case NSQ_COMPRESSION_DEFLATE:
                    m_compression = NSQConfig.Compression.DEFLATE;
                    break;
                case NSQ_COMPRESSION_SNAPPY:
                    m_compression = NSQConfig.Compression.SNAPPY;
                    break;
                default:
                    m_compression = NSQConfig.Compression.NO_COMPRESSION;
                    break;
            }
        }

        public void deflateLevel(final int level) {
            m_deflateLevel = level;
        }

        public void featureNegotiation(final boolean negotiation) {
            m_featureNegotiation = negotiation;
        }

        public void sslEnabled(final boolean enabled) {
            m_sslEnabled = enabled;
        }

        public void sslClientAuth(final boolean enabled) {
            m_sslClientAuth = enabled;
        }

        public void heartbeatInterval(final int interval) {
            m_heartbeatInterval = interval;
        }

        public void messageTimeout(final int timeout) {
            m_msgTimeout = timeout;
        }

        public void outputBufferSize(final int size) {
            m_outputBufferSize = size;
        }

        public void outputBufferTimeout(final int timeout) {
            m_outputBufferTimeout = timeout;
        }

        public void sampleRate(final int rate) {
            m_sampleRate = rate;
        }

        public void userAgent(final String agent) {
            m_userAgent = agent;
        }

        public void trustStorePath(final String path) {
            m_trustStorePath = path;
        }

        public void trustStorePassword(final String password) {
            m_trustStorePassword = password;
        }

        public void keyStorePath(final String path) {
            m_keyStorePath = path;
        }

        public void keyStorePassword(final String password) {
            m_keyStorePassword = password;
        }

        public void trustCertChainPath(final String path) {
            m_trustCertChainPath = path;
        }

        public void clientCertPath(final String path) {
            m_clientCertPath = path;
        }

        public void clientKeyPath(final String path) {
            m_clientKeyPath = path;
        }

        public void sslProvider(final String provider) {
            m_sslProvider = provider;
        }

        public void messagesPerBatch(final int count) {
            m_msgPerBatch = count;
        }

        public void lookupPeriod(final int period) {
            m_lookupPeriod = period;
        }

        public void messageQueueSize(final int size) {
            m_msgQueueSize = size;
        }

        public void corePoolSize(final int size) {
            m_corePoolSize = size;
        }

        public void maxPoolSize(final int size) {
            m_maxPoolSize = size;
        }

        public void idleThreadKeepAliveTime(final long time) {
            m_idleThreadKeepAliveTime = time;
        }

        public void nodes(List<String> nodes) {
            m_nodes.clear();
            m_nodes.addAll(nodes);
        }

        public void lookupNodes(List<String> nodes) {
            m_lookupNodes.clear();
            m_lookupNodes.addAll(nodes);
        }

        @Override
        public NsqClientConfig build() {
            return new NsqClientConfig(
                    m_topics,
                    m_nodes,
                    m_lookupNodes,
                    m_featureNegotiation,
                    m_sslEnabled,
                    m_sslClientAuth,
                    m_heartbeatInterval,
                    m_lookupPeriod,
                    m_deflateLevel,
                    m_corePoolSize,
                    m_maxPoolSize,
                    m_msgPerBatch,
                    m_msgTimeout,
                    m_msgQueueSize,
                    m_outputBufferTimeout,
                    m_outputBufferSize,
                    m_sampleRate,
                    m_idleThreadKeepAliveTime,
                    m_clientId,
                    m_userAgent,
                    m_trustStorePath,
                    m_trustStorePassword,
                    m_keyStorePath,
                    m_keyStorePassword,
                    m_trustCertChainPath,
                    m_clientCertPath,
                    m_clientKeyPath,
                    m_sslProvider,
                    m_compression);
        }

        private String newClientId() {
            return NSQ_CLIENT_PREFIX + HostOs.hostName() + "-" + new UUID().toString();
        }
    }
}
