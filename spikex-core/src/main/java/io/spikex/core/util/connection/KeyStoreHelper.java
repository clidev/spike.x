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
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslContextBuilder;
import io.netty.handler.ssl.SslProvider;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Path;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.UnrecoverableKeyException;
import java.security.cert.CertificateException;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.TrustManagerFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author cli
 */
public final class KeyStoreHelper {

    private Path m_keyStorePath;
    private String m_keyStorePassword;
    //
    private Path m_trustStorePath;
    private String m_trustStorePassword;
    //
    private Path m_clientCertPath;
    private Path m_clientKeyPath;
    private Path m_trustCertChainPath;
    //
    private final Logger m_logger = LoggerFactory.getLogger(KeyStoreHelper.class);

    public KeyStoreHelper(
            final Path trustStorePath,
            final String trustStorePassword,
            final Path keyStorePath,
            final String keyStorePassword) {
        //
        // Sanity check
        //
        Preconditions.checkNotNull(trustStorePath);

        m_keyStorePath = keyStorePath;
        m_keyStorePassword = keyStorePassword;

        m_trustStorePath = trustStorePath;
        m_trustStorePassword = trustStorePassword;
    }

    public KeyStoreHelper(
            final Path trustCertChainPath,
            final Path clientCertPath,
            final Path clientKeyPath) {
        //
        // Sanity check
        //
        Preconditions.checkNotNull(trustCertChainPath);

        m_clientCertPath = clientCertPath;
        m_clientKeyPath = clientKeyPath;

        m_trustCertChainPath = trustCertChainPath;
    }

    public Path getTrustStorePath() {
        return m_trustStorePath;
    }

    public Path getKeyStorePath() {
        return m_keyStorePath;
    }

    public String geTrustStorePassword() {
        return m_trustStorePassword;
    }

    public String geKeyStorePassword() {
        return m_keyStorePassword;
    }

    public Path getTrustCertChainPath() {
        return m_trustCertChainPath;
    }

    public Path getClientCertPath() {
        return m_clientCertPath;
    }

    public Path getClientKeyPath() {
        return m_clientKeyPath;
    }

    public KeyStore loadKeyStore(
            final Path keyStorePath,
            final String keyStorePassword) throws KeyStoreException {
        //
        // Sanity check
        //
        Preconditions.checkNotNull(keyStorePath);

        KeyStore ks = KeyStore.getInstance(KeyStore.getDefaultType());
        InputStream in = null;
        try {
            in = new FileInputStream(keyStorePath.toAbsolutePath().normalize().toFile());
            ks.load(in, keyStorePassword != null ? keyStorePassword.toCharArray() : null);
        } catch (IOException | NoSuchAlgorithmException | CertificateException e) {
            throw new KeyStoreException("Failed to load keyStore", e);
        } finally {
            if (in != null) {
                try {
                    in.close();
                } catch (IOException e) {
                    m_logger.error("Failed to close keyStore input stream", e);
                }
            }
        }
        return ks;
    }

    public SslContext buildClientContext(final boolean clientAuth) throws KeyStoreException {
        return buildJdkClientContext(clientAuth);
    }

    public SslContext buildJdkClientContext(final boolean clientAuth) throws KeyStoreException {

        SslContext ctx = null;

        try {
            TrustManagerFactory trustMgrFactory = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
            {
                String password = geTrustStorePassword();
                KeyStore trustStore = loadKeyStore(getTrustStorePath(), password);
                trustMgrFactory.init(trustStore);
            }

            if (clientAuth) {

                KeyManagerFactory keyMgrFactory = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
                String password = geKeyStorePassword();
                KeyStore keyStore = loadKeyStore(getKeyStorePath(), password);
                keyMgrFactory.init(keyStore, password != null ? password.toCharArray() : null);

                ctx = SslContextBuilder.forClient()
                        .sslProvider(SslProvider.JDK)
                        .trustManager(trustMgrFactory)
                        .keyManager(keyMgrFactory)
                        .build();
            } else {
                ctx = SslContextBuilder.forClient()
                        .sslProvider(SslProvider.JDK)
                        .trustManager(trustMgrFactory)
                        .build();
            }
        } catch (IOException | NoSuchAlgorithmException | UnrecoverableKeyException e) {
            throw new KeyStoreException("Failed to build SSL context", e);
        }

        return ctx;
    }

    public SslContext buildOpenSslClientContext(final boolean clientAuth) throws IOException {

        SslContext ctx;

        if (clientAuth) {
            ctx = SslContextBuilder.forClient()
                    .sslProvider(SslProvider.OPENSSL)
                    .trustManager(getTrustCertChainPath().toAbsolutePath().normalize().toFile())
                    .keyManager(getClientCertPath().toAbsolutePath().normalize().toFile(),
                            getClientKeyPath().toAbsolutePath().normalize().toFile())
                    .build();
        } else {
            ctx = SslContextBuilder.forClient()
                    .sslProvider(SslProvider.OPENSSL)
                    .trustManager(getTrustCertChainPath().toAbsolutePath().normalize().toFile())
                    .build();
        }

        return ctx;
    }
}
