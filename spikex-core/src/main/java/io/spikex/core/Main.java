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
package io.spikex.core;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.eventbus.Subscribe;
import com.hazelcast.config.Config;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import static com.sun.akuma.CLibrary.LIBC;
import com.sun.akuma.Daemon;
import com.sun.akuma.JavaVMArguments;
import static io.spikex.core.AbstractVerticle.CONF_KEY_CLUSTER_NAME;
import static io.spikex.core.AbstractVerticle.CONF_KEY_CLUSTER_PASSWORD;
import static io.spikex.core.AbstractVerticle.CONF_KEY_CONF_PATH;
import static io.spikex.core.AbstractVerticle.CONF_KEY_DATA_PATH;
import static io.spikex.core.AbstractVerticle.CONF_KEY_HOME_PATH;
import static io.spikex.core.AbstractVerticle.CONF_KEY_LOCAL_ADDRESS;
import static io.spikex.core.AbstractVerticle.CONF_KEY_NODE_NAME;
import static io.spikex.core.AbstractVerticle.CONF_KEY_TMP_PATH;
import static io.spikex.core.AbstractVerticle.CONF_KEY_USER;
import io.spikex.core.helper.Commands;
import static io.spikex.core.util.Files.Permission.OWNER_FULL_GROUP_EXEC_OTHER_EXEC;
import io.spikex.core.util.HostOs;
import io.spikex.core.util.NioDirWatcher;
import io.spikex.core.util.Version;
import io.spikex.core.util.resource.YamlDocument;
import io.spikex.core.util.resource.YamlResource;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.math.BigInteger;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardWatchEventKinds;
import java.nio.file.WatchEvent;
import java.security.InvalidKeyException;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.NoSuchProviderException;
import java.security.Provider;
import java.security.SecureRandom;
import java.security.Security;
import java.security.SignatureException;
import java.security.cert.Certificate;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.bouncycastle.asn1.x500.X500NameBuilder;
import org.bouncycastle.asn1.x500.style.BCStyle;
import org.bouncycastle.asn1.x509.BasicConstraints;
import org.bouncycastle.asn1.x509.ExtendedKeyUsage;
import org.bouncycastle.asn1.x509.GeneralName;
import org.bouncycastle.asn1.x509.GeneralNames;
import org.bouncycastle.asn1.x509.KeyPurposeId;
import org.bouncycastle.asn1.x509.KeyUsage;
import org.bouncycastle.asn1.x509.X509Extensions;
import org.bouncycastle.cert.X509CertificateHolder;
import org.bouncycastle.cert.X509v3CertificateBuilder;
import org.bouncycastle.cert.jcajce.JcaX509CertificateConverter;
import org.bouncycastle.cert.jcajce.JcaX509v3CertificateBuilder;
import org.bouncycastle.jce.provider.BouncyCastleProvider;
import org.bouncycastle.openssl.PEMParser;
import org.bouncycastle.operator.ContentSigner;
import org.bouncycastle.operator.OperatorCreationException;
import org.bouncycastle.operator.jcajce.JcaContentSignerBuilder;
import org.kohsuke.args4j.Argument;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.vertx.java.core.AsyncResult;
import org.vertx.java.core.Handler;
import org.vertx.java.core.json.JsonObject;
import org.vertx.java.platform.PlatformLocator;
import org.vertx.java.platform.PlatformManager;

/**
 *
 * @author cli
 */
public final class Main {

    @Option(name = "-conf", usage = "Configuration files")
    private String m_optConfDir;

    @Option(name = "-data", usage = "Persistent data files")
    private String m_optDataDir;

    @Option(name = "-tmp", usage = "Temporary files")
    private String m_optTmpDir;

    @Option(name = "-umask", usage = "Set process umask")
    private Integer m_optUmask;

    @Option(name = "-daemon", usage = "Daemonize process")
    private boolean m_optDaemon;

    @Option(name = "-version", usage = "Display version information")
    private boolean m_optVersion;

    @Argument
    private final List<String> m_args = new ArrayList();

    // Home directory
    private Path m_homePath;

    // Configuration file name, base and hash
    private String m_confName;
    private Path m_confPath;
    private int m_confHash;

    // Last time we did a re-init
    private long m_lastInitTm;

    // Vert.x platform manager
    private static PlatformManager m_manager;

    // Deployed modules
    private static final Map<String, Module> m_modules = new ConcurrentHashMap();

    private static final String SYS_PROPERTY_HOME_VERTX = "vertx.home";
    private static final String SYS_PROPERTY_HOME_SPIKEX = "spikex.home";
    private static final String SYS_PROPERTY_USER = "spikex.user";
    private static final String SYS_PROPERTY_PIDFILE = "spikex.pidfile";
    private static final String SYS_PROPERTY_MAX_DEPLOY_SECS = "spikex.module.deploy.secs";
    private static final String SYS_PROPERTY_MAX_UNDEPLOY_SECS = "spikex.module.undeploy.secs";

    private static final String DEF_CONF_DIR = "conf";
    private static final String DEF_DATA_DIR = "data";
    private static final String DEF_TMP_DIR = "tmp";
    private static final String DEF_USER = "spikex";
    private static final String DEF_CONF_NAME = "spikex";
    private static final String DEF_PID_FILE = "/var/run/spikex.pid";
    private static final String DEF_KEYSTORE_PATH = "keystore.jks";
    private static final String DEF_KEYSTORE_PASSWORD = "1234secret!";
    private static final String DEF_TRUSTSTORE_PATH = "truststore.jks";
    private static final String DEF_TRUSTSTORE_PASSWORD = "1234secret!";
    private static final String DEF_TRUSTSTORE_CERTS_DIR = "certs";
    private static final String DEF_MAX_DEPLOY_SECS = "45";
    private static final int DEF_UMASK = 022;
    private static final boolean DEF_DAEMON = false;
    private static final boolean DEF_CLUSTERED = false;
    private static final boolean DEF_GENERATE_KEYSTORE = true;
    private static final boolean DEF_GENERATE_TRUSSTORE = true;

    private static final String CONF_KEY_CLUSTER = "cluster";
    private static final String CONF_KEY_ENABLED = "enabled";
    private static final String CONF_KEY_NAME = "name";
    private static final String CONF_KEY_PASSWORD = "password";
    private static final String CONF_KEY_KEYSTORE = "keystore";
    private static final String CONF_KEY_TRUSTSTORE = "truststore";
    private static final String CONF_KEY_PATH = "path";
    private static final String CONF_KEY_GENERATE = "generate";
    private static final String CONF_KEY_HAZELCAST = "hazelcast";
    private static final String CONF_KEY_MODULES = "modules";
    private static final String CONF_KEY_MODULE_ID = "id";
    private static final String CONF_KEY_MODULE_CONFIG = "config";
    private static final String CONF_KEY_HOST_ADDRESS = "host-address";
    private static final String CONF_KEY_HOST_PORT = "host-port";
    private static final String CONF_KEY_HOST_PORT_AUTO_INCR = "hots-port-auto-increment";
    private static final String CONF_KEY_HOST_PORT_COUNT = "hots-port-count";
    private static final String CONF_KEY_HOST_FQDN = "host-fqdn";
    private static final String CONF_KEY_SUBJECT_ALT_NAME = "subject-alt-name";
    private static final String CONF_KEY_INSTANCES = "instances";

    private static final Pattern REGEXP_MODGROUP = Pattern.compile("([^~]+)");
    private static final String SPIKEX_DEPLOY_DIR = "deploy";

    private static final Logger m_logger = LoggerFactory.getLogger(Main.class);

    public Main() {

        String homeDir = System.getProperty(SYS_PROPERTY_HOME_SPIKEX); // spikex.home

        // Sanity check
        if (Strings.isNullOrEmpty(homeDir)) {
            throw new IllegalStateException(SYS_PROPERTY_HOME_SPIKEX
                    + " must be defined. Please specify it using: -D"
                    + SYS_PROPERTY_HOME_SPIKEX
                    + "=<node home directory>");
        }

        // Set vertx.home
        m_homePath = Paths.get(homeDir).toAbsolutePath().normalize();
        System.setProperty(SYS_PROPERTY_HOME_VERTX, m_homePath.toString());

        // Defaults
        m_optConfDir = m_homePath.resolve(DEF_CONF_DIR).toString();
        m_optDataDir = m_homePath.resolve(DEF_DATA_DIR).toString();
        m_optTmpDir = m_homePath.resolve(DEF_TMP_DIR).toString();
        m_optUmask = DEF_UMASK;
        m_optDaemon = DEF_DAEMON;
        m_confName = DEF_CONF_NAME;
        m_lastInitTm = System.currentTimeMillis();
    }

    public static void main(final String[] args) {

        m_logger.info("Starting Spike.x {} ({})",
                SpikexInfo.version(),
                SpikexInfo.buildTimestamp());

        Daemon d = new Daemon();
        Main m = new Main();

        if (Main.isAkumaSupportedOs() && d.isDaemonized()) {
            try {
                m_logger.debug("Initializing daemon");
                d.init(System.getProperty(SYS_PROPERTY_PIDFILE, DEF_PID_FILE));
                m.start(args);
            } catch (Exception e) {
                m_logger.error("Failed to initialize daemon", e);
            }
        } else {
            m_logger.debug("Parsing command line arguments");
            m.parseCommandLine(args);
            if (Main.isAkumaSupportedOs() && m.m_optDaemon) {
                try {
                    m_logger.debug("Daemonizing process");
                    JavaVMArguments jvmArgs = JavaVMArguments.current();
                    jvmArgs.remove("-daemon"); // Don't try to daemonize again
                    d.daemonize(jvmArgs);
                    System.exit(0);
                } catch (IOException e) {
                    m_logger.error("Failed to daemonize process", e);
                }
            } else {
                // Startup in non-daemon mode
                m.start(args);
                // Register shutdown hook
                Runtime.getRuntime().addShutdownHook(new Thread() {
                    @Override
                    public void run() {
                        if (m_modules != null
                                && !m_modules.isEmpty()) {
                            for (Map.Entry<String, Module> entry : m_modules.entrySet()) {
                                undeployModule(entry.getValue());
                            }
                        }
                    }
                });
            }
        }
    }

    public void parseCommandLine(final String args[]) {
        //
        // Parse command line options and arguments
        //
        CmdLineParser parser = new CmdLineParser(this);
        try {
            parser.parseArgument(args);
            if (!m_args.isEmpty()) {
                m_confName = m_args.get(0);
            }
        } catch (CmdLineException e) {
            System.err.println(e.getMessage());
            parser.printUsage(System.err);
            System.exit(-1);
        }
    }

    public void start(final String args[]) {

        parseCommandLine(args);

        //
        // Just display information and exit
        //
        if (m_optVersion) {
            SpikexInfo.main(args);
            System.exit(0);
        }

        // Sanity checks
        if (Files.notExists(m_homePath)) {
            throw new IllegalStateException("Home directory does not exist: "
                    + m_homePath.toString());
        }
        if (Files.notExists(Paths.get(m_optConfDir))) {
            throw new IllegalStateException("Configuration directory does not exist: "
                    + m_optConfDir);
        }

        String workDir = m_homePath.toAbsolutePath().toString();
        m_logger.info("Home directory: {}", workDir);

        if (Main.isAkumaSupportedOs()) {
            //
            // Output PID and PPID
            //
            m_logger.info("PID: {} PPID: {}", LIBC.getpid(), LIBC.getppid());

            //
            // Set umask and chdir
            //
            m_logger.info("Process umask: {}", Integer.toOctalString(m_optUmask));
            LIBC.umask(m_optUmask);
            LIBC.chdir(workDir);
        }
        System.setProperty("user.dir", workDir);

        try {
            //
            // Watch configuration
            //
            m_confPath = Paths.get(m_optConfDir);
            NioDirWatcher watcher = new NioDirWatcher();
            watcher.register(this);
            watcher.watch(m_confPath,
                    new WatchEvent.Kind[]{
                        StandardWatchEventKinds.ENTRY_CREATE,
                        StandardWatchEventKinds.ENTRY_MODIFY
                    });
            //
            // Initialize platform
            //
            init();

        } catch (IOException e) {
            throw new RuntimeException("Failed to initialize configuration "
                    + "directory watcher", e);
        }
    }

    @Subscribe
    public void handleConfChanged(final WatchEvent event) {

        try {

            // Throttle re-init (5 sec)
            long nowTm = System.currentTimeMillis();
            long lastTm = m_lastInitTm;
            if ((nowTm - lastTm) > 5000L) {
                m_lastInitTm = nowTm;
                Path filePath = (Path) event.context();
                m_logger.trace("Got file changed event: {}", filePath);
                if (filePath != null
                        && filePath.getFileName().toString().startsWith(m_confName)) {
                    String filename = filePath.getFileName().toString();
                    m_logger.info("Configuration file modified: {}", filename);
                    init();
                }
            }
        } catch (Exception e) {
            m_logger.error("Failed to handle configuration change", e);
        }
    }

    private static boolean isAkumaSupportedOs() {
        return (HostOs.isLinux() || HostOs.isMac() || HostOs.isSolaris());
    }

    private void init() {

        try {
            //
            // Initialize if first call or if configuration has changed
            //
            YamlResource confResource = YamlResource.builder(m_confPath.toUri())
                    .name(m_confName)
                    .version(Version.none())
                    .build();

            // Calculate hash
            Path confFile = m_confPath.resolve(confResource.getQualifiedName());
            int hash = io.spikex.core.util.Files.hashOfFile(confFile);

            // Load configuration and create Vert.x platform (if not created already)
            if (m_confHash != hash) {
                m_confHash = hash;
                confResource = confResource.load();

                if (!confResource.isEmpty()) {
                    YamlDocument conf = confResource.getData().get(0);
                    createPlatform(conf);
                    createDirectories();
                    createKeyStore(conf);
                    createTrustStore(conf);
                    deployModules(conf);
                    initModules();
                } else {
                    m_logger.error("Ignoring empty configuration file: {}", confFile);
                }
            }

        } catch (IOException e) {
            m_logger.error("Failed to initialize platform", e);
        }
    }

    private void createPlatform(final YamlDocument conf) {

        //
        // Create platform manager only once (configuration change doesn't matter)
        //
        if (m_manager == null) {
            YamlDocument confCluster = conf.getDocument(CONF_KEY_CLUSTER);

            if (confCluster.getValue(CONF_KEY_ENABLED, DEF_CLUSTERED)) {
                //
                // Create clustered platform
                //
                m_logger.info("Starting in clustered mode");
                YamlDocument confHazelcast = conf.getDocument(CONF_KEY_HAZELCAST);
                int hostPort = confHazelcast.getValue(CONF_KEY_HOST_PORT);
                List<String> hostAddrs = confHazelcast.getList(CONF_KEY_HOST_ADDRESS);
                m_manager = PlatformLocator.factory.createPlatformManager(hostPort, hostAddrs.get(0));
                //
                // Retrieve hazelcast used by Vert.x (first instance)
                //
                Set<HazelcastInstance> hzInstances = Hazelcast.getAllHazelcastInstances();
                HazelcastInstance hzInstance = hzInstances.iterator().next();
                Config hzConfig = hzInstance.getConfig();
                hzConfig.getNetworkConfig().getInterfaces().clear();

                String clusterName = confCluster.getValue(CONF_KEY_NAME);
                String clusterPasswd = confCluster.getValue(CONF_KEY_PASSWORD);

                //
                // Sanity checks
                //
                Preconditions.checkNotNull(clusterName, "No cluster name "
                        + "defined in configuration file");
                Preconditions.checkNotNull(clusterPasswd, "No cluster password "
                        + "defined in configuration file");

                hzConfig.getGroupConfig().setName(clusterName);
                hzConfig.getGroupConfig().setPassword(clusterPasswd);

                boolean autoIncrement = confHazelcast.getValue(CONF_KEY_HOST_PORT_AUTO_INCR);
                int portCount = confHazelcast.getValue(CONF_KEY_HOST_PORT_COUNT);
                hzConfig.getNetworkConfig().setPort(hostPort);
                hzConfig.getNetworkConfig().setPortAutoIncrement(autoIncrement);
                hzConfig.getNetworkConfig().setPortCount(portCount);

                // Enable interfaces
                hzConfig.getNetworkConfig().getInterfaces().setInterfaces(hostAddrs);

            } else {
                //
                // Create non-clustered platform
                //
                m_logger.info("Starting in non-clustered mode");
                m_manager = PlatformLocator.factory.createPlatformManager();
            }
        }
    }

    private void createDirectories() throws IOException {
        //
        // Create data and temp directory
        //
        String user = System.getProperty(SYS_PROPERTY_USER, DEF_USER);
        Path dataPath = Paths.get(m_optDataDir).toAbsolutePath().normalize();
        final Path tmpPath = Paths.get(m_optTmpDir).toAbsolutePath().normalize();

        //
        // Delete tmp on startup
        //
        if (Files.exists(tmpPath)) {
            m_logger.debug("Deleting {}", tmpPath);
            io.spikex.core.util.Files.deleteDirectory(tmpPath, true);
        }

        io.spikex.core.util.Files.createDirectories(
                user,
                OWNER_FULL_GROUP_EXEC_OTHER_EXEC,
                dataPath,
                tmpPath);

        if (Files.notExists(dataPath)) {
            m_logger.debug("Setting owner of \"{}\" to: {}", dataPath, user);
            io.spikex.core.util.Files.setOwner(
                    user,
                    dataPath);

            if (HostOs.isUnix()) {
                io.spikex.core.util.Files.setUnixGroup(
                        user,
                        dataPath);
            }
        }

        if (Files.notExists(tmpPath)) {
            m_logger.debug("Setting owner of \"{}\" to: {}", tmpPath, user);
            io.spikex.core.util.Files.setOwner(
                    user,
                    tmpPath);

            if (HostOs.isUnix()) {
                io.spikex.core.util.Files.setUnixGroup(
                        user,
                        tmpPath);
            }
        }
    }

    private void createKeyStore(final YamlDocument conf) {

        YamlDocument confKeyStore = conf.getDocument(CONF_KEY_KEYSTORE);
        boolean generate = confKeyStore.getValue(CONF_KEY_GENERATE, DEF_GENERATE_KEYSTORE);

        if (generate) {

            Path keyStorePath = Paths.get(confKeyStore.getValue(CONF_KEY_PATH,
                    m_confPath.resolve(DEF_KEYSTORE_PATH).toString())).toAbsolutePath().normalize();

            if (!Files.exists(keyStorePath)) {

                Provider bcProvider = Security.getProvider(BouncyCastleProvider.PROVIDER_NAME);
                if (bcProvider == null) {
                    Security.addProvider(new BouncyCastleProvider());
                }

                String password = confKeyStore.getValue(CONF_KEY_PASSWORD, DEF_KEYSTORE_PASSWORD);
                String hostFqdn = confKeyStore.getValue(CONF_KEY_HOST_FQDN, HostOs.hostName());
                List<String> subjAltNames = confKeyStore.getValue(CONF_KEY_SUBJECT_ALT_NAME, new ArrayList());

                try (FileOutputStream out = new FileOutputStream(keyStorePath.toFile())) {

                    m_logger.info("Generating keystore: {}", keyStorePath);

                    KeyPairGenerator generator = KeyPairGenerator.getInstance("RSA",
                            BouncyCastleProvider.PROVIDER_NAME);

                    SecureRandom rnd = new SecureRandom();
                    generator.initialize(2048, rnd);
                    KeyPair pair = generator.generateKeyPair();

                    // DN
                    X500NameBuilder nameBuilder = new X500NameBuilder(BCStyle.INSTANCE);
                    nameBuilder.addRDN(BCStyle.C, System.getProperty("user.country.format", "NU"));
                    nameBuilder.addRDN(BCStyle.OU, "Self-signed test certificate");
                    nameBuilder.addRDN(BCStyle.OU, "For testing purposes only");
                    nameBuilder.addRDN(BCStyle.O, "Spike.x");
                    nameBuilder.addRDN(BCStyle.CN, hostFqdn);

                    long oneDay = 24 * 60 * 60 * 1000;
                    Date notBefore = new Date(System.currentTimeMillis() - oneDay); // Yesterday
                    Date notAfter = new Date(System.currentTimeMillis() + (oneDay * 3 * 365)); // 3 years

                    BigInteger serialNum = BigInteger.valueOf(rnd.nextLong());
                    X509v3CertificateBuilder x509v3Builder
                            = new JcaX509v3CertificateBuilder(
                                    nameBuilder.build(),
                                    serialNum,
                                    notBefore,
                                    notAfter,
                                    nameBuilder.build(),
                                    pair.getPublic());

                    //
                    // Extensions
                    //
                    x509v3Builder.addExtension(X509Extensions.BasicConstraints, true,
                            new BasicConstraints(false));
                    x509v3Builder.addExtension(X509Extensions.KeyUsage, true,
                            new KeyUsage(KeyUsage.digitalSignature | KeyUsage.keyEncipherment));
                    x509v3Builder.addExtension(X509Extensions.ExtendedKeyUsage, true,
                            new ExtendedKeyUsage(KeyPurposeId.id_kp_serverAuth));

                    GeneralName[] dnsNames = new GeneralName[subjAltNames.size()];
                    for (int i = 0; i < subjAltNames.size(); i++) {
                        String name = subjAltNames.get(i);
                        m_logger.info("Adding subject alt name: {}", name);
                        dnsNames[i] = new GeneralName(GeneralName.dNSName, name);
                    }
                    x509v3Builder.addExtension(X509Extensions.SubjectAlternativeName, false, new GeneralNames(dnsNames));

                    ContentSigner signer = new JcaContentSignerBuilder("SHA256WithRSAEncryption")
                            .setProvider(BouncyCastleProvider.PROVIDER_NAME)
                            .build(pair.getPrivate());

                    X509Certificate cert = new JcaX509CertificateConverter()
                            .setProvider(BouncyCastleProvider.PROVIDER_NAME)
                            .getCertificate(x509v3Builder.build(signer));

                    // Validate
                    cert.checkValidity(new Date());
                    cert.verify(cert.getPublicKey());

                    // Save in keystore
                    KeyStore ks = KeyStore.getInstance(KeyStore.getDefaultType());
                    ks.load(null);
                    ks.setKeyEntry(
                            hostFqdn,
                            pair.getPrivate(),
                            password.toCharArray(),
                            new Certificate[]{cert});

                    m_logger.info("Created self-signed certificate: {}", hostFqdn);
                    ks.store(out, password.toCharArray());

                } catch (KeyStoreException | IOException | NoSuchAlgorithmException | CertificateException | NoSuchProviderException | OperatorCreationException | InvalidKeyException | SignatureException e) {
                    throw new RuntimeException("Failed to create keystore: " + keyStorePath, e);
                }
            }
        }
    }

    private void createTrustStore(final YamlDocument conf) {

        YamlDocument confTrustStore = conf.getDocument(CONF_KEY_TRUSTSTORE);
        boolean generate = confTrustStore.getValue(CONF_KEY_GENERATE, DEF_GENERATE_TRUSSTORE);

        if (generate) {

            Path trustStorePath = Paths.get(confTrustStore.getValue(CONF_KEY_PATH,
                    m_confPath.resolve(DEF_TRUSTSTORE_PATH)).toString()).toAbsolutePath().normalize();

            Path certsPath = m_confPath.resolve(DEF_TRUSTSTORE_CERTS_DIR).toAbsolutePath().normalize();

            if (!Files.exists(trustStorePath)
                    && Files.exists(certsPath)) {

                Provider bcProvider = Security.getProvider(BouncyCastleProvider.PROVIDER_NAME);
                if (bcProvider == null) {
                    Security.addProvider(new BouncyCastleProvider());
                }
                try {
                    // Create keystore
                    m_logger.info("Generating truststore: {}", trustStorePath);
                    KeyStore ts = KeyStore.getInstance(KeyStore.getDefaultType());
                    ts.load(null);

                    //
                    // Import PEM certificates
                    // https://gist.github.com/akorobov/6910564
                    //
                    try (DirectoryStream<Path> dirStream = Files.newDirectoryStream(certsPath)) {

                        JcaX509CertificateConverter converter
                                = new JcaX509CertificateConverter().setProvider(BouncyCastleProvider.PROVIDER_NAME);

                        for (Path path : dirStream) {
                            PEMParser parser = new PEMParser(new FileReader(path.toFile()));

                            while (true) {
                                int index = 1;
                                Object object = parser.readObject();

                                if (object != null) {
                                    if (object instanceof X509CertificateHolder) {
                                        X509Certificate cert = converter.getCertificate((X509CertificateHolder) object);

                                        m_logger.debug("Certificate issuer: {} subject: {} serial: {} validity: {}-{}",
                                                cert.getIssuerX500Principal().getName(),
                                                cert.getSubjectX500Principal().getName(),
                                                cert.getSerialNumber(),
                                                cert.getNotBefore(),
                                                cert.getNotAfter());

                                        // Validate
                                        cert.checkValidity(new Date());

                                        // Alias
                                        String alias = cert.getSubjectX500Principal().getName();
                                        if (Strings.isNullOrEmpty(alias)) {
                                            alias = "cert-" + index++;
                                        }

                                        // Save in trusstore
                                        ts.setCertificateEntry(
                                                alias,
                                                cert);
                                        m_logger.info("Imported trusted certificate: {}", alias);
                                    }
                                } else {
                                    break;
                                }
                            }
                        }
                    } catch (CertificateException e) {
                        m_logger.error("Failed to import trusted certificate", e);
                    }

                    // Save truststore
                    String password = confTrustStore.getValue(CONF_KEY_PASSWORD, DEF_TRUSTSTORE_PASSWORD);
                    ts.store(new FileOutputStream(trustStorePath.toFile()), password.toCharArray());

                } catch (IOException | NoSuchAlgorithmException | KeyStoreException | CertificateException e) {
                    throw new RuntimeException("Failed to create truststore: " + trustStorePath, e);
                }
            }
        }
    }

    private void deployModules(final YamlDocument conf) {
        //
        // Sanity check
        //
        List<Map> modules = conf.getList(CONF_KEY_MODULES);
        Preconditions.checkNotNull(modules, "No modules defined in "
                + "configuration file");

        //
        // Always available configuration
        //
        String user = System.getProperty(SYS_PROPERTY_USER, DEF_USER);
        YamlDocument confCluster = conf.getDocument(CONF_KEY_CLUSTER);
        Map<String, String> defConf = new HashMap();
        defConf.put(CONF_KEY_NODE_NAME, m_homePath.getFileName().toString());
        defConf.put(CONF_KEY_CLUSTER_NAME, (String) confCluster.getValue(CONF_KEY_NAME));
        defConf.put(CONF_KEY_CLUSTER_PASSWORD, (String) confCluster.getValue(CONF_KEY_PASSWORD));
        defConf.put(CONF_KEY_HOME_PATH, m_homePath.toString());
        defConf.put(CONF_KEY_CONF_PATH, m_confPath.toString());

        // Normalize data and tmp path
        Path dataPath = Paths.get(m_optDataDir).toAbsolutePath().normalize();
        Path tmpPath = Paths.get(m_optTmpDir).toAbsolutePath().normalize();

        defConf.put(CONF_KEY_DATA_PATH, dataPath.toString());
        defConf.put(CONF_KEY_TMP_PATH, tmpPath.toString());
        defConf.put(CONF_KEY_USER, user);

        // Keep track of deployed modules
        final CountDownLatch deployedLatch = new CountDownLatch(modules.size());

        for (Map module : modules) {

            final String moduleId = (String) module.get(CONF_KEY_MODULE_ID);
            final String groupArtifactId = getGroupAndArtifactId(moduleId); // Group and artifact ID
            final String artifactName = getArtifactName(moduleId); // Name only
            final String artifactVersion = getArtifactVersion(moduleId); // Version only

            // Sanity check
            if (Strings.isNullOrEmpty(groupArtifactId)
                    || Strings.isNullOrEmpty(artifactVersion)) {
                m_logger.error("Skipping module. Unable to resolve "
                        + "artifact group, name or version: ", moduleId);
                continue; // Next module
            }

            // Undeploy module if already deployed (and not of same version)
            boolean undeployed = true;
            Module oldModule = findModule(groupArtifactId);
            if (oldModule != null) {
                if (!oldModule.getArtifactVersion().equals(artifactVersion)) {
                    undeployed = undeployModule(oldModule);
                } else {
                    undeployed = false; // Same version, do not redeploy
                    deployedLatch.countDown();
                }
            }

            if (undeployed) {

                // Use group and artifact ID as local address, if not explicitly defined
                String localAddress = groupArtifactId;

                // Build verticle configuration
                Map vertConf = new HashMap(defConf);
                Map confMap = (Map) module.get(CONF_KEY_MODULE_CONFIG);
                if (confMap != null) {
                    String address = (String) confMap.get(CONF_KEY_LOCAL_ADDRESS);
                    if (!Strings.isNullOrEmpty(address)) {
                        localAddress = address;
                    }
                    vertConf.putAll(confMap);
                }

                vertConf.put(CONF_KEY_LOCAL_ADDRESS, localAddress);

                final Integer instances = (Integer) module.get(CONF_KEY_INSTANCES);
                final JsonObject config = new JsonObject(vertConf);

                //
                // Check if module can be found in install directory
                //
                String zipName = artifactName + "-" + artifactVersion + ".zip";
                Path zipPath = Paths.get(m_homePath.toString(), SPIKEX_DEPLOY_DIR, zipName);
                m_logger.debug("Looking for local module: {}", zipPath);
                if (Files.exists(zipPath)) {

                    m_logger.info("Deploying local module: {}", zipPath);
                    m_manager.deployModuleFromZip(zipPath.toString(), config, (instances == null ? 1 : instances),
                            new DeploymentHandler(
                                    moduleId,
                                    groupArtifactId,
                                    artifactVersion,
                                    config.getString(CONF_KEY_LOCAL_ADDRESS),
                                    deployedLatch));
                } else {

                    m_logger.info("Deploying module: {}", moduleId);
                    m_manager.deployModule(moduleId, config, (instances == null ? 1 : instances),
                            new DeploymentHandler(
                                    moduleId,
                                    groupArtifactId,
                                    artifactVersion,
                                    config.getString(CONF_KEY_LOCAL_ADDRESS),
                                    deployedLatch));
                }
            }
        }
        try {
            int maxWaitSec = Integer.parseInt(
                    System.getProperty(SYS_PROPERTY_MAX_DEPLOY_SECS, DEF_MAX_DEPLOY_SECS));

            // Wait max n seconds for deployment to finish
            if (deployedLatch.await(maxWaitSec, TimeUnit.SECONDS)) {
                m_logger.info("Modules deployed successfully");
            } else {
                m_logger.error("Not all modules were deployed within time limit ({} sec)", maxWaitSec);
            }
        } catch (InterruptedException e) {
            m_logger.error("Module deployment interrupted", e);
        }
    }

    private void initModules() {

        for (Module module : m_modules.values()) {
            m_logger.info("Initializing module: {}", module.getModuleId());
            String localAddress = module.getLocalAddress();
            Commands.call(
                    m_manager.vertx().eventBus(),
                    localAddress,
                    Commands.cmdInitVerticle());
        }
    }

    private String getGroupAndArtifactId(final String moduleId) {

        StringBuilder sb = new StringBuilder();
        String group = "";
        String artifact = "";

        Matcher m = REGEXP_MODGROUP.matcher(moduleId);

        if (m.find()) {
            group = m.group();
            if (m.find()) {
                artifact = m.group();
            }
        }

        if (!Strings.isNullOrEmpty(group)
                && !Strings.isNullOrEmpty(artifact)) {
            sb.append(group);
            sb.append('~');
            sb.append(artifact);
        }

        return sb.toString();
    }

    private String getArtifactName(final String moduleId) {

        String name = "";
        Matcher m = REGEXP_MODGROUP.matcher(moduleId);

        if (m.find() && m.find()) {
            name = m.group();
        }

        return name;
    }

    private String getArtifactVersion(final String moduleId) {

        String version = "";
        Matcher m = REGEXP_MODGROUP.matcher(moduleId);

        if (m.find() && m.find() && m.find()) {
            version = m.group();
        }

        return version;
    }

    private Module findModule(final String groupArtifactId) {

        Module module = null;
        Iterator<String> keys = m_modules.keySet().iterator();
        while (keys.hasNext()) {
            String key = keys.next();
            Module tmp = m_modules.get(key);
            if (tmp.getGroupAndArtifactId().equals(groupArtifactId)) {
                module = tmp;
                break;
            }
        }
        return module;
    }

    private static boolean undeployModule(final Module module) {

        final AtomicBoolean undeployed = new AtomicBoolean(false);

        // Synchronous undeployment
        m_logger.info("Undeploying module: {}", module.getModuleId());

        final CountDownLatch undeployLatch = new CountDownLatch(1);
        m_manager.undeploy(module.getDeploymentId(),
                new Handler<AsyncResult<Void>>() {

                    @Override
                    public void handle(AsyncResult<Void> ar) {
                        if (ar.succeeded()) {
                            undeployed.set(true);
                        }
                        undeployLatch.countDown();
                    }
                });
        try {
            int maxWaitSec = Integer.parseInt(
                    System.getProperty(SYS_PROPERTY_MAX_UNDEPLOY_SECS, DEF_MAX_DEPLOY_SECS));

            // Wait max n seconds for undeployment to finish
            if (undeployLatch.await(maxWaitSec, TimeUnit.SECONDS)) {
                m_logger.debug("Module undeployed successfully: {}", module.getModuleId());
            } else {
                m_logger.error("Module was not undeployed within time limit ({} sec): {}",
                        maxWaitSec, module.getModuleId());
            }
        } catch (InterruptedException e) {
            m_logger.error("Module undeployment was interrupted", e);
        }

        return undeployed.get();
    }

    private static class DeploymentHandler implements Handler<AsyncResult<String>> {

        private final String m_moduleId;
        private final String m_groupArtifactId;
        private final String m_artifactVersion;
        private final String m_localAddress;
        private final CountDownLatch m_latch;

        private final Logger m_logger = LoggerFactory.getLogger(DeploymentHandler.class);

        private DeploymentHandler(
                final String moduleId,
                final String groupArtifactId,
                final String artifactVersion,
                final String localAddress,
                final CountDownLatch latch) {

            m_moduleId = moduleId;
            m_groupArtifactId = groupArtifactId;
            m_artifactVersion = artifactVersion;
            m_localAddress = localAddress;
            m_latch = latch;
        }

        @Override
        public void handle(AsyncResult<String> ar) {

            if (ar.succeeded()) {
                String deploymentId = ar.result();
                m_logger.debug("Deployment identifier: {}", deploymentId);
                m_modules.put(m_groupArtifactId,
                        new Module(
                                m_groupArtifactId,
                                m_artifactVersion,
                                m_localAddress,
                                deploymentId)
                );
                m_latch.countDown();
            } else {
                throw new RuntimeException("Failed to deploy module: "
                        + m_moduleId, ar.cause());
            }
        }
    }

    private static class Module {

        private final String m_groupArtifactId;
        private final String m_artifactVersion;
        private final String m_localAddress;
        private final String m_deploymentId;

        private Module(
                final String groupArtifactId,
                final String artifactVersion,
                final String localAddress,
                final String deploymentId) {

            m_groupArtifactId = groupArtifactId;
            m_artifactVersion = artifactVersion;
            m_localAddress = localAddress;
            m_deploymentId = deploymentId;
        }

        private String getGroupAndArtifactId() {
            return m_groupArtifactId;
        }

        private String getArtifactVersion() {
            return m_artifactVersion;
        }

        private String getModuleId() {
            return getGroupAndArtifactId() + "~" + getArtifactVersion();
        }

        private String getLocalAddress() {
            return m_localAddress;
        }

        private String getDeploymentId() {
            return m_deploymentId;
        }
    }
}
