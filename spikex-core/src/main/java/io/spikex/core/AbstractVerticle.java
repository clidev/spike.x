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

import com.eaio.uuid.UUID;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import io.spikex.core.helper.Variables;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Set;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.vertx.java.core.Handler;
import org.vertx.java.core.eventbus.EventBus;
import org.vertx.java.core.eventbus.Message;
import org.vertx.java.core.json.JsonObject;
import org.vertx.java.platform.Verticle;

/**
 * Logging, filters, commands, event bus and sessions...
 *
 * TODO Unify creation of objects - use builder if complex, use Class.create if
 * simple (privatize constructors) TODO Unify usage of guava - has lots of
 * useful helpers (Strings.isNullOrEmpty, Predicates, etc..) TODO Do not call
 * toString when passing arguments to SLF4J logging methods TODO Serializable
 * and "value" classes must implement hashCode and equals TODO Implement
 * toString for "def" and "value" classes (eg. JobDef, Throttle) TODO Unify
 * usage of config(), logger() and eventBus() methods TODO Implement queue
 * support for Limit filter TODO Test and refactor for Windows (eg. service
 * script and creation of dirs) TODO Use guava Objects to check for null or
 * empty parameter TODO Consider using Guava CharMatcher instead of stock regexp
 * Pattern+Matcher TODO Use Path instead of File TODO io.spikex.core.task and
 * job could be moved to mod-scheduler? TODO Consider using gatling for
 * performance testing TODO Support for scripted filters (Javascript filter)
 * TODO Add common raft classes - used for distributed notifier and scheduler
 *
 * @author cli
 */
public abstract class AbstractVerticle extends Verticle {

    // Cluster-wide unique address of this verticle
    private final String m_address;

    // Local address of this verticle
    private String m_localAddress;

    // Verticle timer
    private long m_interval;
    private long m_timerId;

    // Paths
    private Path m_homePath;
    private Path m_dataPath;
    private Path m_confPath;
    private Path m_tmpPath;

    // Verticle configuration
    private JsonObject m_config;

    // Vert.x event bus
    private EventBus m_eventBus;

    // Variables utility class
    private Variables m_vars;

    public static final String CONF_KEY_NODE_NAME = "node-name";
    public static final String CONF_KEY_CLUSTER_NAME = "cluster-name";
    public static final String CONF_KEY_CLUSTER_PASSWORD = "cluster-password";
    public static final String CONF_KEY_LOCAL_ADDRESS = "local-address";
    public static final String CONF_KEY_HOME_PATH = "home-path";
    public static final String CONF_KEY_CONF_PATH = "conf-path";
    public static final String CONF_KEY_DATA_PATH = "data-path";
    public static final String CONF_KEY_TMP_PATH = "tmp-path";
    public static final String CONF_KEY_USER = "spikex-user";
    public static final String CONF_KEY_UPDATE_INTERVAL = "update-interval";

    public static final String SHARED_METRICS_KEY = "io.spikex.metrics";
    public static final String SHARED_SENSORS_KEY = "io.spikex.sensors";

    private static final String EVENT_LOGGER = "io.spikex.events";

    private final Logger m_logger = LoggerFactory.getLogger(getClass());
    private final Logger m_evnLogger = LoggerFactory.getLogger(EVENT_LOGGER);

    public AbstractVerticle() {
        m_interval = 0L;
        m_timerId = 0L;
        m_config = new JsonObject(); // Empty until verticle start method called
        m_localAddress = "";

        // Create unique address
        m_address = getClass().getName() + "." + new UUID().toString();
    }

    @Override
    public void start() {

        logger().debug("Starting {} verticle", getClass().getName());

        m_eventBus = vertx.eventBus();
        m_config = container.config();

        // Sanity check
        Preconditions.checkState(m_config != null
                && !m_config.getFieldNames().isEmpty(),
                "No verticle configuration defined. Stopping.");

        // Spike.x node paths
        m_homePath = Paths.get(m_config.getString(CONF_KEY_HOME_PATH));
        m_confPath = Paths.get(m_config.getString(CONF_KEY_CONF_PATH));
        m_dataPath = Paths.get(m_config.getString(CONF_KEY_DATA_PATH));
        m_tmpPath = Paths.get(m_config.getString(CONF_KEY_TMP_PATH));

        // Start listening to messages on the JVM local address
        String localAddress = m_config.getString(CONF_KEY_LOCAL_ADDRESS);
        if (!Strings.isNullOrEmpty(localAddress)) {
            m_localAddress = localAddress;
            m_eventBus.registerLocalHandler(m_localAddress, new Handler<Message>() {

                @Override
                public void handle(final Message message) {
                    handleLocalMessage(message);
                }

            });
            logger().debug("Listening on local address: {}", m_localAddress);
        }

        // Start listening to messages on the globally unique address
        m_eventBus.registerHandler(m_address, new Handler<Message>() {

            @Override
            public void handle(final Message message) {
                handleMessage(message);
            }

        });
        logger().debug("Listening on cluster-wide address: {}", m_address);

        // Resolve timer interval (support interval suffixes: s, m, h)
        m_interval = 0L;
        if (m_config.containsField(CONF_KEY_UPDATE_INTERVAL)) {

            String value = String.valueOf(m_config.getValue(CONF_KEY_UPDATE_INTERVAL));

            if (value.endsWith("s")
                    || value.endsWith("m")
                    || value.endsWith("h")) {

                int len = value.length();
                long base = Long.parseLong(value.substring(0, len - 1));
                String suffix = value.substring(len - 1);

                switch (suffix) {
                    case "s":
                        m_interval = base * 1000L;
                        break;
                    case "m":
                        m_interval = base * 1000L * 60L;
                        break;
                    case "h":
                        m_interval = base * 1000L * 60L * 60L;
                        break;
                }
            } else {
                m_interval = Long.parseLong(value);
            }
        }

        // Call custom start method
        startVerticle();

        // Start timer
        if (m_interval > 0L) {
            logger().debug("Starting timer with interval: {}", m_interval);
            m_timerId = vertx.setPeriodic(m_interval, new Handler<Long>() {

                @Override
                public void handle(final Long timerId) {
                    handleTimerEvent();
                }
            });
        }

        eventLogger().info("Started {} verticle", getClass().getName());
    }

    @Override
    public void stop() {

        // Perform custom shutdown tasks...
        logger().debug("Stopping {} verticle", getClass().getName());
        stopVerticle();

        // Stop timer
        if (m_timerId != -1) {
            vertx.cancelTimer(m_timerId);
        }

        eventLogger().info("Stopped {} verticle", getClass().getName());
    }

    /**
     * Returns the Vert.x event bus.
     *
     * @return the Vert.x event bus
     */
    protected final EventBus eventBus() {
        return m_eventBus;
    }

    /**
     * Returns the variable resolver.
     *
     * @return the variable resolver
     */
    protected final Variables variables() {

        Variables vars = m_vars;

        // Create variables utility instance
        if (vars == null) {
            vars = createVariables();
            m_vars = vars;
        }

        return vars;
    }

    protected final HazelcastInstance hazelcastInstance() {
        //
        // Retrieve hazelcast used by Vert.x (first instance)
        //
        Set<HazelcastInstance> hzInstances = Hazelcast.getAllHazelcastInstances();
        if (hzInstances != null && hzInstances.size() > 0) {
            return hzInstances.iterator().next();
        } else {
            return null;
        }
    }

    protected Variables createVariables() {
        return new Variables(config(), vertx);
    }

    protected final Path homePath() {
        return m_homePath;
    }

    protected final Path confPath() {
        return m_confPath;
    }

    protected final Path dataPath() {
        return m_dataPath;
    }

    protected final Path tmpPath() {
        return m_tmpPath;
    }

    protected final String address() {
        return m_address;
    }

    protected final String localAddress() {
        return m_localAddress;
    }

    /**
     * Returns the verticle configuration.
     *
     * @return the Vert.x event bus
     */
    protected final JsonObject config() {
        return m_config;
    }

    protected final long updateInterval() {
        return m_interval;
    }

    /**
     * Start verticle.
     */
    protected abstract void startVerticle();

    /**
     * Stop verticle.
     */
    protected void stopVerticle() {
        // Do nothing by default...
    }

    /**
     * Initialize verticle.
     */
    protected void initVerticle() {
        // Do nothing by default...
    }

    /**
     * Handle JVM local message.
     *
     * @param message the received message
     */
    protected void handleLocalMessage(final Message message) {
        // Do nothing by default...
    }

    /**
     * Handle cluster-wide message.
     *
     * @param message the received message
     */
    protected void handleMessage(final Message message) {
        // Do nothing by default...
    }

    /**
     * Handle verticle timer event.
     */
    protected void handleTimerEvent() {
        // Do nothing by default...
    }

    /**
     * Returns the standard logger that is used by this verticle.
     *
     * @return the standard logger
     */
    protected final Logger logger() {
        return m_logger;

    }

    /**
     * Returns the event logger that is used by this verticle.
     *
     * @return the event logger
     */
    protected final Logger eventLogger() {
        return m_evnLogger;

    }
}
