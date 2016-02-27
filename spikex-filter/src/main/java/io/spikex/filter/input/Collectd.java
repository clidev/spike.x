/**
 *
 * Copyright (c) 2016 NG Modular Oy.
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
package io.spikex.filter.input;

import io.spikex.core.AbstractFilter;
import io.spikex.core.helper.Events;
import static io.spikex.core.helper.Events.DSTIME_PRECISION_MILLIS;
import static io.spikex.core.helper.Events.DSTIME_PRECISION_SEC;
import static io.spikex.core.helper.Events.EVENT_FIELD_TAGS;
import static io.spikex.core.helper.Events.TIMEZONE_UTC;
import io.spikex.filter.internal.CollectdMapping;
import static io.spikex.filter.internal.CollectdMapping.DEF_MAPPING;
import static io.spikex.filter.internal.CollectdMapping.SPIKEX_KEY_DSNAME;
import static io.spikex.filter.internal.CollectdMapping.SPIKEX_KEY_DSTYPE;
import static io.spikex.filter.internal.CollectdMapping.SPIKEX_KEY_INSTANCE;
import static io.spikex.filter.internal.CollectdMapping.SPIKEX_KEY_SUBGROUP;
import io.spikex.filter.internal.CollectdMeasurement;
import static io.spikex.filter.internal.CollectdMeasurement.DSTYPE_ABSOLUTE;
import static io.spikex.filter.internal.CollectdMeasurement.DSTYPE_COUNTER;
import static io.spikex.filter.internal.CollectdMeasurement.DSTYPE_DERIVE;
import static io.spikex.filter.internal.CollectdMeasurement.DSTYPE_GAUGE;
import static io.spikex.filter.internal.CollectdTypes.COLLECTD_KEY_PLUGIN;
import static io.spikex.filter.internal.CollectdTypes.TYPES_DB;
import static io.spikex.filter.internal.CollectdTypes.TYPE_ENCRYPTION;
import static io.spikex.filter.internal.CollectdTypes.TYPE_HOST;
import static io.spikex.filter.internal.CollectdTypes.TYPE_INTERVAL_HIGHRES;
import static io.spikex.filter.internal.CollectdTypes.TYPE_INTERVAL_RRD;
import static io.spikex.filter.internal.CollectdTypes.TYPE_MESSAGE;
import static io.spikex.filter.internal.CollectdTypes.TYPE_PLUGIN;
import static io.spikex.filter.internal.CollectdTypes.TYPE_PLUGIN_INSTANCE;
import static io.spikex.filter.internal.CollectdTypes.TYPE_SEVERITY;
import static io.spikex.filter.internal.CollectdTypes.TYPE_SIGNATURE;
import static io.spikex.filter.internal.CollectdTypes.TYPE_TIME_EPOCH;
import static io.spikex.filter.internal.CollectdTypes.TYPE_TIME_HIGHRES;
import static io.spikex.filter.internal.CollectdTypes.TYPE_TYPE;
import static io.spikex.filter.internal.CollectdTypes.TYPE_TYPE_INSTANCE;
import static io.spikex.filter.internal.CollectdTypes.TYPE_VALUES;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.HashMap;
import java.util.Map;
import org.vertx.java.core.AsyncResult;
import org.vertx.java.core.AsyncResultHandler;
import org.vertx.java.core.Handler;
import org.vertx.java.core.buffer.Buffer;
import org.vertx.java.core.datagram.DatagramPacket;
import org.vertx.java.core.datagram.DatagramSocket;
import org.vertx.java.core.datagram.InternetProtocolFamily;
import org.vertx.java.core.json.JsonArray;
import org.vertx.java.core.json.JsonObject;

/**
 * namespace.instrumented section.target (noun).action (past tense verb)
 * accounts.authentication.password.attempted
 * accounts.authentication.password.succeeded
 * accounts.authentication.password.failed
 *
 * http://matt.aimonetti.net/posts/2013/06/26/practical-guide-to-graphite-monitoring
 * http://obfuscurity.com/2012/05/Organizing-Your-Graphite-Metrics
 *
 * @author cli
 */
public final class Collectd extends AbstractFilter {

    private int m_secLevel;
    private JsonArray m_tags;
    private final CollectdMeasurement m_measurement;
    private final Map<String, CollectdMapping> m_mappings;

    private static final String CONF_KEY_HOST = "host";
    private static final String CONF_KEY_PORT = "port";
    private static final String CONF_KEY_MCAST_GROUP = "multicast-group";
    private static final String CONF_KEY_SECURITY_LEVEL = "security-level";
    private static final String CONF_KEY_MAPPINGS = "mappings";
    private static final String CONF_KEY_DSNAME_SEPARATOR = "dsname-separator";
    private static final String CONF_KEY_ITEM_SEPARATOR = "item-separator";
    private static final String CONF_KEY_ADD_TAGS = "add-tags";

    // Security levels
    private static final String SECURITY_LEVEL_NONE = "None";
    private static final String SECURITY_LEVEL_SIGN = "Sign";
    private static final String SECUIRYT_LEVEL_ENCRYPT = "Encrypt";

    private static final int SEC_NONE = 0;
    private static final int SEC_SIGN = 1;
    private static final int SEC_ENCRYPT = 2;

    // Configuration defaults
    private static final int DEF_PORT = 25826;
    private static final String DEF_HOST = "localhost";
    private static final String DEF_MCAST_GROUP = "239.192.74.66";
    private static final String DEF_SECURITY_LEVEL = SECURITY_LEVEL_NONE;
    private static final String DEF_DSNAME_SEPARATOR = ".";
    private static final String DEF_ITEM_SEPARATOR = "_";

    public Collectd() {
        m_measurement = new CollectdMeasurement();
        m_mappings = new HashMap();
    }

    @Override
    protected void startFilter() {

        final int port = config().getInteger(CONF_KEY_PORT, DEF_PORT);
        final String host = config().getString(CONF_KEY_HOST, DEF_HOST);
        final String mcastGroup = config().getString(CONF_KEY_MCAST_GROUP, DEF_MCAST_GROUP);
        final String securityLevel = config().getString(CONF_KEY_SECURITY_LEVEL, DEF_SECURITY_LEVEL);

        m_secLevel = SEC_NONE;
        switch (securityLevel) {
            case SECURITY_LEVEL_SIGN:
                m_secLevel = SEC_SIGN;
                break;
            case SECUIRYT_LEVEL_ENCRYPT:
                m_secLevel = SEC_ENCRYPT;
                break;
        }

        // Tags to add
        m_tags = config().getArray(CONF_KEY_ADD_TAGS, new JsonArray());

        String dsnameSeparator = config().getString(CONF_KEY_DSNAME_SEPARATOR, DEF_DSNAME_SEPARATOR);
        String itemSeparator = config().getString(CONF_KEY_ITEM_SEPARATOR, DEF_ITEM_SEPARATOR);

        // Build default mappings
        CollectdMapping.buildDefaults(m_mappings,
                dsnameSeparator,
                itemSeparator);

        // Mappings defined in the configuration
        JsonArray jsonMappings = config().getArray(CONF_KEY_MAPPINGS, new JsonArray());
        for (int i = 0; i < jsonMappings.size(); i++) {

            JsonObject jsonMapping = (JsonObject) jsonMappings.get(i);
            String plugin = jsonMapping.getString(COLLECTD_KEY_PLUGIN);

            CollectdMapping mapping = new CollectdMapping(
                    jsonMapping.getString(SPIKEX_KEY_DSNAME, ""),
                    jsonMapping.getString(SPIKEX_KEY_DSTYPE, ""),
                    jsonMapping.getString(SPIKEX_KEY_SUBGROUP, ""),
                    jsonMapping.getString(SPIKEX_KEY_INSTANCE, ""),
                    dsnameSeparator,
                    itemSeparator);

            m_mappings.put(plugin, mapping);
        }

        // 
        // Start listening to packets
        //
        final DatagramSocket socket = vertx.createDatagramSocket(InternetProtocolFamily.IPv4);
        socket.listen(host, port, new AsyncResultHandler<DatagramSocket>() {
            @Override
            public void handle(final AsyncResult<DatagramSocket> ar) {
                if (ar.succeeded()) {

                    logger().info("Listening on {}:{}", host, port);

                    socket.dataHandler(new Handler<DatagramPacket>() {
                        @Override
                        public void handle(final DatagramPacket packet) {
                            handlePacket(packet);
                        }
                    });

                    // Also join multicast group
                    socket.listenMulticastGroup(mcastGroup, new AsyncResultHandler<DatagramSocket>() {
                        @Override
                        public void handle(final AsyncResult<DatagramSocket> ar) {
                            if (ar.failed()) {
                                logger().error("Failed to join multicast group: {}",
                                        mcastGroup, ar.cause());
                            }
                        }
                    });

                } else {
                    logger().error("Failed to bind to {}:{}", host, port,
                            ar.cause());
                }
            }
        });
    }

    private void handlePacket(final DatagramPacket packet) {

        try {

            final Buffer buffer = packet.data();
            logger().trace("Packet size: {}", buffer.length());

            CollectdMeasurement measurement = m_measurement;
            measurement.clear();
            String precision = DSTIME_PRECISION_SEC; // Default

            int i = 0;
            while (i < buffer.length()) {

                // Next part
                int partType = buffer.getShort(i);
                int partLen = buffer.getShort(i + 2);
                int pos = i + 4;
                int strLen = i + partLen - 1;

                switch (partType) {
                    case TYPE_HOST: {
                        measurement.setHostname(buffer, pos, strLen);
                        break;
                    }
                    case TYPE_TIME_EPOCH: {
                        long tm = buffer.getLong(pos) * 1000L; // ms
                        measurement.setTimestamp(tm);
                        precision = DSTIME_PRECISION_SEC; // Seconds
                        break;
                    }
                    case TYPE_TIME_HIGHRES: {
                        long tm = (buffer.getLong(pos) / 1073741824L * 1000L); // ms
                        // Add millis - we want ms precision
                        long millis = System.currentTimeMillis();
                        long epoch = (millis / 1000L) * 1000L;
                        measurement.setTimestamp(tm + (millis - epoch));
                        precision = DSTIME_PRECISION_MILLIS; // Higher precision
                        break;
                    }
                    case TYPE_PLUGIN: {
                        measurement.setPlugin(buffer, pos, strLen);
                        break;
                    }
                    case TYPE_PLUGIN_INSTANCE: {
                        measurement.setPluginInstance(buffer, pos, strLen);
                        break;
                    }
                    case TYPE_TYPE: {
                        measurement.setType(buffer, pos, strLen);
                        break;
                    }
                    case TYPE_TYPE_INSTANCE: {
                        measurement.setTypeInstance(buffer, pos, strLen);
                        break;
                    }
                    case TYPE_VALUES: {
                        int numValues = buffer.getShort(pos++);
                        pos++;
                        // Read types
                        byte[] types = new byte[numValues];
                        for (int n = 0; n < numValues; n++) {
                            types[n] = buffer.getByte(pos++);
                        }
                        // Read actual values
                        for (int n = 0; n < numValues; n++) {
                            switch (types[n]) {
                                // COUNTER
                                case 0: {
                                    long value = buffer.getLong(pos);
                                    measurement.pushValue(DSTYPE_COUNTER, value);
                                    break;
                                }
                                // GAUGE
                                case 1: {
                                    ByteBuffer buf = ByteBuffer.wrap(buffer.getBytes(pos, pos + 8));
                                    double value = buf.order(ByteOrder.LITTLE_ENDIAN).getDouble();
                                    measurement.pushValue(DSTYPE_GAUGE, value);
                                    break;
                                }
                                // DERIVE
                                case 2: {
                                    long value = buffer.getLong(pos);
                                    measurement.pushValue(DSTYPE_DERIVE, value);
                                    break;
                                }
                                // ABSOLUTE
                                case 3: {
                                    long value = buffer.getLong(pos);
                                    measurement.pushValue(DSTYPE_ABSOLUTE, value);
                                    break;
                                }
                            }
                            pos += 8; // 64 bit field
                        }
                        break;
                    }
                    case TYPE_INTERVAL_RRD: {
                        long interval = buffer.getLong(i + 4);
                        measurement.setInterval(interval);
                        break;
                    }
                    case TYPE_INTERVAL_HIGHRES: {
                        long interval = (buffer.getLong(i + 4) / 1073741824L);
                        measurement.setInterval(interval);
                        break;
                    }
                    case TYPE_MESSAGE:
                        logger().trace("Message - not supported");
                        break;
                    case TYPE_SEVERITY:
                        logger().trace("Severity - not supported");
                        break;
                    case TYPE_SIGNATURE:
                        logger().trace("Signature - not supported");
                        break;
                    case TYPE_ENCRYPTION:
                        logger().trace("Encryption - not supported");
                        break;
                }

                String destAddr = getDestinationAddress();
                if (measurement.hasValues()
                        && destAddr != null && destAddr.length() > 0) {

                    logger().trace("{} - precision {}", measurement, precision);

                    String plugin = measurement.getPlugin();
                    String[] dsnames = TYPES_DB.get(plugin);

                    for (int n = 0; n < measurement.getValueCount(); n++) {

                        Object value = measurement.getValue(n);
                        String dstype = measurement.getDstype(n);
                        String dsname = "value";
                        if (dsnames != null && n < dsnames.length) {
                            dsname = dsnames[n];
                        }

                        //
                        // Create new event per value
                        //
                        JsonObject event = Events.createMetricEvent(this,
                                measurement.getTimestamp(),
                                TIMEZONE_UTC,
                                measurement.getHostname(),
                                dsname,
                                dstype,
                                precision,
                                "-", // subgroup
                                "-", // instance
                                measurement.getInterval(),
                                value);

                        //
                        // Resolve and set dsname, subgroup and instance
                        //
                        CollectdMapping mapping = m_mappings.get(measurement.getPlugin());

                        // Use default mapping?
                        if (mapping == null) {
                            mapping = m_mappings.get(DEF_MAPPING);
                        }

                        mapping.resolveAndSetFields(
                                event,
                                measurement.getPlugin(),
                                measurement.getPluginInstance(),
                                measurement.getType(),
                                measurement.getTypeInstance(),
                                dsname,
                                dstype);

                        // Add tags
                        event.putArray(EVENT_FIELD_TAGS, m_tags);

                        eventBus().publish(destAddr, event);
                    }

                    measurement.clear(); // Ready for next round
                }

                i += partLen;
            }
        } catch (Exception e) {
            logger().error("Failed to handle packet", e);
        }
    }
}
