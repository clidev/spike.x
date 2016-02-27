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
package io.spikex.core.helper;

import static io.spikex.core.AbstractFilter.CONF_KEY_CHAIN_NAME;
import static io.spikex.core.AbstractVerticle.CONF_KEY_CLUSTER_NAME;
import static io.spikex.core.AbstractVerticle.CONF_KEY_CONF_PATH;
import static io.spikex.core.AbstractVerticle.CONF_KEY_DATA_PATH;
import static io.spikex.core.AbstractVerticle.CONF_KEY_HOME_PATH;
import static io.spikex.core.AbstractVerticle.CONF_KEY_NODE_NAME;
import static io.spikex.core.AbstractVerticle.CONF_KEY_TMP_PATH;
import static io.spikex.core.AbstractVerticle.SHARED_METRICS_KEY;
import static io.spikex.core.AbstractVerticle.SHARED_SENSORS_KEY;
import io.spikex.core.util.HostOs;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.ISODateTimeFormat;
import org.vertx.java.core.Vertx;
import org.vertx.java.core.json.JsonObject;

/**
 * Variable resolving utility class.
 * <p>
 * This class is not thread-safe.
 *
 * @author cli
 */
public final class Variables {

    private final Map<String, String> m_systemEnv; // System env variables
    private final Map<Object, Object> m_systemProps; // System properties

    private final Vertx m_vertx; // Used to get shared metric and sensor data

    private final String m_nodeName;
    private final String m_clusterName;
    private final String m_hostName;
    private final String m_spikexHome;
    private final String m_spikexConf;
    private final String m_spikexData;
    private final String m_spikexTmp;
    private final String m_chainName;

    // Built-ins
    private static final String BUILTIN_PREFIX = "#";
    private static final String BUILTIN_NODE = "#node";
    private static final String BUILTIN_CLUSTER = "#cluster";
    private static final String BUILTIN_CHAIN = "#chain";
    private static final String BUILTIN_HOST = "#host";
    private static final String BUILTIN_DATE = "#date";
    private static final String BUILTIN_TIMESTAMP = "#timestamp";
    private static final String BUILTIN_NOW = "#now";
    private static final String BUILTIN_NOW_EXTENDED = "#now(";
    private static final String BUILTIN_ENV = "#env.";
    private static final String BUILTIN_PROP = "#prop.";
    private static final String BUILTIN_METRIC = "#metric.";
    private static final String BUILTIN_SENSOR = "#sensor.";
    private static final String BUILTIN_SDF = "#+";
    private static final String BUILTIN_SPIKEX_HOME = "#spikex.home";
    private static final String BUILTIN_SPIKEX_CONF = "#spikex.conf";
    private static final String BUILTIN_SPIKEX_DATA = "#spikex.data";
    private static final String BUILTIN_SPIKEX_TMP = "#spikex.tmp";

    private static final Pattern REGEXP_NOW
            = Pattern.compile("#now\\("
                    + "([A-Z][0-9\\w\\-\\+_/]+)?,?" // Timezone
                    + "([\\+\\-]?[0-9]+h)?,?" // Hours
                    + "([\\+\\-]?[0-9]+m)?,?" // Minutes
                    + "([\\+\\-]?[0-9]+s)?" // Seconds
                    + "\\)");

    private static final String VAR_PREFIX = "%{";
    private static final String VAR_SUFFIX = "}"; // Must be only one character long

    public static final String VAR_SPIKEX_HOME = VAR_PREFIX + BUILTIN_SPIKEX_HOME + VAR_SUFFIX;
    public static final String VAR_SPIKEX_CONF = VAR_PREFIX + BUILTIN_SPIKEX_CONF + VAR_SUFFIX;
    public static final String VAR_SPIKEX_DATA = VAR_PREFIX + BUILTIN_SPIKEX_DATA + VAR_SUFFIX;
    public static final String VAR_SPIKEX_TMP = VAR_PREFIX + BUILTIN_SPIKEX_TMP + VAR_SUFFIX;

    public Variables(final JsonObject nodeConfig) {
        this(nodeConfig, null);
    }

    public Variables(
            final JsonObject nodeConfig,
            final Vertx vertx) {
        //
        m_systemEnv = new ConcurrentHashMap();
        m_systemEnv.putAll(System.getenv());
        //
        m_systemProps = new ConcurrentHashMap();
        Properties props = System.getProperties();
        for (Entry<Object, Object> entry : props.entrySet()) {
            m_systemProps.put(entry.getKey(), entry.getValue());
        }
        //
        m_vertx = vertx;
        //
        m_nodeName = nodeConfig.getString(CONF_KEY_NODE_NAME, "");
        m_clusterName = nodeConfig.getString(CONF_KEY_CLUSTER_NAME, "");
        m_spikexHome = nodeConfig.getString(CONF_KEY_HOME_PATH, "");
        m_spikexConf = nodeConfig.getString(CONF_KEY_CONF_PATH, "");
        m_spikexData = nodeConfig.getString(CONF_KEY_DATA_PATH, "");
        m_spikexTmp = nodeConfig.getString(CONF_KEY_TMP_PATH, "");
        m_chainName = nodeConfig.getString(CONF_KEY_CHAIN_NAME, "");
        m_hostName = HostOs.hostName();
    }

    public <T extends Object, V extends Object> T translate(final V value) {
        return translate(null, value);
    }

    public <T extends Object, V extends Object> T translate(
            final JsonObject event,
            final V value) {

        Object result = value;

        if (value != null
                && value instanceof String) {

            String strValue = (String) value;
            StringBuilder sb = new StringBuilder();

            int strLen = strValue.length();
            int pfxLen = VAR_PREFIX.length();
            int pfxPos = strValue.indexOf(VAR_PREFIX);
            int sfxPos = strValue.indexOf(VAR_SUFFIX);
            int pos = 0;

            while (pfxPos >= 0
                    && sfxPos > pfxPos) {

                String var = strValue.substring(pfxPos + pfxLen, sfxPos);
                result = resolveValue(event, var);

                if (result != null) {
                    if (pfxPos > 0
                            || sfxPos < (strLen - 1)) {
                        sb.append(strValue.substring(pos, pfxPos));
                        sb.append(String.valueOf(result));
                    }
                } else {
                    sb.append(strValue.substring(pos, sfxPos + 1));
                }

                if (sfxPos < (strLen - 1)) {

                    pos = sfxPos + 1;
                    pfxPos = strValue.indexOf(VAR_PREFIX, pos);
                    sfxPos = strValue.indexOf(VAR_SUFFIX, pos);

                } else {
                    pos = strLen;
                    pfxPos = -1; // break
                }
            }

            if (sb.length() > 0) {
                if (pos < strLen) {
                    sb.append(strValue.substring(pos, strLen));
                }
                result = sb.toString();
            }
        }

        return (T) (result == null ? "" : result);
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("node.name: ");
        sb.append(m_nodeName);
        sb.append(",cluster.name: ");
        sb.append(m_clusterName);
        sb.append(",host.name: ");
        sb.append(m_hostName);
        sb.append(",spikex.home: ");
        sb.append(m_spikexHome);
        sb.append(",spikex.conf: ");
        sb.append(m_spikexConf);
        sb.append(",spikex.data: ");
        sb.append(m_spikexData);
        sb.append(",spikex.tmp: ");
        sb.append(m_spikexTmp);
        sb.append(",chain.name: ");
        sb.append(m_chainName);
        sb.append(",env-vars: ");
        sb.append(m_systemEnv);
        sb.append(",sys-props: ");
        sb.append(m_systemProps);
        sb.append(",metrics: {");
        {
            Set<Entry<Object, Object>> entries = m_vertx.sharedData()
                    .getMap(SHARED_METRICS_KEY).entrySet();
            for (Entry entry : entries) {
                sb.append(String.valueOf(entry.getKey()));
                sb.append("=");
                sb.append(String.valueOf(entry.getValue()));
                sb.append(", ");
            }
        }
        sb.append(",sensors: {");
        {
            Set<Entry<Object, Object>> entries = m_vertx.sharedData()
                    .getMap(SHARED_SENSORS_KEY).entrySet();
            for (Entry entry : entries) {
                sb.append(String.valueOf(entry.getKey()));
                sb.append("=");
                sb.append(String.valueOf(entry.getValue()));
                sb.append(", ");
            }
        }
        sb.append("}");
        return sb.toString();
    }

    private Object resolveValue(
            final JsonObject event,
            final String var) {

        Object value = null;

        if (var.length() > 0) {

            if (var.startsWith(BUILTIN_PREFIX)) {

                DateTime dtNow;
                DateTimeFormatter fmt;

                if (var.startsWith(BUILTIN_SDF)) {
                    // Simple date format
                    String pattern = var.substring(BUILTIN_SDF.length());
                    dtNow = new DateTime(DateTimeZone.UTC);
                    fmt = DateTimeFormat.forPattern(pattern);
                    value = fmt.print(dtNow);
                } else if (var.startsWith(BUILTIN_ENV)) {
                    // env
                    Object val = m_systemEnv.get(var.substring(BUILTIN_ENV.length()));
                    value = (val != null ? String.valueOf(val) : "");
                } else if (var.startsWith(BUILTIN_PROP)) {
                    // prop
                    Object val = m_systemProps.get(var.substring(BUILTIN_PROP.length()));
                    value = (val != null ? String.valueOf(val) : "");
                } else if (var.startsWith(BUILTIN_METRIC)) {
                    // metrics
                    if (m_vertx != null) {
                        Map<String, Object> values = m_vertx.sharedData().getMap(SHARED_METRICS_KEY);
                        value = values.get(var.substring(BUILTIN_METRIC.length()));
                    }
                } else if (var.startsWith(BUILTIN_SENSOR)) {
                    // sensor
                    if (m_vertx != null) {
                        Map<String, Object> values = m_vertx.sharedData().getMap(SHARED_SENSORS_KEY);
                        value = values.get(var.substring(BUILTIN_SENSOR.length()));
                    }
                } else if (var.startsWith(BUILTIN_NOW_EXTENDED)) {
                    // now extended with timezone and time offset
                    DateTime dt = Variables.createDateTimeNow(var);
                    value = dt.getMillis();
                } else {

                    switch (var) {
                        case BUILTIN_NODE:
                            value = m_nodeName;
                            break;
                        case BUILTIN_CLUSTER:
                            value = m_clusterName;
                            break;
                        case BUILTIN_SPIKEX_HOME:
                            value = m_spikexHome;
                            break;
                        case BUILTIN_SPIKEX_CONF:
                            value = m_spikexConf;
                            break;
                        case BUILTIN_SPIKEX_DATA:
                            value = m_spikexData;
                            break;
                        case BUILTIN_SPIKEX_TMP:
                            value = m_spikexTmp;
                            break;
                        case BUILTIN_CHAIN:
                            value = m_chainName;
                            break;
                        case BUILTIN_HOST:
                            value = m_hostName;
                            break;
                        case BUILTIN_DATE:
                            dtNow = new DateTime(DateTimeZone.UTC);
                            fmt = ISODateTimeFormat.basicDate();
                            value = fmt.print(dtNow);
                            break;
                        case BUILTIN_TIMESTAMP:
                            dtNow = new DateTime(DateTimeZone.UTC);
                            fmt = ISODateTimeFormat.basicDateTime();
                            value = fmt.print(dtNow);
                            break;
                        case BUILTIN_NOW:
                            value = System.currentTimeMillis();
                            break;
                        default:
                            value = var; // Just return the variable def
                            break;
                    }
                }
            } else {
                //
                // Retrieve value from existing field in the event
                //
                if (event != null) {
                    value = event.getValue(var);
                }
            }
        }
        return value;
    }

    public static DateTime createDateTimeNow(final String var) {

        DateTime dt;

        // #now
        // #now(UTC)
        // #now(UTC,0h,-10m,0s)
        // #now(0h,-10m,0s)
        // #now(30m)
        Matcher m = REGEXP_NOW.matcher(var);
        //
        // Timezone
        //
        String tz = null;
        boolean found = m.find();
        if (found) {
            tz = m.group(1);
        }
        //
        // Hours
        //
        int hours = 0;
        if (found) {
            String val = m.group(2);
            if (val != null) {
                hours = Integer.parseInt(val.substring(0, val.length() - 1));
            }
        }
        //
        // Minutes
        //
        int mins = 0;
        if (found) {
            String val = m.group(3);
            if (val != null) {
                mins = Integer.parseInt(val.substring(0, val.length() - 1));
            }
        }
        //
        // Seconds
        //
        int secs = 0;
        if (found) {
            String val = m.group(4);
            if (val != null) {
                secs = Integer.parseInt(val.substring(0, val.length() - 1));
            }
        }

        System.out.println("var: " + var + " tz: " + tz + " hours: " + hours + " mins: " + mins + " secs: " + secs);

        if (tz != null) {
            dt = new DateTime(DateTimeZone.forID(tz));
        } else {
            dt = new DateTime();
        }

        dt = dt.plusHours(hours);
        dt = dt.plusMinutes(mins);
        dt = dt.plusSeconds(secs);

        return dt;
    }
}
