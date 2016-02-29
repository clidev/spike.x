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
package io.spikex.filter.output;

import com.google.common.base.CharMatcher;
import com.google.common.base.Strings;
import com.google.common.escape.Escaper;
import com.google.common.net.UrlEscapers;
import io.netty.handler.codec.http.HttpHeaders;
import static io.spikex.core.helper.Events.DSTIME_PRECISION_HOUR;
import static io.spikex.core.helper.Events.DSTIME_PRECISION_MICROS;
import static io.spikex.core.helper.Events.DSTIME_PRECISION_MILLIS;
import static io.spikex.core.helper.Events.DSTIME_PRECISION_MIN;
import static io.spikex.core.helper.Events.DSTIME_PRECISION_NANOS;
import static io.spikex.core.helper.Events.DSTIME_PRECISION_SEC;
import static io.spikex.core.helper.Events.EVENT_FIELD_BATCH_EVENTS;
import static io.spikex.core.helper.Events.EVENT_FIELD_BATCH_SIZE;
import static io.spikex.core.helper.Events.EVENT_FIELD_DSTIME_PRECISION;
import static io.spikex.core.helper.Events.EVENT_FIELD_ID;
import io.spikex.core.util.Base64;
import io.spikex.core.util.Numbers;
import io.spikex.core.util.connection.AsbtractHttpClient;
import io.spikex.core.util.connection.ConnectionException;
import io.spikex.core.util.connection.DefaultConnectionExceptionHandler;
import io.spikex.core.util.connection.HttpClientAdapter;
import io.spikex.core.util.connection.HttpClientResponseAdapter;
import io.spikex.core.util.connection.HttpConnection;
import io.spikex.core.util.connection.IConnection;
import io.spikex.filter.internal.Rule;
import java.util.ArrayList;
import java.util.List;
import org.msgpack.core.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.vertx.java.core.buffer.Buffer;
import org.vertx.java.core.http.HttpClient;
import org.vertx.java.core.http.HttpClientRequest;
import org.vertx.java.core.http.HttpClientResponse;
import static org.vertx.java.core.http.HttpHeaders.CONTENT_LENGTH;
import org.vertx.java.core.json.JsonArray;
import org.vertx.java.core.json.JsonObject;

/**
 * https://github.com/influxdb/influxdb/issues/4226
 *
 * <pre>
 * Timestamp provided
 * no precision parameter, no line protocol suffix: no truncation, time is considered ns
 * precision parameter provided, no line protocol suffix: consider timestamp the parameter specified precision
 * precision parameter provided, line protocol suffix provided: ignore param, use line protocol for timestamp precision, no truncation
 * </pre>
 *
 * @author cli
 */
public final class InfluxDb extends AsbtractHttpClient {

    private String m_databaseName;
    private String m_consistency;
    private String m_precision;
    private List<Serie> m_series;

    private static final String CONF_KEY_DATABASE_NAME = "database-name";
    private static final String CONF_KEY_ADMIN_USER = "admin-user";
    private static final String CONF_KEY_ADMIN_PASSWORD = "admin-password";
    private static final String CONF_KEY_CREATE_DATABASE_ON_STARTUP = "create-database-on-startup";
    private static final String CONF_KEY_RETENTION_DURATION = "retention-duration";
    private static final String CONF_KEY_CONSISTENCY = "consistency";
    private static final String CONF_KEY_PRECISION = "precision";
    private static final String CONF_KEY_SERIES = "series";

    //
    // Configuration defaults
    //
    private static final String DEF_DATABASE_NAME = "spikex";
    private static final String DEF_RETENTION_DURATION = "INF";
    private static final String DEF_CONSISTENCY = "ALL";
    private static final String DEF_PRECISION = DSTIME_PRECISION_MILLIS;
    private static final boolean DEF_CREATE_DATABASE_ON_STARTUP = false;

    //
    // InfluxDB URIs
    //
    private static final String INFLUXDB_STATUS_URI = "/ping"; // GET
    private static final String INFLUXDB_QUERY_URI = "/query?q="; // GET
    private static final String INFLUXDB_PUBLISH_URI = "/write?db="; // POST

    //
    // InfluxDB data types
    //
    private static final String INFLUXDB_DATA_TYPE_BOOLEAN = "boolean";
    private static final String INFLUXDB_DATA_TYPE_FLOAT = "float"; // Default
    private static final String INFLUXDB_DATA_TYPE_INTEGER = "integer";
    private static final String INFLUXDB_DATA_TYPE_STRING = "string";

    // Common data source types
    private static final String TYPE_GAUGE = "GAUGE";
    private static final String TYPE_COUNTER = "COUNTER";
    private static final String TYPE_ABSOLUTE = "ABSOLUTE";
    private static final String TYPE_DERIVE = "DERIVE";
    private static final String TYPE_STRING = "STRING";

    @Override
    protected void startClient() {

        // URL escape database name and admin (the others must be predefined constants)
        m_databaseName = config().getString(CONF_KEY_DATABASE_NAME, DEF_DATABASE_NAME);
        m_consistency = config().getString(CONF_KEY_CONSISTENCY, DEF_CONSISTENCY);
        m_precision = config().getString(CONF_KEY_PRECISION, DEF_PRECISION);

        String adminUser = config().getString(CONF_KEY_ADMIN_USER, "");
        String adminPassword = config().getString(CONF_KEY_ADMIN_PASSWORD, "");

        //
        // Series
        // 
        m_series = new ArrayList();
        JsonArray series = config().getArray(CONF_KEY_SERIES, new JsonArray());
        for (int i = 0; i < series.size(); i++) {
            JsonObject serieConfig = series.get(i);
            m_series.add(Serie.create(serieConfig));
        }

        //
        // Sanity checks
        //
        Preconditions.checkArgument(m_series.size() > 0, "No series defined");

        //
        // Create database on startup
        //
        if (config().getBoolean(CONF_KEY_CREATE_DATABASE_ON_STARTUP,
                DEF_CREATE_DATABASE_ON_STARTUP)) {

            String retention = config().getString(CONF_KEY_RETENTION_DURATION,
                    DEF_RETENTION_DURATION);

            logger().info("Creating database: {} with retention duration: {}",
                    m_databaseName,
                    retention);

            doQuery(
                    "CREATE DATABASE " + m_databaseName,
                    adminUser,
                    adminPassword);

            doQuery(
                    "CREATE RETENTION POLICY " + m_databaseName
                    + " ON " + m_databaseName
                    + " DURATION " + retention
                    + " REPLICATION 1",
                    adminUser,
                    adminPassword);
        }
    }

    @Override
    protected void handleEvent(final JsonObject batchEvent) {
        try {
            if (isStarted()) {

                int available = connections().getAvailableCount();
                logger().trace("Received event: {} batch-size: {} available servers: {}",
                        batchEvent.getString(EVENT_FIELD_ID),
                        batchEvent.getInteger(EVENT_FIELD_BATCH_SIZE, 0),
                        available);

                //
                // Operate on arrays only (batches)
                //
                JsonArray batch = batchEvent.getArray(EVENT_FIELD_BATCH_EVENTS, new JsonArray());
                if (!batchEvent.containsField(EVENT_FIELD_BATCH_EVENTS)) {
                    batch.addObject(batchEvent);
                }

                if (available > 0) {
                    String precision = m_precision;
                    List<String> points = new ArrayList();

                    for (int i = 0; i < batch.size(); i++) {
                        //
                        // Does event match any serie definition
                        //
                        JsonObject event = batch.get(i);
                        for (Serie serie : m_series) {
                            if (serie.isMatch(event)) {
                                String point = translateToDataPoint(serie, event, precision);
                                logger().trace("Data point: {}", point);
                                // Ignore empty points (eg. empty value fields)
                                if (point != null) {
                                    points.add(point);
                                } else {
                                    throw new RuntimeException("BOOOOOOOOOOM");
                                }
                                break; // Match found, handle next event
                            }
                        }
                    }

                    //
                    // Store events in InfluxDB
                    //
                    if (points.size() > 0) {
                        IConnection<HttpClient> connection = connections().next();
                        InfluxDbWriter handler = new InfluxDbWriter(
                                connection,
                                m_databaseName,
                                m_consistency,
                                m_precision,
                                points);
                        connection.doRequest(handler);
                    }
                }
            }
        } catch (ConnectionException e) {
            logger().error("Failed to publish event: {}",
                    batchEvent.getString(EVENT_FIELD_ID), e);
        }
    }

    private String translateToDataPoint(
            final Serie serie,
            final JsonObject event,
            final String precision) {

        //
        // Series name (mandatory)
        //
        StringBuilder point = new StringBuilder();
        {
            String name = variables().translate(event, serie.getName());
            //
            // Bad data point! No name defined/resolved
            //
            if (name.length() == 0) {
                logger().error("No data point name found: {}", event);
                return null;
            }
            // Replace special characters with "_"
            name = CharMatcher.anyOf("=, ").replaceFrom(name, '_');
            point.append(name);
        }

        //
        // Tags (mandatory) - quoted
        //
        if (serie.hasTags()) {

            String[] keys = serie.getAddTagKeys();
            String[] values = serie.getAddTagValues();

            point.append(",");
            for (int i = 0; i < keys.length; i++) {

                point.append(escapeSpecialChars(keys[i]));
                point.append("=");
                String value = String.valueOf(variables().translate(event, values[i]));
                if (value.length() > 0) {
                    point.append(escapeSpecialChars(value));
                } else {
                    // InfluxDB doesn't seem to like empty tag values
                    point.append('-');
                }

                if (i < (keys.length - 1)) {
                    point.append(",");
                }
            }
        }

        //
        // Tags (optional) - quoted
        //
        if (serie.hasOptionalTags()) {

            String[] keys = serie.getOptionalTags();

            for (String tag : keys) {
                Object rawValue = event.getValue(tag);

                if (rawValue != null) {

                    point.append(",");
                    point.append(escapeSpecialChars(tag));
                    point.append("=");

                    String value = String.valueOf(rawValue);
                    if (value.length() > 0) {
                        point.append(escapeSpecialChars(value));
                    } else {
                        // InfluxDB doesn't seem to like empty tag values
                        point.append('-');
                    }
                }
            }
        }

        //
        // Value fields (mandatory)
        //
        boolean foundValues = false;
        point.append(" ");
        List<String[]> fields = serie.getValueFields();
        for (int i = 0; i < fields.size(); i++) {

            String[] field = fields.get(i);
            String name = field[0]; // Field name
            String type = variables().translate(event, field[1]); // Data type

            point.append(escapeSpecialChars(name));
            point.append("=");

            // Convert value to string
            String value = String.valueOf(event.getValue(name));
logger().info("============================== name: {} value: {} type: {}", name, value, type);
            
            if (value.length() > 0) {

                // Consider data types
                switch (type) {
                    // Needs an "i" at the end
                    case INFLUXDB_DATA_TYPE_INTEGER:
                    case TYPE_COUNTER:
                    case TYPE_ABSOLUTE:
                        // Ensure that value is an integer
                        if (Numbers.isInteger(value)) {
                            point.append(value);
                            point.append("i");
                            foundValues = true;
                        }
logger().info("------- INTEGER found: {}", foundValues);
                        break;
                    case TYPE_GAUGE:
                        // Ensure that value is numerical
                        if (Numbers.isDecimal(value)
                                || Numbers.isInteger(value)
                                || Numbers.isFloatingPoint(value)) {
                            point.append(value.toLowerCase()); // as-is, but use lower case 'exponent constant' (float64)
                            foundValues = true;
                        }
logger().info("------- GAUGE found: {}", foundValues);
                        break;
                    // Escape double-quotes and surround with quotes (InfluxDB 0.10+)
                    case INFLUXDB_DATA_TYPE_STRING:
                    case TYPE_STRING:
                        point.append("\"");
                        point.append(escapeStringValue(value));
                        point.append("\"");
                        foundValues = true;
logger().info("------- STRING found: {}", foundValues);
                        break;
                    default:
                        point.append(value.toLowerCase()); // as-is, but use lower case 'exponent constant' (float64)
                        foundValues = true;
logger().info("------- OTHER found: {}", foundValues);
                        break;
                }
            }

            if (i < (fields.size() - 1)) {
                point.append(",");
            }
        }

        //
        // Bad data point! No values found, skip point...
        //
        if (!foundValues) {
            logger().trace("Ignoring data point: {}", event);
            return null;
        }

        //
        // Time (optional)
        //
        String timeField = serie.getTimeField();
        if (timeField.length() > 0) {

            point.append(" ");
            long tm = event.getLong(timeField);

            // Precision (convert if not same as query param)
            String dsPrecision = event.getString(EVENT_FIELD_DSTIME_PRECISION, DEF_PRECISION);
            if (!dsPrecision.equals(precision)) {
                tm = downscaleTimestamp(tm, precision, dsPrecision);
            }

            point.append(String.valueOf(tm));
        }

        return point.toString();
    }

    private String escapeSpecialChars(final String str) {
        String escaped = str;
        escaped = CharMatcher.is('\\').replaceFrom(escaped, '/'); // Backslash (must be first)
        escaped = CharMatcher.is('@').removeFrom(escaped); // Remove "at" chars (@value => value)
        escaped = CharMatcher.is(',').replaceFrom(escaped, "\\,"); // Comma
        escaped = CharMatcher.is(' ').replaceFrom(escaped, "\\ "); // Space
        escaped = CharMatcher.is('=').replaceFrom(escaped, "\\="); // Equals sign
        escaped = CharMatcher.JAVA_ISO_CONTROL.replaceFrom(escaped, "\\ "); // New line
        return escaped;
    }

    private String escapeStringValue(final String str) {
        return CharMatcher.is('"').replaceFrom(str, "\\\""); // Double-quotes
    }

    private long downscaleTimestamp(
            long tm,
            final String precision,
            final String dsPrecision) {

        switch (precision) {
            case DSTIME_PRECISION_NANOS:
                switch (dsPrecision) {
                    case DSTIME_PRECISION_MICROS:
                        tm = (tm / 1000L) * 1000L;
                        break;
                    case DSTIME_PRECISION_MILLIS:
                        tm = (tm / 1000L / 1000L) * 1000L * 1000L;
                        break;
                    case DSTIME_PRECISION_SEC:
                        tm = (tm / 1000L / 1000L / 1000L) * 1000L * 1000L * 1000L;
                        break;
                    case DSTIME_PRECISION_MIN:
                        tm = (tm / 1000L / 1000L / 1000L / 60L) * 1000L * 1000L * 1000L * 60L;
                        break;
                    case DSTIME_PRECISION_HOUR:
                        tm = (tm / 1000L / 1000L / 1000L / 60L / 60L) * 1000L * 1000L * 1000L * 60L * 60L;
                        break;
                }
                break;
            case DSTIME_PRECISION_MICROS:
                switch (dsPrecision) {
                    case DSTIME_PRECISION_MILLIS:
                        tm = (tm / 1000L) * 1000L;
                        break;
                    case DSTIME_PRECISION_SEC:
                        tm = (tm / 1000L / 1000L) * 1000L * 1000L;
                        break;
                    case DSTIME_PRECISION_MIN:
                        tm = (tm / 1000L / 1000L / 60L) * 1000L * 1000L * 60L;
                        break;
                    case DSTIME_PRECISION_HOUR:
                        tm = (tm / 1000L / 1000L / 60L / 60L) * 1000L * 1000L * 60L * 60L;
                        break;
                }
                break;
            case DSTIME_PRECISION_MILLIS:
                switch (dsPrecision) {
                    case DSTIME_PRECISION_SEC:
                        tm = (tm / 1000L) * 1000L;
                        break;
                    case DSTIME_PRECISION_MIN:
                        tm = (tm / 1000L / 60L) * 1000L * 60L;
                        break;
                    case DSTIME_PRECISION_HOUR:
                        tm = (tm / 1000L / 60L / 60L) * 1000L * 60L * 60L;
                        break;
                }
                break;
            case DSTIME_PRECISION_SEC:
                switch (dsPrecision) {
                    case DSTIME_PRECISION_MIN:
                        tm = (tm / 60L) * 60L;
                        break;
                    case DSTIME_PRECISION_HOUR:
                        tm = (tm / 60L / 60L) * 60L * 60L;
                        break;
                }
                break;
            case DSTIME_PRECISION_MIN:
                switch (dsPrecision) {
                    case DSTIME_PRECISION_HOUR:
                        tm = (tm / 60L) * 60L;
                        break;
                }
                break;
        }

        return tm;
    }

    private void doQuery(
            final String query,
            final String adminUser,
            final String adminPassword) {

        try {

            IConnection<HttpClient> connection = connections().next();
            InfluxDbQuery handler = new InfluxDbQuery(
                    connection,
                    query,
                    adminUser,
                    adminPassword);
            connection.doRequest(handler);

        } catch (ConnectionException e) {
            logger().error("Failed to create database", e);
        }
    }

    private static String base64UserAndPassword(
            final String userName,
            final String userPassword) {

        StringBuilder sb = new StringBuilder(userName);
        sb.append(":");
        sb.append(userPassword);

        return Base64.encodeBytes(sb.toString().getBytes());
    }

    private static class Serie {

        private final String m_name;
        private final String m_timeField;
        private final List<String[]> m_valueFields;
        private final String[] m_addTagKeys;
        private final String[] m_addTagValues;
        private final String[] m_optionalTags;
        private final Rule m_rule;

        private static final String CONF_KEY_NAME = "name";
        private static final String CONF_KEY_TYPE = "type";
        private static final String CONF_KEY_TIME_FIELD = "time-field";
        private static final String CONF_KEY_VALUE_FIELDS = "value-fields";
        private static final String CONF_KEY_ADD_TAGS = "add-tags";
        private static final String CONF_KEY_OPTIONAL_TAGS = "optional-tags";
        private static final String CONF_KEY_RULE = "rule";

        private static final String RULE_KEY_ACTION = "modifier";
        private static final String RULE_KEY_SCHEDULE = "schedule";

        private Serie(
                final String name,
                final String timeField,
                List<String[]> valueFields,
                final String[] addTagKeys,
                final String[] addTagValues,
                final String[] optionalTags,
                final Rule rule) {

            m_name = name;
            m_timeField = timeField;
            m_valueFields = valueFields;
            m_addTagKeys = addTagKeys;
            m_addTagValues = addTagValues;
            m_optionalTags = optionalTags;
            m_rule = rule;
        }

        public String getName() {
            return m_name;
        }

        public String getTimeField() {
            return m_timeField;
        }

        public List<String[]> getValueFields() {
            return m_valueFields;
        }

        public String[] getAddTagKeys() {
            return m_addTagKeys;
        }

        public String[] getAddTagValues() {
            return m_addTagValues;
        }

        public String[] getOptionalTags() {
            return m_optionalTags;
        }

        private boolean isMatch(final JsonObject event) {
            return m_rule.match(event);
        }

        private boolean hasTags() {
            return (m_addTagKeys.length > 0);
        }

        private boolean hasOptionalTags() {
            return (m_optionalTags.length > 0);
        }

        @Override
        public String toString() {
            StringBuilder sb = new StringBuilder("Serie: ");
            sb.append(m_name);
            sb.append(" time-field: ");
            sb.append(m_timeField);
            sb.append(" ");
            if (hasTags()) {
                sb.append("tags: ");
                for (int i = 0; i < m_addTagKeys.length; i++) {
                    String key = m_addTagKeys[i];
                    String value = m_addTagValues[i];
                    sb.append(key);
                    sb.append("=");
                    sb.append(value);
                    sb.append(" ");
                }
            }
            if (hasOptionalTags()) {
                sb.append("optional tags: ");
                for (String optionalTag : m_optionalTags) {
                    sb.append(optionalTag);
                    sb.append(" ");
                }
            }
            sb.append("fields: ");
            List<String[]> fields = m_valueFields;
            for (String[] field : fields) {
                String name = field[0]; // Field name
                String type = field[1]; // Data type
                sb.append(name);
                sb.append("=");
                sb.append(type);
                sb.append(" ");
            }
            return sb.toString();
        }

        private static Serie create(final JsonObject config) {

            String name = config.getString(CONF_KEY_NAME);
            String timeField = config.getString(CONF_KEY_TIME_FIELD, "");
            JsonArray valueFields = config.getArray(CONF_KEY_VALUE_FIELDS);
            JsonObject addTags = config.getObject(CONF_KEY_ADD_TAGS, new JsonObject());
            JsonArray optionalTags = config.getArray(CONF_KEY_OPTIONAL_TAGS, new JsonArray());

            //
            // Sanity checks
            // 
            Preconditions.checkArgument(!Strings.isNullOrEmpty(name),
                    "serie name is null or empty");
            Preconditions.checkArgument(valueFields != null && valueFields.size() > 0,
                    "serie value fields are missing");

            //
            // Value fields
            //
            List<String[]> fields = new ArrayList();
            for (int i = 0; i < valueFields.size(); i++) {
                JsonObject items = valueFields.get(i);
                String fieldName = items.getString(CONF_KEY_NAME);
                String fieldType = items.getString(CONF_KEY_TYPE, INFLUXDB_DATA_TYPE_FLOAT);
                //
                // Sanity check
                // 
                Preconditions.checkArgument(!Strings.isNullOrEmpty(fieldName),
                        "field name is null or empty");
                fields.add(new String[]{fieldName, fieldType});
            }

            // Mandatory tags
            String[] keys = addTags.getFieldNames().toArray(new String[0]);
            String[] tagKeys = new String[keys.length];
            String[] tagValues = new String[keys.length];
            for (int i = 0; i < keys.length; i++) {
                tagKeys[i] = keys[i];
                tagValues[i] = addTags.getString(keys[i], "");
            }

            // Optional tags
            String[] tagOptional = new String[optionalTags.size()];
            for (int i = 0; i < tagOptional.length; i++) {
                tagOptional[i] = optionalTags.get(i);
            }

            JsonObject ruleConfig = config.getObject(CONF_KEY_RULE, new JsonObject());
            ruleConfig.putString(RULE_KEY_ACTION, "*");
            ruleConfig.putString(RULE_KEY_SCHEDULE, "*");

            return new Serie(
                    name,
                    timeField,
                    fields,
                    tagKeys,
                    tagValues,
                    tagOptional,
                    Rule.create(name, ruleConfig)
            );
        }
    }

    private static class InfluxDbQuery extends HttpClientAdapter {

        private final String m_query;
        private final String m_adminUser;
        private final String m_adminPassword;
        private final Logger m_logger = LoggerFactory.getLogger(InfluxDbQuery.class);

        private InfluxDbQuery(
                final IConnection<HttpClient> connection,
                final String query,
                final String adminUser,
                final String adminPassword) {

            super(connection);
            m_query = query;
            m_adminUser = adminUser;
            m_adminPassword = adminPassword;
        }

        @Override
        protected void doRequest(final HttpClient client) {

            // Build query URI
            Escaper escaper = UrlEscapers.urlFormParameterEscaper();
            StringBuilder queryUri = new StringBuilder(INFLUXDB_QUERY_URI);
            queryUri.append(escaper.escape(m_query));

            m_logger.debug("Executing query: {} (host: {})",
                    queryUri, getConnection().getAddress());

            HttpClientRequest request = doGet(queryUri.toString(), new HttpClientResponseAdapter() {

                @Override
                protected void handleFailure(final HttpClientResponse response) {
                    if (response.statusCode() != 200) {
                        m_logger.error("Failed to execute query: {} (host: {})",
                                m_query,
                                getConnection().getAddress(),
                                new IllegalStateException("HTTP get failure: "
                                        + response.statusCode()
                                        + "/"
                                        + response.statusMessage()));
                    }
                }
            });

            // HTTP basic authentication
            if (!Strings.isNullOrEmpty(m_adminUser)) {
                request.putHeader(HttpHeaders.Names.AUTHORIZATION, "Basic "
                        + base64UserAndPassword(m_adminUser, m_adminPassword));
            }

            request.exceptionHandler(new DefaultConnectionExceptionHandler(
                    getConnection()));
            request.end();
        }
    }

    private static class InfluxDbWriter extends HttpClientAdapter {

        private String m_base64UserAndPassword;

        private final String m_databaseName;
        private final String m_consistency;
        private final String m_precision;
        private final List<String> m_points;
        private final Logger m_logger = LoggerFactory.getLogger(InfluxDbWriter.class);

        private InfluxDbWriter(
                final IConnection<HttpClient> connection,
                final String databaseName,
                final String consistency,
                final String precision,
                final List<String> points) {

            super(connection);
            m_databaseName = databaseName;
            m_consistency = consistency;
            m_precision = precision;
            m_points = points;

            if (connection instanceof HttpConnection) {
                m_base64UserAndPassword
                        = ((HttpConnection) connection).getBase64UserAndPassword();
            }
        }

        @Override
        protected void doRequest(final HttpClient client) {

            // Build write URI
            StringBuilder queryUri = new StringBuilder(INFLUXDB_PUBLISH_URI);
            queryUri.append(m_databaseName);
            queryUri.append("&precision=");
            queryUri.append(m_precision);
            queryUri.append("&consistency=");
            queryUri.append(m_consistency);

            HttpClientRequest request = doPost(queryUri.toString(), new HttpClientResponseAdapter() {

                @Override
                protected void handleFailure(final HttpClientResponse response) {
                    if (response.statusCode() != 200) {
                        m_logger.error("Failed to write data points to InfluxDB: {} (host: {})",
                                m_points,
                                getConnection().getAddress(),
                                new IllegalStateException("HTTP post failure: "
                                        + response.statusCode()
                                        + "/"
                                        + response.statusMessage()));
                    }
                }
            });

            StringBuilder bulk = new StringBuilder();
            for (String point : m_points) {
                bulk.append(point);
                bulk.append("\n"); // https://influxdb.com/docs/v0.9/write_protocols/write_syntax.html
            }

            byte[] body = bulk.toString().getBytes();
            request.putHeader(CONTENT_LENGTH, String.valueOf(body.length));

            // HTTP basic authentication
            if (!Strings.isNullOrEmpty(m_base64UserAndPassword)) {
                request.putHeader(HttpHeaders.Names.AUTHORIZATION, "Basic "
                        + m_base64UserAndPassword);
            }

            request.exceptionHandler(new DefaultConnectionExceptionHandler(
                    getConnection()));
            request.write(new Buffer(body));
            request.end();
        }
    }
}
