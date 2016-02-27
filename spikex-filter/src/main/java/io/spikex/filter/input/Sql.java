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
package io.spikex.filter.input;

import com.google.common.base.CharMatcher;
import com.google.common.base.Splitter;
import com.google.common.base.Strings;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import io.spikex.core.AbstractFilter;
import io.spikex.core.helper.Events;
import static io.spikex.core.helper.Events.DSTIME_PRECISION_MIN;
import static io.spikex.core.helper.Events.TIMEZONE_UTC;
import io.spikex.filter.internal.Modifier;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.sql2o.Connection;
import org.sql2o.Sql2o;
import org.sql2o.data.Column;
import org.sql2o.data.Row;
import org.sql2o.data.Table;
import org.sql2o.quirks.Db2Quirks;
import org.sql2o.quirks.NoQuirks;
import org.sql2o.quirks.OracleQuirks;
import org.sql2o.quirks.PostgresQuirks;
import org.sql2o.quirks.Quirks;
import org.vertx.java.core.json.JsonArray;
import org.vertx.java.core.json.JsonObject;

/**
 *
 * @author cli
 */
public final class Sql extends AbstractFilter {

    private Sql2o m_sql2o;

    private final Map<String, Modifier> m_actions; // action-id => action

    private static final String DEFAULT_MODIFIER = "*"; // Always performed (not rule based)

    private static final String CONF_KEY_JDBC_URL = "jdbc-url";
    private static final String CONF_KEY_JDBC_DRIVER_CLASS = "jdbc-driver-class";
    private static final String CONF_KEY_USER_NAME = "user-name";
    private static final String CONF_KEY_USER_PASSWORD = "user-password";
    private static final String CONF_KEY_HIKARI_CP = "hikari-cp";
    private static final String CONF_KEY_CONN_INIT_SQL = "connection-init-sql";
    private static final String CONF_KEY_CONN_TEST_SQL = "connection-test-sql";
    private static final String CONF_KEY_CONN_TIMEOUT = "connection-timeout";
    private static final String CONF_KEY_IDLE_TIMEOUT = "idle-timeout";
    private static final String CONF_KEY_INIT_FAIL_FAST = "initialization-fail-fast";
    private static final String CONF_KEY_MAX_LIFETIME = "max-lifetime";
    private static final String CONF_KEY_MAX_POOL_SIZE = "max-pool-size";
    private static final String CONF_KEY_MIN_IDLE = "min-idle";
    private static final String CONF_KEY_DATASOURCE_PROPS = "datasource-properties";
    private static final String CONF_KEY_BOOTSTRAP_FILE = "bootstrap-file";
    private static final String CONF_KEY_BOOTSTRAP_IGNORE_ERRORS = "bootstrap-ignore-errors";
    private static final String CONF_KEY_QUIRKS = "quirks";
    private static final String CONF_KEY_QUERIES = "queries";
    private static final String CONF_KEY_NAME = "name";
    private static final String CONF_KEY_TIMESTAMP_PRECISION = "timestamp-precision";
    private static final String CONF_KEY_SQL = "sql";

    private static final long DEF_CONN_TIMEOUT = TimeUnit.SECONDS.toMillis(30); // 30 sec
    private static final long DEF_IDLE_TIMEOUT = TimeUnit.MINUTES.toMillis(10); // 10 min
    private static final long DEF_MAX_LIFETIME = TimeUnit.MINUTES.toMillis(30); // 30 min
    private static final int DEF_MAX_POOL_SIZE = 1;
    private static final int DEF_MIN_IDLE = 1;
    private static final boolean DEF_INIT_FAIL_FAST = true;
    private static final boolean DEF_BOOTSTRAP_IGNORE_ERRORS = false;
    private static final String DEF_TIMESTAMP_PRECISION = DSTIME_PRECISION_MIN; // Minutes

    private static final String COLUMN_HOST = "host";
    private static final String COLUMN_VALUE = "value";
    private static final String COLUMN_DSNAME = "dsname";
    private static final String COLUMN_DSTYPE = "dstype";
    private static final String COLUMN_DSPRECISION = "dsprecision";
    private static final String COLUMN_TIMESTAMP = "timestamp";
    private static final String COLUMN_TIMEZONE = "timezone";
    private static final String COLUMN_INSTANCE = "instance";
    private static final String COLUMN_SUBGROUP = "subgroup";

    // Db2Quirks, NoQuirks, OracleQuirks, PostgresQuirks
    private static final String QUIRKS_NONE = "none"; // Default
    private static final String QUIRKS_ORACLE = "oracle";
    private static final String QUIRKS_DB2 = "db2";
    private static final String QUIRKS_POSTGRES = "postgres";

    public Sql() {
        m_actions = new HashMap();
    }

    @Override
    protected void startFilter() {
        //
        // Non-rules based modifiers
        //
        m_actions.clear();
        Modifier defModifier = Modifier.create(DEFAULT_MODIFIER, variables(), config());
        if (!defModifier.isEmpty()) {
            m_actions.put(DEFAULT_MODIFIER, defModifier);
        }
        //
        // Load JDBC driver
        //
        if (config().containsField(CONF_KEY_JDBC_DRIVER_CLASS)) {
            try {
                Class.forName(config().getString(CONF_KEY_JDBC_DRIVER_CLASS, ""));
            } catch (ClassNotFoundException e) {
                throw new IllegalStateException("Failed to load JDBC driver", e);
            }
        }

        //
        // HikariCP connection pool
        //
        HikariDataSource dataSource = null;
        if (config().containsField(CONF_KEY_HIKARI_CP)) {
            JsonObject hikariJson = config().getObject(CONF_KEY_HIKARI_CP);

            HikariConfig hkriConfig = new HikariConfig();
            hkriConfig.setConnectionInitSql(hikariJson.getString(CONF_KEY_CONN_INIT_SQL, ""));
            hkriConfig.setConnectionTestQuery(hikariJson.getString(CONF_KEY_CONN_TEST_SQL, ""));
            hkriConfig.setConnectionTimeout(hikariJson.getLong(CONF_KEY_CONN_TIMEOUT, DEF_CONN_TIMEOUT));
            hkriConfig.setIdleTimeout(hikariJson.getLong(CONF_KEY_IDLE_TIMEOUT, DEF_IDLE_TIMEOUT));
            hkriConfig.setInitializationFailFast(hikariJson.getBoolean(CONF_KEY_INIT_FAIL_FAST, DEF_INIT_FAIL_FAST));
            hkriConfig.setMaxLifetime(hikariJson.getLong(CONF_KEY_MAX_LIFETIME, DEF_MAX_LIFETIME));
            hkriConfig.setMaximumPoolSize(hikariJson.getInteger(CONF_KEY_MAX_POOL_SIZE, DEF_MAX_POOL_SIZE));
            hkriConfig.setMinimumIdle(hikariJson.getInteger(CONF_KEY_MIN_IDLE, DEF_MIN_IDLE));

            if (config().containsField(CONF_KEY_JDBC_DRIVER_CLASS)) {
                hkriConfig.setDriverClassName(config().getString(CONF_KEY_JDBC_DRIVER_CLASS));
            }
            hkriConfig.setJdbcUrl(config().getString(CONF_KEY_JDBC_URL));
            hkriConfig.setUsername(config().getString(CONF_KEY_USER_NAME));
            hkriConfig.setPassword(config().getString(CONF_KEY_USER_PASSWORD));
            hkriConfig.setReadOnly(true); // Input filter

            // Driver specific properties
            if (hikariJson.containsField(CONF_KEY_DATASOURCE_PROPS)) {
                JsonObject dsPropsJson = hikariJson.getObject(CONF_KEY_DATASOURCE_PROPS);
                for (String key : dsPropsJson.getFieldNames()) {
                    Object value = dsPropsJson.getValue(key);
                    hkriConfig.addDataSourceProperty(key, value);
                }
            }

            dataSource = new HikariDataSource(hkriConfig);
        }

        //
        // Sql2o and bootstrapping
        //
        Quirks quirks;
        switch (config().getString(CONF_KEY_QUIRKS, QUIRKS_NONE)) {
            case QUIRKS_ORACLE:
                quirks = new OracleQuirks();
                break;
            case QUIRKS_DB2:
                quirks = new Db2Quirks();
                break;
            case QUIRKS_POSTGRES:
                quirks = new PostgresQuirks();
                break;
            default:
                quirks = new NoQuirks();
                break;
        }

        if (dataSource != null) {
            m_sql2o = new Sql2o(dataSource, quirks);
        } else {
            m_sql2o = new Sql2o(
                    config().getString(CONF_KEY_JDBC_URL),
                    config().getString(CONF_KEY_USER_NAME),
                    config().getString(CONF_KEY_USER_PASSWORD),
                    quirks);
        }

        String bootstrapFile = config().getString(CONF_KEY_BOOTSTRAP_FILE);
        boolean bootstrapIgnoreErrors
                = config().getBoolean(CONF_KEY_BOOTSTRAP_IGNORE_ERRORS,
                        DEF_BOOTSTRAP_IGNORE_ERRORS);

        if (!Strings.isNullOrEmpty(bootstrapFile)) {
            performBootsrap(
                    m_sql2o,
                    (String) variables().translate(bootstrapFile),
                    bootstrapIgnoreErrors);
        }
    }

    @Override
    protected void handleTimerEvent() {

        JsonArray queries = config().getArray(CONF_KEY_QUERIES, new JsonArray());
        try (Connection con = m_sql2o.open()) {
            for (int i = 0; i < queries.size(); i++) {

                JsonObject query = queries.get(i);
                String sql = query.getString(CONF_KEY_SQL);
                String name = query.getString(CONF_KEY_NAME, sql);
                String precision = query.getString(CONF_KEY_TIMESTAMP_PRECISION,
                        DEF_TIMESTAMP_PRECISION);

                try {
                    Table result = con.createQuery(sql, name).executeAndFetchTable();
                    emitSqlEvents(result, precision);

                } catch (Exception e) {
                    logger().error("Failed to execute {}: {}", name, sql, e);
                }
            }
            con.close();

        } catch (Exception e) {
            logger().error("Failed to open connection", e);
        }
    }

    private void emitSqlEvents(
            final Table result,
            final String precision) {

        //
        // Resolve existence of optional columns
        //
        boolean hasHost = false;
        boolean hasTimestamp = false;
        boolean hasPrecision = false;
        boolean hasTimezone = false;
        boolean hasInstance = false;
        boolean hasSubgroup = false;

        // Extra fields
        List<String> fields = new ArrayList();

        for (Column col : result.columns()) {
            if (!Strings.isNullOrEmpty(col.getName())) {
                String name = col.getName().toLowerCase();
                switch (name) {
                    case COLUMN_HOST:
                        hasHost = true;
                        break;
                    case COLUMN_TIMESTAMP:
                        hasTimestamp = true;
                        break;
                    case COLUMN_DSPRECISION:
                        hasPrecision = true;
                        break;
                    case COLUMN_TIMEZONE:
                        hasTimezone = true;
                        break;
                    case COLUMN_INSTANCE:
                        hasInstance = true;
                        break;
                    case COLUMN_SUBGROUP:
                        hasSubgroup = true;
                        break;
                    default:
                        if (!COLUMN_VALUE.equals(name)
                                && !COLUMN_DSNAME.equals(name)
                                && !COLUMN_DSTYPE.equals(name)) {

                            logger().trace("Found extra field: {}", name);
                            fields.add(name);
                        }
                        break;
                }
            }
        }

        for (Row row : result.rows()) {
            //
            // Mandatory fields
            //
            Object value = row.getObject(COLUMN_VALUE);
            String dsname = row.getString(COLUMN_DSNAME);
            String dstype = row.getString(COLUMN_DSTYPE);
            //
            // Optional fields
            //
            String host = "-";
            if (hasHost) {
                host = row.getString(COLUMN_HOST);
            }
            long timestamp = System.currentTimeMillis();
            if (hasTimestamp) {
                Object tm = row.getObject(COLUMN_TIMESTAMP);
                if (tm instanceof java.sql.Timestamp) {
                    timestamp = ((java.sql.Timestamp) tm).getTime();
                } else {
                    timestamp = (long) tm;
                }
            }
            String dsPrecision = precision;
            if (hasPrecision) {
                dsPrecision = row.getString(COLUMN_DSPRECISION);
            }
            String timezone = TIMEZONE_UTC.getId();
            if (hasTimezone) {
                timezone = row.getString(COLUMN_TIMEZONE);
            }
            String instance = "-";
            if (hasInstance) {
                instance = row.getString(COLUMN_INSTANCE);
            }
            String subgroup = "-";
            if (hasSubgroup) {
                subgroup = row.getString(COLUMN_SUBGROUP);
            }
            //
            // Create new event per value
            //
            JsonObject event = Events.createMetricEvent(
                    this,
                    timestamp,
                    ZoneId.of(timezone),
                    host,
                    dsname.toLowerCase(),
                    dstype.toUpperCase(),
                    dsPrecision.toLowerCase(),
                    subgroup.toLowerCase(),
                    instance,
                    updateInterval(),
                    value);
            //
            // Add extra fields (if any)
            //
            for (String field : fields) {
                Object extraValue = row.getObject(field);
                if (extraValue != null) {
                    event.putValue(field, extraValue);
                }
            }
            //
            // Default modifier
            //
            Map<String, Modifier> actions = m_actions;
            Modifier defModifier = actions.get(DEFAULT_MODIFIER);
            if (defModifier != null) {
                logger().trace("Applying default modifier: {}", defModifier);
                defModifier.handle(event);
            }
            //
            // Forward event
            //
            emitEvent(event);
        }
    }

    private void performBootsrap(
            final Sql2o sql2o,
            final String filename,
            final boolean ignoreErrors) {
        //
        // Read file line-by-line and do an executeUpdate for each line
        //
        try {
            //
            // Read all SQL lines to execute
            //
            String lines = new String(Files.readAllBytes(Paths.get(filename)));
            //
            // Split SQL on semicolon
            //
            Iterable<String> clauses = Splitter.on(";")
                    .omitEmptyStrings()
                    .trimResults(CharMatcher.BREAKING_WHITESPACE)
                    .split(lines);
            //
            // Execute each SQL and commit
            //
            try (Connection con = sql2o.beginTransaction()) {
                try {
                    for (String sql : clauses) {
                        logger().trace("Bootstrap SQL: {}", sql);
                        con.createQuery(sql, sql).executeUpdate();
                    }
                } catch (Exception e) {
                    if (ignoreErrors) {
                        logger().warn("Failed to execute bootstrap SQL", e);
                    } else {
                        throw e;
                    }
                }
                con.commit();
            }
        } catch (IOException e) {
            throw new IllegalStateException("Failed to execute bootstrap file", e);
        }
    }
}
