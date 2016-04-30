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
package io.spikex.filter.output;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import static io.spikex.core.helper.Events.EVENT_FIELD_BATCH_EVENTS;
import static io.spikex.core.helper.Events.EVENT_FIELD_BATCH_SIZE;
import static io.spikex.core.helper.Events.EVENT_FIELD_DSNAME;
import static io.spikex.core.helper.Events.EVENT_FIELD_ID;
import io.spikex.core.helper.Variables;
import io.spikex.core.util.connection.AsbtractHttpClient;
import io.spikex.core.util.connection.ConnectionException;
import io.spikex.core.util.connection.Connections;
import io.spikex.core.util.connection.DefaultConnectionExceptionHandler;
import io.spikex.core.util.connection.HttpClientAdapter;
import io.spikex.core.util.connection.HttpClientResponseAdapter;
import io.spikex.core.util.connection.HttpConnection;
import io.spikex.core.util.connection.IConnection;
import io.spikex.filter.internal.Rule;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.vertx.java.core.Handler;
import org.vertx.java.core.VoidHandler;
import org.vertx.java.core.buffer.Buffer;
import org.vertx.java.core.http.HttpClient;
import org.vertx.java.core.http.HttpClientRequest;
import org.vertx.java.core.http.HttpClientResponse;
import static org.vertx.java.core.http.HttpHeaders.CONTENT_LENGTH;
import static org.vertx.java.core.http.HttpHeaders.CONTENT_TYPE;
import org.vertx.java.core.json.JsonArray;
import org.vertx.java.core.json.JsonElement;
import org.vertx.java.core.json.JsonObject;

/**
 *
 * @author cli
 */
public class Ubidots extends AsbtractHttpClient {

    private String m_apiToken;
    private String m_apiPath;
    private Rule m_rule;
    private boolean m_initialized;
    private static Map<String, String> m_datasources; // datasource name -> datasource id
    private Map<String, Variable> m_variables; // variable name -> variable def
    private Map<String, Context> m_contexts; // variable name -> context def

    private static final String CONF_KEY_API_PATH = "api-path";
    private static final String CONF_KEY_API_TOKEN = "api-token";
    private static final String CONF_KEY_DATASOURCES = "datasources";
    private static final String CONF_KEY_DATASOURCE = "datasource";
    private static final String CONF_KEY_VARIABLES = "variables";
    private static final String CONF_KEY_CONTEXTS = "contexts";
    private static final String CONF_KEY_VARIABLE = "variable";
    private static final String CONF_KEY_FIELDS = "fields";
    private static final String CONF_KEY_NAME = "name";
    private static final String CONF_KEY_VALUE = "value";
    private static final String CONF_KEY_UNIT = "unit";
    private static final String CONF_KEY_RULE = "rule";
    private static final String CONF_KEY_DESCRIPTION = "description";
    private static final String CONF_KEY_TIME_FIELD = "time-field";
    private static final String CONF_KEY_VALUE_FIELD = "value-field";

    //
    // Configuration defaults
    //
    private static final String DEF_API_PATH = "/api/v1.6";
    private static final String DEF_TIME_FIELD = "@timestamp";
    private static final String DEF_VALUE_FIELD = "@value";
    private static final String DEF_DATASOURCE = "Spike.x";

    // API
    private static final String API_KEY_TOKEN = "/?token=";
    private static final String API_KEY_DATASOURCES = "/datasources";
    private static final String API_KEY_VARIABLES = "/variables";
    private static final String API_KEY_COLLECTIONS_VALUES = "/collections/values";
    private static final String API_KEY_PAGE_SIZE_DEF = "&page_size=100";
    private static final String API_KEY_ID = "id";
    private static final String API_KEY_NAME = "name";
    private static final String API_KEY_DESCRIPTION = "description";
    private static final String API_KEY_UNIT = "unit";
    private static final String API_KEY_RESULTS = "results";
    private static final String API_KEY_VARIABLE = "variable";
    private static final String API_KEY_VALUE = "value";
    private static final String API_KEY_TIMESTAMP = "timestamp";
    private static final String API_KEY_CONTEXT = "context";

    private static final String RULE_NAME = "ubidots";
    private static final String RULE_KEY_ACTION = "modifier";
    private static final String RULE_KEY_SCHEDULE = "schedule";

    private static final String VAR_KEY_DSNAME = "%{@dsname}";

    @Override
    protected void startClient() {
        m_initialized = false;
        m_apiToken = config().getString(CONF_KEY_API_TOKEN, "");
        m_apiPath = config().getString(CONF_KEY_API_PATH, DEF_API_PATH);
        //
        // Sanity check
        //
        Preconditions.checkArgument(m_apiToken.length() > 0, "Please specify an API token");
        //
        // Rule
        //
        JsonObject ruleConfig = config().getObject(CONF_KEY_RULE, new JsonObject());
        ruleConfig.putString(RULE_KEY_ACTION, "*");
        ruleConfig.putString(RULE_KEY_SCHEDULE, "*");
        m_rule = Rule.create(RULE_NAME, ruleConfig);
        //
        // Variables
        //
        m_variables = new HashMap();
        JsonArray variables = config().getArray(CONF_KEY_VARIABLES, new JsonArray());
        for (int i = 0; i < variables.size(); i++) {
            JsonObject variable = variables.get(i);
            String name = variable.getString(CONF_KEY_NAME, "");
            //
            // Sanity check
            //
            Preconditions.checkArgument(name.length() > 0, "Variable is missing name");
            m_variables.put(name, Variable.create(
                    name,
                    variable.getString(CONF_KEY_DESCRIPTION, ""),
                    variable.getString(CONF_KEY_UNIT, ""),
                    variable.getString(CONF_KEY_TIME_FIELD, DEF_TIME_FIELD),
                    variable.getString(CONF_KEY_VALUE_FIELD, DEF_VALUE_FIELD),
                    variable.getString(CONF_KEY_DATASOURCE, DEF_DATASOURCE)));
        }
        //
        // Contexts
        //
        m_contexts = new HashMap();
        JsonArray contexts = config().getArray(CONF_KEY_CONTEXTS, new JsonArray());
        for (int i = 0; i < contexts.size(); i++) {

            JsonObject context = contexts.get(i);
            String variable = context.getString(CONF_KEY_VARIABLE);
            Context ctx = new Context();

            JsonArray fields = context.getArray(CONF_KEY_FIELDS, new JsonArray());
            for (int j = 0; j < fields.size(); j++) {
                JsonObject field = fields.get(j);
                ctx.addField(
                        field.getString(CONF_KEY_NAME),
                        field.getString(CONF_KEY_VALUE));
            }
            m_contexts.put(variable, ctx);
        }
        //
        // Initialize datasources
        //
        m_datasources = new HashMap();
        initDatasources(config().getArray(CONF_KEY_DATASOURCES, new JsonArray()));
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

                if (available > 0 && m_initialized) {
                    //
                    // Operate on arrays only (batches)
                    //
                    JsonArray batch = batchEvent.getArray(EVENT_FIELD_BATCH_EVENTS, new JsonArray());
                    if (!batchEvent.containsField(EVENT_FIELD_BATCH_EVENTS)) {
                        batch.addObject(batchEvent);
                    }

                    List<Variable> updatedVariables = new ArrayList();
                    for (int i = 0; i < batch.size(); i++) {

                        JsonObject event = batch.get(i);
                        if (m_rule.match(event)) {

                            String dsname = event.getString(EVENT_FIELD_DSNAME);
                            Variable variable = m_variables.get(dsname);

                            if (variable == null) {
                                //
                                // Get special variable if defined (template)
                                // Instantiate new variable based on template
                                //
                                variable = Variable.copy(
                                        dsname,
                                        m_variables.get(VAR_KEY_DSNAME),
                                        event,
                                        variables());
                            }

                            // Update variable value and timestamp
                            String timeField = variable.getTimeField();
                            String valueField = variable.getValueField();
                            variable.setValue(
                                    event.getValue(valueField),
                                    event.getLong(timeField, System.currentTimeMillis()));

                            // Create variable in Ubidots (if needed)
                            if (!variable.isCreated()
                                    && !m_variables.containsKey(dsname)) {
                                initVariable(variable);
                            }
                            m_variables.put(dsname, variable);

                            // Mark as updateable
                            if (variable.isCreated()) {
                                variable.updateContext(
                                        m_contexts,
                                        m_variables);
                                updatedVariables.add(variable);
                            }
                        }
                    }
                    //
                    // Post variable values to Ubidots (also consider context)
                    //
                    if (updatedVariables.size() > 0) {
                        JsonArray data = new JsonArray();
                        for (Variable var : updatedVariables) {
                            logger().debug("Posting variable: {} ({}) value: {} unit: {}",
                                    var.getName(), var.getId(), var.getValue(), var.getUnit());
                            data.add(var.toJson());
                        }
                        String query = m_apiPath + API_KEY_COLLECTIONS_VALUES;
                        doPost(
                                query,
                                m_apiToken,
                                data,
                                new UbidotsPostResponseHandler(query, true) {

                                    @Override
                                    protected void handleResponse(final JsonElement data) {
                                        logger().trace("Response: {}", ((JsonArray) data).encode());
                                    }
                                });
                    }
                }
            }
        } catch (Exception e) {
            logger().error("Failed to publish event: {}",
                    batchEvent.getString(EVENT_FIELD_ID), e);
        }
    }

    private void initDatasources(final JsonArray datasources) {

        for (int i = 0; i < datasources.size(); i++) {

            JsonObject datasource = datasources.get(i);
            String name = datasource.getString(CONF_KEY_NAME);
            String desc = datasource.getString(CONF_KEY_DESCRIPTION, "");

            if (!Strings.isNullOrEmpty(name)) {

                // Create datasource if missing
                JsonObject data = new JsonObject();
                data.putString(API_KEY_NAME, name);
                data.putString(API_KEY_DESCRIPTION, desc);
                final String query = m_apiPath + API_KEY_DATASOURCES;

                doQuery(
                        query,
                        m_apiToken,
                        new UbidotsQueryResponseHandler(
                                connections(),
                                new UbidotsPostResponseHandler(query) {

                                    @Override
                                    protected void handleResponse(final JsonElement data) {
                                        String name = ((JsonObject) data).getString(API_KEY_NAME);
                                        String id = ((JsonObject) data).getString(API_KEY_ID);
                                        logger().info("Found or created datasource: {} ({})", name, id);
                                        m_datasources.put(name, id);
                                        //
                                        // Initialize variables related to datasource
                                        //
                                        initVariables(name, id);
                                    }
                                },
                                query,
                                m_apiToken,
                                data));
            } else {
                logger().error("Ignoring datasource with no name");
            }
        }
    }

    private void initVariables(
            final String datasource,
            final String dsId) {

        final String query = m_apiPath + API_KEY_DATASOURCES + "/" + dsId + API_KEY_VARIABLES;
        doQuery(query,
                m_apiToken + API_KEY_PAGE_SIZE_DEF,
                new UbidotsQueryResponseHandler(
                        connections(),
                        new UbidotsPostResponseHandler(query) {

                            @Override
                            protected void handleResponse(JsonElement data) {

                                String name = ((JsonObject) data).getString(API_KEY_NAME);
                                String id = ((JsonObject) data).getString(API_KEY_ID);
                                String unit = ((JsonObject) data).getString(API_KEY_UNIT);
                                logger().info("Found variable: {} unit: {} ({})", name, unit, id);
                                Variable var = Variable.create(
                                        name,
                                        "",
                                        unit,
                                        DEF_TIME_FIELD,
                                        DEF_VALUE_FIELD,
                                        datasource);
                                var.setId(id);
                                m_variables.put(name, var);
                            }

                            @Override
                            protected void responseDone() {
                                m_initialized = true;
                            }
                        },
                        null,
                        null,
                        null));
    }

    private void initVariable(final Variable variable) {

        String name = variable.getName();
        String dsId = m_datasources.get(variable.getDatasource());

        if (!Strings.isNullOrEmpty(dsId)) {

            // Create variable if missing
            JsonObject data = new JsonObject();
            data.putString(API_KEY_NAME, name);
            data.putString(API_KEY_DESCRIPTION, variable.getDescription());
            data.putString(API_KEY_UNIT, variable.getUnit());
            final String query = m_apiPath + API_KEY_DATASOURCES + "/" + dsId + API_KEY_VARIABLES;

            doPost(query,
                    m_apiToken,
                    data,
                    new UbidotsPostResponseHandler(query) {

                        @Override
                        protected void handleResponse(final JsonElement data) {
                            String name = ((JsonObject) data).getString(API_KEY_NAME);
                            String id = ((JsonObject) data).getString(API_KEY_ID);
                            logger().info("Created variable: {} ({})", name, id);
                            variable.setId(id);
                            m_variables.put(name, variable);
                        }
                    });
        } else {
            logger().warn("Ignoring variable without datasource: {}", name);
        }
    }

    private void doQuery(
            final String query,
            final String token,
            final Handler<HttpClientResponse> handler) {

        try {

            IConnection<HttpClient> connection = connections().next();
            UbidotsQuery queryHandler = new UbidotsQuery(
                    connection,
                    query,
                    token,
                    handler);
            connection.doRequest(queryHandler);

        } catch (ConnectionException e) {
            logger().error("Failed to query ubidots service", e);
        }
    }

    private void doPost(
            final String query,
            final String token,
            final JsonElement data,
            final UbidotsPostResponseHandler handler) {

        try {

            IConnection<HttpClient> connection = connections().next();
            UbidotsPost postHandler = new UbidotsPost(
                    connection,
                    handler,
                    query,
                    token,
                    data);
            connection.doRequest(postHandler);

        } catch (ConnectionException e) {
            logger().error("Failed to create datasource or variable", e);
        }
    }

    private static class UbidotsQuery extends HttpClientAdapter {

        private final String m_query;
        private final String m_token;
        private final Handler<HttpClientResponse> m_handler;
        private final Logger m_logger = LoggerFactory.getLogger(UbidotsQuery.class);

        private UbidotsQuery(
                final IConnection<HttpClient> connection,
                final String query,
                final String token,
                final Handler<HttpClientResponse> handler) {

            super(connection);
            m_query = query;
            m_token = token;
            m_handler = handler;
        }

        @Override
        protected void doRequest(final HttpClient client) {

            // Build query URI
            StringBuilder queryUri = new StringBuilder(m_query);
            queryUri.append(API_KEY_TOKEN);
            queryUri.append(m_token);

            m_logger.trace("Executing query: {} (host: {})",
                    queryUri, getConnection().getAddress());

            HttpClientRequest request = doGet(queryUri.toString(), m_handler);
            request.exceptionHandler(new DefaultConnectionExceptionHandler(
                    getConnection()));
            request.end();
        }
    }

    private static class UbidotsPost extends HttpClientAdapter {

        private final String m_query;
        private final String m_token;
        private final JsonElement m_data;
        private final HttpClientResponseAdapter m_responseHandler;
        private final Logger m_logger = LoggerFactory.getLogger(UbidotsQuery.class);

        private UbidotsPost(
                final IConnection<HttpClient> connection,
                final HttpClientResponseAdapter responseHandler,
                final String query,
                final String token,
                final JsonElement data) {

            super(connection);
            m_responseHandler = responseHandler;
            m_query = query;
            m_token = token;
            m_data = data;
        }

        @Override
        protected void doRequest(final HttpClient client) {

            // Build post URI
            StringBuilder postUri = new StringBuilder(m_query);
            postUri.append(API_KEY_TOKEN);
            postUri.append(m_token);

            m_logger.trace("Executing post: {} (host: {})",
                    postUri, getConnection().getAddress());

            HttpClientRequest request = doPost(postUri.toString(), m_responseHandler);
            m_logger.trace("Posting data: {}", m_data);
            byte[] body;
            if (m_data.isObject()) {
                body = ((JsonObject) m_data).encode().getBytes();
            } else {
                body = ((JsonArray) m_data).encode().getBytes();
            }
            request.putHeader(CONTENT_TYPE, "application/json");
            request.putHeader(CONTENT_LENGTH, String.valueOf(body.length));
            request.exceptionHandler(new DefaultConnectionExceptionHandler(
                    getConnection()));
            request.write(new Buffer(body));
            request.end();
        }
    }

    private static class UbidotsResponseHandler extends HttpClientResponseAdapter {

        private final Connections<HttpConnection> m_conns;
        private final String m_query;
        private final String m_token;

        private final Logger m_logger = LoggerFactory.getLogger(getClass());

        private UbidotsResponseHandler(final UbidotsResponseHandler parent) {
            m_conns = parent.m_conns;
            m_query = parent.m_query;
            m_token = parent.m_token;
        }

        private UbidotsResponseHandler(
                final Connections<HttpConnection> conns,
                final String query,
                final String token) {

            m_conns = conns;
            m_query = query;
            m_token = token;
        }

        protected Connections<HttpConnection> connections() {
            return m_conns;
        }

        protected String query() {
            return m_query;
        }

        protected String token() {
            return m_token;
        }

        protected Logger logger() {
            return m_logger;
        }

        @Override
        protected void handleFailure(final HttpClientResponse response) {
            if (response.statusCode() != 200) {
                m_logger.error("Failed to execute query: {}",
                        m_query,
                        new IllegalStateException("HTTP get failure: "
                                + response.statusCode()
                                + "/"
                                + response.statusMessage()));
            }
        }
    }

    private static class UbidotsQueryResponseHandler extends UbidotsResponseHandler {

        private final UbidotsPostResponseHandler m_resultHandler;
        private final JsonObject m_data;

        private UbidotsQueryResponseHandler(
                final Connections<HttpConnection> conns,
                final UbidotsPostResponseHandler resultHandler,
                final String query,
                final String token,
                final JsonObject data) {

            super(conns,
                    query,
                    token);

            m_resultHandler = resultHandler;
            m_data = data;
        }

        @Override
        protected void handleSuccess(final HttpClientResponse response) {

            final Buffer body = new Buffer(0);

            response.dataHandler(new Handler<Buffer>() {
                @Override
                public void handle(final Buffer data) {
                    body.appendBuffer(data);
                }
            });

            response.endHandler(new VoidHandler() {
                @Override
                public void handle() {
                    //
                    // Parse query response
                    //
                    String responseData = new String(body.getBytes(), StandardCharsets.UTF_8);
                    logger().trace("Response: {}", responseData);
                    JsonObject jsonData = new JsonObject(responseData);
                    JsonArray items = jsonData.getArray(API_KEY_RESULTS, new JsonArray());
                    boolean found = false;
                    for (int i = 0; i < items.size(); i++) {
                        JsonObject item = items.get(i);
                        //
                        // Handle existing item (datasource, variable, etc..)
                        //
                        m_resultHandler.handleResponse(item);
                        found = true;
                    }
                    m_resultHandler.responseDone();
                    //
                    // Create new item (datasource, variable, etc..)
                    //
                    if (!found && m_data != null) {
                        try {
                            logger().debug("Creating: {}", m_data.encode());
                            IConnection<HttpClient> connection = connections().next();
                            UbidotsPost postHandler = new UbidotsPost(
                                    connection,
                                    m_resultHandler,
                                    query(),
                                    token(),
                                    m_data);
                            connection.doRequest(postHandler);

                        } catch (ConnectionException e) {
                            logger().error("Failed to create datasource or variable", e);
                        }
                    }
                }
            });
        }
    }

    private static abstract class UbidotsPostResponseHandler extends HttpClientResponseAdapter {

        private final boolean m_responseIsArray;
        private final String m_query;
        private final Logger m_logger = LoggerFactory.getLogger(UbidotsPostResponseHandler.class);

        private UbidotsPostResponseHandler(final String query) {
            this(query, false);
        }

        private UbidotsPostResponseHandler(
                final String query,
                final boolean responseIsArray) {

            m_query = query;
            m_responseIsArray = responseIsArray;
        }

        protected Logger logger() {
            return m_logger;
        }

        @Override
        protected void handleSuccess(final HttpClientResponse response) {

            final Buffer body = new Buffer(0);

            response.dataHandler(new Handler<Buffer>() {
                @Override
                public void handle(final Buffer data) {
                    body.appendBuffer(data);
                }
            });

            response.endHandler(new VoidHandler() {
                @Override
                public void handle() {
                    //
                    // Parse query response
                    //
                    String responseData = new String(body.getBytes(), StandardCharsets.UTF_8);
                    logger().trace("Response: {}", responseData);
                    if (m_responseIsArray) {
                        JsonArray jsonData = new JsonArray(responseData);
                        handleResponse(jsonData);
                    } else {
                        JsonObject jsonData = new JsonObject(responseData);
                        handleResponse(jsonData);
                    }
                }
            });
        }

        protected abstract void handleResponse(final JsonElement data);

        protected void responseDone() {
            // Do nothing by default...
        }

        @Override
        protected void handleFailure(final HttpClientResponse response) {
            if (response.statusCode() != 200) {
                m_logger.error("Failed to execute query: {}",
                        m_query,
                        new IllegalStateException("HTTP get failure: "
                                + response.statusCode()
                                + "/"
                                + response.statusMessage()));
            }
        }
    }

    private static class Variable {

        private final String m_name;
        private final String m_description;
        private final String m_unit;
        private final String m_timeField;
        private final String m_valueField;
        private final String m_datasource;

        private String m_id;
        private Object m_value;
        private long m_timestamp;
        private JsonObject m_context;

        private static final String FIELD_KEY_VAR = "%{var:";
        private final Logger m_logger = LoggerFactory.getLogger(Variable.class);

        private Variable(
                final String name,
                final String description,
                final String unit,
                final String timeField,
                final String valueField,
                final String datasource) {

            m_name = name;
            m_description = description;
            m_unit = unit;
            m_timeField = timeField;
            m_valueField = valueField;
            m_datasource = datasource;
        }

        private boolean isCreated() {
            return (m_id != null);
        }

        private String getId() {
            return m_id;
        }

        private String getName() {
            return m_name;
        }

        private String getDescription() {
            return m_description;
        }

        private String getUnit() {
            return m_unit;
        }

        private String getTimeField() {
            return m_timeField;
        }

        private String getValueField() {
            return m_valueField;
        }

        private String getDatasource() {
            return m_datasource;
        }

        private Object getValue() {
            return m_value;
        }

        private long getTimestamp() {
            return m_timestamp;
        }

        private void setId(final String id) {
            m_id = id;
        }

        private void setValue(
                final Object value,
                final long timestamp) {

            m_value = value;
            m_timestamp = timestamp;
        }

        private void updateContext(
                final Map<String, Context> contexts,
                final Map<String, Variable> variables) {

            Context ctx = contexts.get(getName());
            if (ctx != null) {
                m_context = new JsonObject();
                for (Field field : ctx.getFields()) {
                    String name = field.getName();
                    Object value = field.getValue();

                    // Lookup value from another variable?
                    if (value != null
                            && (value instanceof String)
                            && ((String) value).startsWith(FIELD_KEY_VAR)) {

                        String lookupDef = (String) value;
                        int len = lookupDef.length();
                        String variable = lookupDef.substring(FIELD_KEY_VAR.length(), len - 1);
                        Variable var = variables.get(variable);

                        value = var.getValue();
                        if (value == null) {
                            value = "";
                        }
                        m_logger.trace("Looked up \"{}\" value: {}", var, value);
                    }

                    m_context.putValue(name, value);
                }
            }
        }

        private JsonObject toJson() {
            JsonObject json = new JsonObject();
            json.putString(API_KEY_VARIABLE, getId()); // VAR_ID
            json.putValue(API_KEY_VALUE, getValue());
            json.putNumber(API_KEY_TIMESTAMP, getTimestamp());

            // Context
            if (m_context != null) {
                json.putObject(API_KEY_CONTEXT, m_context);
            }

            return json;
        }

        private static Variable create(
                final String name,
                final String description,
                final String unit,
                final String timeField,
                final String valueField,
                final String datasource) {

            return new Variable(
                    name,
                    description,
                    unit,
                    timeField,
                    valueField,
                    datasource);
        }

        private static Variable copy(
                final String dsname,
                final Variable tmpl,
                final JsonObject event,
                final Variables vars) {

            Variable var;

            if (tmpl != null) {
                String unit = vars.translate(event, tmpl.getUnit());
                var = Variable.create(
                        dsname,
                        tmpl.getDescription(),
                        unit,
                        tmpl.getTimeField(),
                        tmpl.getValueField(),
                        tmpl.getDatasource());
            } else {
                // Create variable on the fly (no definition found)
                var = Variable.create(
                        dsname,
                        "",
                        "",
                        DEF_TIME_FIELD,
                        DEF_VALUE_FIELD,
                        DEF_DATASOURCE);
            }
            return var;
        }
    }

    private static class Context {

        private final List<Field> m_fields;

        private Context() {
            m_fields = new ArrayList();
        }

        private List<Field> getFields() {
            return m_fields;
        }

        private void addField(
                final String name,
                final String value) {

            m_fields.add(Field.create(name, value));
        }

    }

    private static class Field {

        private final String m_name;
        private final String m_value;

        private Field(
                final String name,
                final String value) {

            m_name = name;
            m_value = value;
        }

        private String getName() {
            return m_name;
        }

        private String getValue() {
            return m_value;
        }

        private static Field create(
                final String name,
                final String value) {

            return new Field(name, value);
        }
    }
}
