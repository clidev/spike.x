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
import io.spikex.core.AbstractFilter;
import io.spikex.core.helper.Events;
import static io.spikex.core.helper.Events.DSTIME_PRECISION_SEC;
import static io.spikex.core.helper.Events.TIMEZONE_UTC;
import io.spikex.filter.internal.Modifier;
import java.net.URI;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;
import org.jolokia.client.BasicAuthenticator;
import org.jolokia.client.J4pClient;
import org.jolokia.client.exception.J4pException;
import org.jolokia.client.request.J4pListRequest;
import org.jolokia.client.request.J4pReadRequest;
import org.jolokia.client.request.J4pResponse;
import org.json.simple.JSONObject;
import org.vertx.java.core.json.JsonArray;
import org.vertx.java.core.json.JsonObject;

/**
 *
 * @author cli
 */
public final class Jolokia extends AbstractFilter {

    private RequestConfig m_config;
    private boolean m_fetchJavaLangMbeanMetadata; // JBoss 4.x / 5.x bug circumvention

    private final List<J4pClient> m_clients;
    private final Map<String, Modifier> m_actions; // action-id => action

    private static final String DEFAULT_MODIFIER = "*"; // Always performed (not rule based)

    private static final String CONF_KEY_JOLOKIA_URLS = "jolokia-urls";
    private static final String CONF_KEY_USER_NAME = "user-name";
    private static final String CONF_KEY_USER_PASSWORD = "user-password";
    private static final String CONF_KEY_AUTH_PREEMPTIVE = "auth-preemptive";
    private static final String CONF_KEY_REQUESTS = "requests";
    private static final String CONF_KEY_MBEAN = "mbean";
    private static final String CONF_KEY_ATTRIBUTES = "attributes";
    private static final String CONF_KEY_DSTYPES = "dstypes";
    private static final String CONF_KEY_PATH = "path";
    // https://labs.consol.de/jmx4perl/2009/11/23/jboss-remote-jmx.html - JBoss 4.x / 5.x bug circumvention
    private static final String CONF_KEY_FETCH_JAVA_LANG_MBEAN_METADATA = "fetch-java-lang-mbean-metadata";

    private static final boolean DEF_AUTH_PREEMPTIVE = false;
    private static final boolean DEF_FETCH_JAVA_LANG_MBEAN_METADATA = false;

    // Common data source types
    private static final String TYPE_GAUGE = "GAUGE";
    private static final String TYPE_COUNTER = "COUNTER";
    private static final String TYPE_ABSOLUTE = "ABSOLUTE";

    private static final String JSON_FIELD_STATUS = "status";
    private static final String JSON_FIELD_STACKTRACE = "stacktrace";
    private static final String JSON_FIELD_REQUEST = "request";
    private static final String JSON_FIELD_MBEAN = "mbean";
    private static final String JSON_FIELD_ATTRIBUTE = "attribute";
    private static final String JSON_FIELD_TIMESTAMP = "timestamp";

    private static final String MBEAN_FIELD_TYPE = "type";
    private static final String MBEAN_JAVA_LANG_MEMORY = "java.lang:type=Memory";

    public Jolokia() {
        m_clients = new ArrayList();
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
        // Jolokia URL(s)
        //
        JsonArray urls = config().getArray(CONF_KEY_JOLOKIA_URLS, new JsonArray());
        for (int i = 0; i < urls.size(); i++) {
            String url = urls.get(i).toString();
            logger().info("Connecting to: {}", url);
            m_clients.add(J4pClient.url(url)
                    .user(config().getString(CONF_KEY_USER_NAME))
                    .password(config().getString(CONF_KEY_USER_PASSWORD))
                    .authenticator(new BasicAuthenticator(config().getBoolean(CONF_KEY_AUTH_PREEMPTIVE, DEF_AUTH_PREEMPTIVE)))
                    .build());
        }
        //
        // Build requests
        //
        m_config = new RequestConfig();
        JsonArray requests = config().getArray(CONF_KEY_REQUESTS, new JsonArray());
        for (int i = 0; i < requests.size(); i++) {

            JsonObject request = requests.get(i);
            String mbean = request.getString(CONF_KEY_MBEAN);
            List<String> attributes
                    = (List<String>) request.getArray(CONF_KEY_ATTRIBUTES, new JsonArray()).toList();

            // Dstypes
            Map<String, Object> types = request.getObject(CONF_KEY_DSTYPES, new JsonObject()).toMap();
            m_config.setDstypes(mbean, types);

            String path = request.getString(CONF_KEY_PATH, "");
            int len = attributes.size();
            if (!Strings.isNullOrEmpty(mbean)
                    && len > 0) {

                try {
                    String[] attrs = attributes.toArray(new String[len]);
                    logger().debug("Request mbean: {} attributes: {}", mbean, attrs);
                    J4pReadRequest readRequest = new J4pReadRequest(mbean, attrs);

                    if (!Strings.isNullOrEmpty(path)) {
                        logger().debug("Request path: {}", path);
                        readRequest.setPath(path);
                    }

                    m_config.addRequest(readRequest);

                } catch (MalformedObjectNameException e) {
                    logger().error("Failed to build Jolokia request: {}:{}:{}",
                            mbean, attributes, path, e);
                }
            }
        }

        m_fetchJavaLangMbeanMetadata = config().getBoolean(
                CONF_KEY_FETCH_JAVA_LANG_MBEAN_METADATA,
                DEF_FETCH_JAVA_LANG_MBEAN_METADATA);

        if (m_fetchJavaLangMbeanMetadata) {
            logger().debug("Fetching metadata of {} (JBoss 4.x/5.x bug circumvention)",
                    MBEAN_JAVA_LANG_MEMORY);
        }
    }

    @Override
    protected void handleTimerEvent() {
        //
        // Execute requests in a bulk
        //
        if (!m_config.isEmpty()) {
            for (J4pClient client : m_clients) {
                JSONObject json = new JSONObject();
                try {
                    URI jolokiaUrl = client.getUri();

                    // Perform a silly java.lang metainfo fetch in case of Jboss 4.x/5.x 
                    // to avoid InstanceNotFoundException exception...
                    if (m_fetchJavaLangMbeanMetadata) {
                        fetchMbeanMetadata(client);
                    }

                    List<J4pReadRequest> requests = m_config.getRequests();
                    List<J4pResponse> responses = client.execute(requests);
                    for (J4pResponse response : responses) {

                        json = response.asJSONObject();
                        logger().trace("Response: {}", json);

                        //
                        // Success?
                        //
                        if ((long) json.get(JSON_FIELD_STATUS) == 200L) {

                            Map<String, Object> request = (Map) json.get(JSON_FIELD_REQUEST);
                            String mbean = (String) request.get(JSON_FIELD_MBEAN);
                            Object attribute = request.get(JSON_FIELD_ATTRIBUTE);
                            long timestamp = (long) json.get(JSON_FIELD_TIMESTAMP) * 1000L; // ms
                            Object value = response.getValue();

                            if (attribute instanceof List) {
                                List<String> attributes = (List) attribute;
                                for (String attrName : attributes) {

                                    String attr = translateAttribute(attrName);
                                    if (value instanceof Map) {
                                        handleResponse(
                                                jolokiaUrl,
                                                mbean,
                                                attr,
                                                ((Map) value).get(attrName),
                                                timestamp);
                                    } else {
                                        // Single value
                                        handleResponse(
                                                jolokiaUrl,
                                                mbean,
                                                attr,
                                                value,
                                                timestamp);
                                    }
                                }
                            } else {

                                String attrName = (String) attribute;
                                String attr = translateAttribute(attrName);
                                if (value instanceof Map) {
                                    handleResponse(
                                            jolokiaUrl,
                                            mbean,
                                            attr,
                                            ((Map) value).get(attrName),
                                            timestamp);
                                } else {
                                    // Single value
                                    handleResponse(
                                            jolokiaUrl,
                                            mbean,
                                            attr,
                                            value,
                                            timestamp);
                                }
                            }
                        }
                    }
                } catch (J4pException e) {
                    logger().error("Failed to execute Jolokia requests - response: "
                            + json, e);
                }
            }
        }
    }

    private void handleResponse(
            final URI jolokiaUrl,
            final String mbean,
            final String subgroup,
            final Object value,
            final long timestamp) {

        if (value instanceof Map) {

            Map<String, Object> values = (Map) value;
            for (Entry<String, Object> entry : values.entrySet()) {

                String key = translateAttribute(entry.getKey()); // attribute
                handleResponse(
                        jolokiaUrl,
                        mbean,
                        concatSubgroup(subgroup, key),
                        entry.getValue(),
                        timestamp);
            }

        } else {

            String dsname = resolveMetricName(mbean);
            String dstype = m_config.getDstype(mbean, subgroup);
            emitJolokiaEvent(
                    jolokiaUrl,
                    dsname,
                    dstype,
                    subgroup,
                    value,
                    timestamp);
        }
    }

    private String resolveMetricName(final String mbean) {

        //java.lang:name=ConcurrentMarkSweep,type=GarbageCollector
        StringBuilder name = new StringBuilder();

        // Add domain (if any)
        String pairs = mbean;
        int n = mbean.indexOf(":");
        if (n > 0) {
            name.append(mbean.substring(0, n));
            name.append(".");
            pairs = mbean.substring(n + 1);
        }

        Map<String, String> items = resolveMbeanItems(pairs);

        // type always comes first
        if (items.containsKey(MBEAN_FIELD_TYPE)) {
            name.append(items.remove(MBEAN_FIELD_TYPE));
            name.append(".");
        }

        // Add remaining items to metric name/key
        for (String value : items.values()) {
            name.append(value);
            name.append(".");
        }

        return translateKey(name.toString().substring(0, name.length() - 1));
    }

    private Map<String, String> resolveMbeanItems(final String mbean) {

        Map<String, String> items = new LinkedHashMap();

        Iterable<String> pairs = Splitter.on(",")
                .trimResults()
                .omitEmptyStrings()
                .split(mbean);

        for (String pair : pairs) {
            int n = pair.indexOf("="); // Must exist
            // lowercase for comparison in resolveMetricKey
            String key = pair.substring(0, n).toLowerCase();
            String value = pair.substring(n + 1);
            items.put(key, value);
        }

        return items;
    }

    private String translateKey(final String key) {
        // Replace special characters with "." and convert to lower case
        return CharMatcher.anyOf("\\/, ").replaceFrom(key, '.').toLowerCase();
    }

    private String translateAttribute(final String attribute) {
        // Remove spaces and special characters and convert to lower case
        String result = CharMatcher.WHITESPACE.removeFrom(attribute);
        return CharMatcher.anyOf("\\/,").replaceFrom(result, '_').toLowerCase();
    }

    private String concatSubgroup(
            final String parent,
            final String child) {

        StringBuilder attribute = new StringBuilder(parent);
        attribute.append("_");
        attribute.append(translateAttribute(child));
        return attribute.toString();
    }

    private void emitJolokiaEvent(
            final URI jolokiaUrl,
            final String dsname,
            final String dstype,
            final String subgroup,
            final Object value,
            final long timestamp) {

        //
        // Create new event per value
        //
        JsonObject event = Events.createMetricEvent(
                this,
                timestamp,
                TIMEZONE_UTC,
                jolokiaUrl.getHost() + ":" + jolokiaUrl.getPort(),
                dsname.toLowerCase(),
                dstype.toUpperCase(),
                DSTIME_PRECISION_SEC, // Jolokia uses epoch in seconds
                subgroup.toLowerCase(),
                "-",
                updateInterval(),
                getNumValue(dstype, value));
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

    private Object getNumValue(
            final String dstype,
            final Object value) {

        Object numValue = value;

        if (value != null) {
            //
            // Sanity conversion
            //
            if (value instanceof Double
                    && (TYPE_COUNTER.equals(dstype)
                    || TYPE_ABSOLUTE.equals(dstype))) {

                numValue = ((Number) value).longValue();
            }
        } else {
            switch (dstype) {
                case TYPE_COUNTER:
                case TYPE_ABSOLUTE:
                    numValue = 0L;
                    break;
                default:
                    numValue = 0.0d;
                    break;
            }
        }

        return numValue;
    }

    private void fetchMbeanMetadata(final J4pClient client) {
        try {
            J4pListRequest request = new J4pListRequest(new ObjectName(MBEAN_JAVA_LANG_MEMORY));
            logger().trace("Listing: {}", MBEAN_JAVA_LANG_MEMORY);
            client.execute(request); // Ignore response
        } catch (J4pException | MalformedObjectNameException e) {
            logger().error("Failed to execute list request: {}", MBEAN_JAVA_LANG_MEMORY, e);
        }
    }

    private static class RequestConfig {

        private final List<J4pReadRequest> m_requests;
        private final Map<String, Map<String, Object>> m_dstypes;

        private RequestConfig() {
            m_requests = new ArrayList();
            m_dstypes = new HashMap();
        }

        private boolean isEmpty() {
            return m_requests.isEmpty();
        }

        private String getDstype(
                final String mbean,
                final String attribute) {

            String type = TYPE_GAUGE;
            Map<String, Object> dstypes = m_dstypes.get(mbean);

            if (dstypes != null
                    && dstypes.containsKey(attribute)) {
                type = (String) dstypes.get(attribute);
            }

            return type;
        }

        private List<J4pReadRequest> getRequests() {
            return m_requests;
        }

        private void addRequest(final J4pReadRequest request) {
            m_requests.add(request);
        }

        private void setDstypes(
                final String mbean,
                final Map<String, Object> dstypes) {

            if (!dstypes.isEmpty()) {
                m_dstypes.put(mbean, dstypes);
            }
        }
    }
}
