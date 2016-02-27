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

import com.google.common.base.Splitter;
import com.google.common.base.Strings;
import io.spikex.core.AbstractFilter;
import io.spikex.core.helper.Events;
import static io.spikex.core.helper.Events.EVENT_FIELD_DSNAME;
import static io.spikex.core.helper.Events.EVENT_FIELD_DSTYPE;
import static io.spikex.core.helper.Events.EVENT_FIELD_SUBGROUP;
import static io.spikex.core.helper.Events.EVENT_FIELD_TAGS;
import static io.spikex.core.helper.Events.EVENT_FIELD_VALUE;
import static io.spikex.core.helper.Events.EVENT_PRIORITY_HIGH;
import static io.spikex.core.helper.Events.EVENT_PRIORITY_NORMAL;
import io.spikex.core.util.Numbers;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.vertx.java.core.eventbus.EventBus;
import org.vertx.java.core.json.JsonArray;
import org.vertx.java.core.json.JsonObject;

/**
 *
 * @author cli
 */
public final class NagiosNrdpProcessor {

    private JsonArray m_tokens;
    private JsonArray m_eventTags;
    private JsonArray m_metricTags;
    private boolean m_fixUnits;

    private final AbstractFilter m_filter;
    private final JsonObject m_config;
    private final EventBus m_eventBus;
    private final JsonArray m_tags;
    private final List<NrdpServiceConfig> m_serviceConfigs;

    // Metric-and-event - processes both events and performance data (default)
    // Metric - processes only performance data (ignores state)
    // Event - processes only events
    private static final String TYPE_METRIC_AND_EVENT = "metric-and-event";
    private static final String TYPE_METRIC = "metric";
    private static final String TYPE_EVENT = "event";

    private static final String CONF_KEY_ACCEPTED_TOKENS = "accepted-tokens";
    private static final String CONF_KEY_SERVICES = "services";
    private static final String CONF_KEY_EVENT_TAGS = "event-tags";
    private static final String CONF_KEY_METRIC_TAGS = "metric-tags";
    private static final String CONF_KEY_NAME = "name";
    private static final String CONF_KEY_TYPE = "type";
    private static final String CONF_KEY_DSNAME = "dsname";
    private static final String CONF_KEY_DSTYPE = "dstype";
    private static final String CONF_KEY_FIX_UNITS = "fix-units";
    private static final String CONF_KEY_IGNORE_STATES = "ignore-states";
    private static final String CONF_KEY_PREFIX_INFO = "prefix-info";
    private static final String CONF_KEY_PREFIX_WARNING = "prefix-warning";
    private static final String CONF_KEY_PREFIX_CRITICAL = "prefix-critical";
    private static final String CONF_KEY_GROUP_FIELDS = "group-fields";
    private static final String CONF_KEY_OUTPUT_PATTERNS = "output-patterns";

    private static final String ANY_NAME = "*";
    private static final String DEF_NAME = ANY_NAME;
    private static final String DEF_TYPE = TYPE_METRIC_AND_EVENT; // event and performance data
    private static final String DEF_PREFIX = "Nagios NRDP event";
    private static final String DEF_DSTYPE = "GAUGE";

    private static final String CONFIG_STATE_OK = "OK";
    private static final String CONFIG_STATE_WARNING = "WARNING";
    private static final String CONFIG_STATE_CRITICAL = "CRITICAL";

    private static final String NRDP_STATE_OK = "0";
    private static final String NRDP_STATE_WARNING = "1";
    private static final String NRDP_STATE_CRITICAL = "2";
    private static final String NRDP_STATE_UNKNOWN = "3"; // Considered same as warning

    private static final String EVENT_FIELD_VALUES = "@values";
    private static final String EVENT_FIELD_SERVICE_NAME = "service-name";

    private static final Logger m_logger = LoggerFactory.getLogger(NagiosNrdpProcessor.class);

    public NagiosNrdpProcessor(
            final AbstractFilter filter,
            final JsonObject config,
            final EventBus eventBus,
            final JsonArray tags) {

        m_filter = filter;
        m_config = config;
        m_eventBus = eventBus;
        m_tags = tags;
        m_serviceConfigs = new ArrayList();
    }

    public void init() {

        m_tokens = m_config.getArray(CONF_KEY_ACCEPTED_TOKENS, new JsonArray());
        m_eventTags = m_config.getArray(CONF_KEY_EVENT_TAGS, new JsonArray());
        m_metricTags = m_config.getArray(CONF_KEY_METRIC_TAGS, new JsonArray());
        m_fixUnits = m_config.getBoolean(CONF_KEY_FIX_UNITS, true);

        JsonArray services = m_config.getArray(CONF_KEY_SERVICES, new JsonArray());
        for (int i = 0; i < services.size(); i++) {
            JsonObject service = (JsonObject) services.get(i);

            // Service configuration
            String name = service.getString(CONF_KEY_NAME, DEF_NAME);
            NrdpServiceConfig serviceConfig = new NrdpServiceConfig(
                    name,
                    service.getString(CONF_KEY_TYPE, DEF_TYPE),
                    service.getString(CONF_KEY_DSNAME, name), // Use service name if no dsname defined
                    service.getString(CONF_KEY_DSTYPE, DEF_DSTYPE),
                    service.getString(CONF_KEY_PREFIX_INFO, DEF_PREFIX),
                    service.getString(CONF_KEY_PREFIX_WARNING, DEF_PREFIX),
                    service.getString(CONF_KEY_PREFIX_CRITICAL, DEF_PREFIX));

            // Ignored states
            JsonArray ignoreStates = service.getArray(CONF_KEY_IGNORE_STATES, new JsonArray());
            for (int j = 0; j < ignoreStates.size(); j++) {
                String state = (String) ignoreStates.get(j);
                if (!Strings.isNullOrEmpty(state)) {
                    switch (state.toUpperCase()) {
                        case CONFIG_STATE_OK:
                            serviceConfig.addIgnoreState(NRDP_STATE_OK);
                            break;
                        case CONFIG_STATE_WARNING:
                            serviceConfig.addIgnoreState(NRDP_STATE_WARNING);
                            break;
                        case CONFIG_STATE_CRITICAL:
                            serviceConfig.addIgnoreState(NRDP_STATE_CRITICAL);
                            break;
                    }
                }
            }

            // Group field names for patterns
            JsonArray groupFields = service.getArray(CONF_KEY_GROUP_FIELDS, new JsonArray());
            for (int j = 0; j < groupFields.size(); j++) {
                String field = (String) groupFields.get(j);
                serviceConfig.addGroupField(field);
            }

            // Output match patterns for metrics
            JsonArray patterns = service.getArray(CONF_KEY_OUTPUT_PATTERNS, new JsonArray());
            for (int j = 0; j < patterns.size(); j++) {
                String pattern = (String) patterns.get(j);
                serviceConfig.addPattern(pattern);
            }
            m_serviceConfigs.add(serviceConfig);
        }
    }

    public boolean isValidToken(final String token) {
        // Check token
        boolean valid = false;
        for (int i = 0; i < m_tokens.size(); i++) {
            String acceptedToken = (String) m_tokens.get(i);
            if (acceptedToken.equals(token)) {
                valid = true;
                break;
            }
        }
        return valid;
    }

    public boolean processCheckResult(
            final String host,
            final String service,
            final String state,
            final String output) {

        boolean success = false;

        m_logger.trace("Got host: {} service: {} state: {} output: {}",
                host, service, state, output);

        try {
            // Find matching config (matching service name)
            NrdpServiceConfig config = null;
            for (NrdpServiceConfig serviceConfig : m_serviceConfigs) {
                if (service.equals(serviceConfig.getName())
                        || ANY_NAME.equals(serviceConfig.getName())) {

                    config = serviceConfig;
                    break;
                }
            }

            if (config != null) {

                String type = config.getType();
                switch (type) {
                    case TYPE_METRIC_AND_EVENT: {
                        // Metric and event
                        String perfdata = getPerfdata(output);
                        if (!Strings.isNullOrEmpty(perfdata)) {
                            processMetrics(config, host, service, perfdata);
                        }
                        // Ignored state (only applies for events)?
                        String message = getMessage(output);
                        if (!Strings.isNullOrEmpty(message)
                                && !config.isIgnoredState(state)) {
                            processEvent(config, host, service, state, message);
                        }
                        m_logger.trace("TYPE_METRIC_AND_EVENT message: {}", message);
                    }
                    break;
                    case TYPE_METRIC: {
                        String perfdata = getPerfdata(output);
                        if (!Strings.isNullOrEmpty(perfdata)) {
                            processMetrics(config, host, service, perfdata);
                        }
                    }
                    break;
                    case TYPE_EVENT: {
                        String message = getMessage(output);
                        m_logger.trace("EVENT message: {}", message);
                        if (!Strings.isNullOrEmpty(message)) {

                            // Special case - when we want to parse metrics from event message
                            m_logger.trace("Group fields: {} patterns: {}", config.hasGroupFields(), config.hasPatterns());
                            if (config.hasGroupFields()
                                    && config.hasPatterns()) {
                                processMetrics(config, host, service, message);
                            }

                            // Ignored state (only applies for events)?
                            if (!config.isIgnoredState(state)) {
                                processEvent(config, host, service, state, message);
                            }
                        }
                    }
                    break;

                    default:
                        m_logger.error("Ignoring NRDP result - unsupported type: {}", type);
                        break;
                }
            } else {
                m_logger.trace("Found no NRDP service match for: {}", service);
            }
        } catch (Exception e) {
            m_logger.error("Failed to process result for {}:{} - output: {}",
                    host, service, output, e);
        }

        return success;
    }

    private String getPerfdata(final String output) {
        String perfdata = "";
        // Cut off the message part from the output
        int n = output.indexOf("|");
        if (n != -1) {
            perfdata = output.substring(n + 1);
        }
        return perfdata;
    }

    private String getMessage(final String output) {
        String message = output; // Accept as message without "|"
        int n = message.indexOf("|");
        if (n != -1) {
            message = message.substring(0, n); // Skip parts after "|"
        }
        return message;
    }

    private void processMetrics(
            final NrdpServiceConfig config,
            final String host,
            final String service,
            final String perfdata) throws ParseException {

        m_logger.trace("Processing {}:{} metrics", host, service);

        //
        // Do pattern matching?
        //
        List<JsonObject> events = new ArrayList();
        if (config.hasPatterns()) {
            //
            // Build event list (try matching against each pattern)
            //
            if (config.hasGroupFields()) {
                buildGroupFieldMetricEvents(m_filter,
                        config,
                        host,
                        service,
                        perfdata,
                        events);
            } else {
                buildFieldValueMetricEvents(m_filter,
                        config,
                        host,
                        service,
                        perfdata,
                        events);
            }

            if (events.isEmpty()) {
                m_logger.debug("Pattern of {} service did not match: {}",
                        service, perfdata);
            }
        } else {

            // No pattern, value = contents of output
            Number value = parseNumber(perfdata);

            //
            // Create metric event
            //
            JsonObject event = createDefaultMetricEvent(
                    m_filter,
                    host);

            event.putString(EVENT_FIELD_SERVICE_NAME, service);
            event.putString(EVENT_FIELD_DSNAME, config.getDsname().toLowerCase());
            event.putString(EVENT_FIELD_DSTYPE, config.getDstype().toUpperCase());
            event.putString(EVENT_FIELD_SUBGROUP, service.toLowerCase());
            event.putValue(EVENT_FIELD_VALUE, value);
            events.add(event);
        }
        //
        // One event per value
        //
        for (JsonObject event : events) {
            //
            // Add tags and publish event
            //
            JsonArray tags = new JsonArray();
            for (int i = 0; i < m_tags.size(); i++) {
                tags.add(m_tags.get(i));
            }
            for (int i = 0; i < m_metricTags.size(); i++) {
                tags.add(m_metricTags.get(i));
            }
            event.putArray(EVENT_FIELD_TAGS, tags);
            m_eventBus.publish(m_filter.getDestinationAddress(), event);
        }

    }

    private void processEvent(
            final NrdpServiceConfig config,
            final String host,
            final String service,
            final String state,
            final String message) {

        m_logger.trace("Processing {}:{} event", host, service);

        if (!Strings.isNullOrEmpty(message)) {
            //
            // Build title
            //
            StringBuilder title = new StringBuilder();
            String priority = EVENT_PRIORITY_NORMAL;
            if (!Strings.isNullOrEmpty(state)) {
                switch (state) {
                    case NRDP_STATE_OK:
                        title.append(config.getPrefixInfo());
                        break;
                    case NRDP_STATE_WARNING:
                    case NRDP_STATE_UNKNOWN:
                        priority = EVENT_PRIORITY_HIGH;
                        title.append(config.getPrefixWarning());
                        break;
                    case NRDP_STATE_CRITICAL:
                        priority = EVENT_PRIORITY_HIGH;
                        title.append(config.getPrefixCritical());
                        break;
                    default:
                        title.append(config.getPrefixInfo());
                        break;
                }
            }

            title.append(" (");
            title.append(host);
            title.append(":");
            title.append(service);
            title.append(")");

            //
            // Create notification event
            //
            JsonObject event = Events.createNotificationEvent(
                    m_filter,
                    host,
                    priority,
                    title.toString(),
                    message);

            // Add tags and publish event
            JsonArray tags = new JsonArray();
            for (int i = 0; i < m_tags.size(); i++) {
                tags.add(m_tags.get(i));
            }
            for (int i = 0; i < m_eventTags.size(); i++) {
                tags.add(m_eventTags.get(i));
            }
            event.putArray(EVENT_FIELD_TAGS, tags);
            event.putString(EVENT_FIELD_SERVICE_NAME, service);
            m_eventBus.publish(m_filter.getDestinationAddress(), event);

        } else {
            m_logger.error("Empty NRDP event received: {}:{}", host, service);
        }
    }

    private void buildGroupFieldMetricEvents(
            final AbstractFilter filter,
            final NrdpServiceConfig config,
            final String host,
            final String service,
            final String output,
            final List<JsonObject> events) throws ParseException {

        m_logger.trace("Matching output: {}", output);
        for (Pattern pattern : config.getPatterns()) {

            JsonObject event = createDefaultMetricEvent(filter, host);
            List<String> groupFields = config.getGroupFields();
            List<String> values = new ArrayList();

            Matcher m = pattern.matcher(output);
            while (m.find()) {

                // Parse values for all defined groups
                for (int i = 1; i <= groupFields.size(); i++) {
                    values.add(m.group(i));
                }

                if (!values.isEmpty()) {

                    event.putString(EVENT_FIELD_SERVICE_NAME, service);
                    event.putString(EVENT_FIELD_DSNAME, config.getDsname().toLowerCase());
                    event.putString(EVENT_FIELD_DSTYPE, config.getDstype().toUpperCase());
                    event.putString(EVENT_FIELD_SUBGROUP, service.toLowerCase());

                    for (int i = 0; i < groupFields.size(); i++) {

                        String field = groupFields.get(i);
                        String value = values.get(i);

                        switch (field) {
                            case EVENT_FIELD_VALUE:
                                // Value field must be a number
                                event.putValue(EVENT_FIELD_VALUE, parseNumber(value));
                                break;
                            default:
                                event.putString(field, value); // event field and value
                                break;
                        }
                    }

                    events.add(event);
                }
            }
        }
    }

    private void buildFieldValueMetricEvents(
            final AbstractFilter filter,
            final NrdpServiceConfig config,
            final String host,
            final String service,
            final String output,
            final List<JsonObject> events) throws ParseException {

        m_logger.trace("Matching output: {}", output);
        for (Pattern pattern : config.getPatterns()) {

            JsonObject fields = new JsonObject();
            JsonObject event = createDefaultMetricEvent(filter, host);
            Map<String, String> values = new HashMap();

            Matcher m = pattern.matcher(output);
            while (m.find()) {

                String field = m.group(1);
                String value = m.group(2);
                String endOfEvent = m.group(3);

                if (field != null) {
                    switch (field) {
                        case EVENT_FIELD_VALUE:
                            // Value field must be a number
                            event.putValue(EVENT_FIELD_VALUE, parseNumber(value));
                            break;
                        case EVENT_FIELD_VALUES:
                            // Multiple values
                            // field: @values
                            // value: used=3.579GB,free=4.356GB,total=7.935GB
                            values = Splitter.on(',')
                                    .trimResults()
                                    .withKeyValueSeparator("=")
                                    .split(value);
                            break;
                        default:
                            fields.putString(field, value); // event field and value
                            break;
                    }
                }

                //
                // Done, start next event
                //
                if (!Strings.isNullOrEmpty(endOfEvent)) {

                    if (values.isEmpty()) {
                        event.putString(EVENT_FIELD_SERVICE_NAME, service);
                        event.mergeIn(fields);
                        events.add(event);
                        fields = new JsonObject();
                        event = createDefaultMetricEvent(filter, host);
                    } else {
                        // One event per subgroup (used, free and total)
                        for (Entry<String, String> entry : values.entrySet()) {
                            event.putString(EVENT_FIELD_SUBGROUP, entry.getKey().toLowerCase());
                            event.putValue(EVENT_FIELD_VALUE, parseNumber(entry.getValue()));
                            event.putString(EVENT_FIELD_SERVICE_NAME, service);
                            event.mergeIn(fields);
                            events.add(event);
                            event = createDefaultMetricEvent(filter, host);
                        }
                        fields = new JsonObject();
                    }

                    values = new HashMap();
                }
            }
        }
    }

    private Number parseNumber(final String value) throws ParseException {
        //
        // Resolve unit (if any) and parse value
        //
        Number normValue = 0.0d;
        if (m_fixUnits) {
            if (!Strings.isNullOrEmpty(value)) {
                String strValue = value.toUpperCase();
                int len = strValue.length();
                if (strValue.endsWith("PB")) {
                    // Convert to bytes
                    normValue = Numbers.parseNumber(strValue.substring(0, len - 2));
                    normValue = normValue.doubleValue() * 1125899906842624.0d;
                } else if (strValue.endsWith("TB")) {
                    // Convert to bytes
                    normValue = Numbers.parseNumber(strValue.substring(0, len - 2));
                    normValue = normValue.doubleValue() * 1099511627776.0d;
                } else if (strValue.endsWith("GB")) {
                    // Convert to bytes
                    normValue = Numbers.parseNumber(strValue.substring(0, len - 2));
                    normValue = normValue.doubleValue() * 1073741824.0d;
                } else if (strValue.endsWith("MB")) {
                    // Convert to bytes
                    normValue = Numbers.parseNumber(strValue.substring(0, len - 2));
                    normValue = normValue.doubleValue() * 1048576.0d;
                } else if (strValue.endsWith("KB")) {
                    // Convert to bytes
                    normValue = Numbers.parseNumber(strValue.substring(0, len - 2));
                    normValue = normValue.doubleValue() * 1024.0d;
                } else if (strValue.endsWith("%")
                        || strValue.endsWith("B")) {
                    // Drop unit
                    normValue = Numbers.parseNumber(strValue.substring(0, len - 1));
                } else {
                    normValue = Numbers.parseNumber(strValue);
                }
            }
        } else {
            normValue = Numbers.parseNumber(value);
        }
        return normValue;
    }

    private JsonObject createDefaultMetricEvent(
            final AbstractFilter filter,
            final String host) {
        //
        // Defaults
        //
        String dsname = "";
        String dstype = DEF_DSTYPE;
        String subgroup = "-";
        String instance = "-";
        int interval = 0;
        double value = 0.0d;
        //
        // Create metric event
        //
        JsonObject event = Events.createMetricEvent(filter,
                host,
                dsname,
                dstype,
                subgroup,
                instance,
                interval,
                value);

        return event;
    }

    private static class NrdpServiceConfig {

        private final String m_name;
        private final String m_type;
        private final String m_dsname;
        private final String m_dstype;
        private final String m_prefixInfo;
        private final String m_prefixWarning;
        private final String m_prefixCritical;
        private final List<String> m_ignoredStates;
        private final List<String> m_groupFields;
        private final List<Pattern> m_patterns;

        private NrdpServiceConfig(
                final String name,
                final String type,
                final String dsname,
                final String dstype,
                final String prefixInfo,
                final String prefixWarning,
                final String prefixCritical) {

            m_name = name;
            m_type = type;
            m_dsname = dsname;
            m_dstype = dstype;
            m_prefixInfo = prefixInfo;
            m_prefixWarning = prefixWarning;
            m_prefixCritical = prefixCritical;
            m_groupFields = new ArrayList();
            m_patterns = new ArrayList();
            m_ignoredStates = new ArrayList();
        }

        private boolean isIgnoredState(final String state) {
            return (m_ignoredStates.contains(state));
        }

        private boolean hasGroupFields() {
            return !(m_groupFields.isEmpty());
        }

        private boolean hasPatterns() {
            return !(m_patterns.isEmpty());
        }

        private String getName() {
            return m_name;
        }

        private String getType() {
            return m_type;
        }

        private String getDsname() {
            return m_dsname;
        }

        private String getDstype() {
            return m_dstype;
        }

        private String getPrefixInfo() {
            return m_prefixInfo;
        }

        private String getPrefixWarning() {
            return m_prefixWarning;
        }

        private String getPrefixCritical() {
            return m_prefixCritical;
        }

        private List<String> getGroupFields() {
            return m_groupFields;
        }

        private List<Pattern> getPatterns() {
            return m_patterns;
        }

        private void addIgnoreState(final String state) {
            m_ignoredStates.add(state);
        }

        private void addPattern(final String pattern) {
            if (!Strings.isNullOrEmpty(pattern)) {
                m_patterns.add(Pattern.compile(pattern));
            }
        }

        private void addGroupField(final String field) {
            if (!Strings.isNullOrEmpty(field)) {
                m_groupFields.add(field);
            }
        }
    }
}
