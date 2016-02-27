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

import com.google.common.base.CharMatcher;
import com.google.common.base.Strings;
import io.spikex.core.AbstractFilter;
import io.spikex.core.helper.Events;
import static io.spikex.core.helper.Events.DSTIME_PRECISION_MILLIS;
import static io.spikex.core.helper.Events.DSTIME_PRECISION_SEC;
import static io.spikex.core.helper.Events.EVENT_FIELD_TAGS;
import static io.spikex.core.helper.Events.TIMEZONE_UTC;
import static io.spikex.filter.internal.CollectdMapping.DEF_MAPPING;
import static io.spikex.filter.internal.CollectdMapping.SPIKEX_KEY_DSNAME;
import static io.spikex.filter.internal.CollectdMapping.SPIKEX_KEY_DSTYPE;
import static io.spikex.filter.internal.CollectdMapping.SPIKEX_KEY_INSTANCE;
import static io.spikex.filter.internal.CollectdMapping.SPIKEX_KEY_SUBGROUP;
import static io.spikex.filter.internal.CollectdTypes.COLLECTD_KEY_DSNAMES;
import static io.spikex.filter.internal.CollectdTypes.COLLECTD_KEY_DSTYPES;
import static io.spikex.filter.internal.CollectdTypes.COLLECTD_KEY_HOST;
import static io.spikex.filter.internal.CollectdTypes.COLLECTD_KEY_INTERVAL;
import static io.spikex.filter.internal.CollectdTypes.COLLECTD_KEY_PLUGIN;
import static io.spikex.filter.internal.CollectdTypes.COLLECTD_KEY_PLUGIN_INSTANCE;
import static io.spikex.filter.internal.CollectdTypes.COLLECTD_KEY_TIME;
import static io.spikex.filter.internal.CollectdTypes.COLLECTD_KEY_TYPE;
import static io.spikex.filter.internal.CollectdTypes.COLLECTD_KEY_TYPE_INSTANCE;
import static io.spikex.filter.internal.CollectdTypes.COLLECTD_KEY_VALUES;
import java.util.HashMap;
import java.util.Map;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.vertx.java.core.Handler;
import org.vertx.java.core.eventbus.EventBus;
import org.vertx.java.core.json.JsonArray;
import org.vertx.java.core.json.JsonObject;

/**
 *
 * @author cli
 */
public final class CollectdJsonHandler implements Handler<HttpResponse> {

    private final AbstractFilter m_filter;
    private final JsonObject m_config;
    private final EventBus m_eventBus;
    private final JsonArray m_tags;
    private final JSONParser m_parser;
    private final Map<String, CollectdMapping> m_mappings;

    private static final String CONF_KEY_COLLECTD_CONFIG = "collectd-config";
    private static final String CONF_KEY_MAPPINGS = "mappings";
    private static final String CONF_KEY_DSNAME_SEPARATOR = "dsname-separator";
    private static final String CONF_KEY_ITEM_SEPARATOR = "item-separator";

    private static final String DEF_DSNAME_SEPARATOR = ".";
    private static final String DEF_ITEM_SEPARATOR = "_";

    private static final Logger m_logger = LoggerFactory.getLogger(CollectdJsonHandler.class);

    public CollectdJsonHandler(
            final AbstractFilter filter,
            final JsonObject config,
            final EventBus eventBus,
            final JsonArray tags) {

        m_filter = filter;
        m_config = config.getObject(CONF_KEY_COLLECTD_CONFIG, new JsonObject());
        m_eventBus = eventBus;
        m_tags = tags;
        m_parser = new JSONParser();
        m_mappings = new HashMap();
    }

    public void init() {

        String dsnameSeparator = m_config.getString(CONF_KEY_DSNAME_SEPARATOR, DEF_DSNAME_SEPARATOR);
        String itemSeparator = m_config.getString(CONF_KEY_ITEM_SEPARATOR, DEF_ITEM_SEPARATOR);

        // Build default mappings
        CollectdMapping.buildDefaults(m_mappings,
                dsnameSeparator,
                itemSeparator);

        // Mappings defined in the configuration
        JsonArray jsonMappings = m_config.getArray(CONF_KEY_MAPPINGS, new JsonArray());
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
    }

    @Override
    public void handle(final HttpResponse response) {

        String body = response.getBody();

        try {
            if (!Strings.isNullOrEmpty(body)) {

                JSONArray items = (JSONArray) m_parser.parse(body);
                for (Object item : items) {

                    JSONObject map = (JSONObject) item;

                    // Common fields
                    String host = (String) map.get(COLLECTD_KEY_HOST);
                    String plugin = (String) map.get(COLLECTD_KEY_PLUGIN);
                    String pluginInstance = (String) map.get(COLLECTD_KEY_PLUGIN_INSTANCE);
                    String type = (String) map.get(COLLECTD_KEY_TYPE);
                    String typeInstance = (String) map.get(COLLECTD_KEY_TYPE_INSTANCE);
                    Number interval = (Number) map.get(COLLECTD_KEY_INTERVAL);

                    // Special handling of time
                    long timestamp;
                    String precision = DSTIME_PRECISION_SEC; // Default
                    Object time = map.get(COLLECTD_KEY_TIME);
                    if (time instanceof Double) {
                        timestamp = (long) ((double) time * 1000.0d); // ms
                    } else if (time instanceof Number) {
                        timestamp = ((Number) time).longValue() * 1000L; // ms
                    } else {
                        // Remove "." character from timestamp and convert to long
                        String timeStr = (String) time;
                        timeStr = CharMatcher.is('.').removeFrom(timeStr);
                        timestamp = Long.parseLong(timeStr);
                        precision = DSTIME_PRECISION_MILLIS; // Higher precision
                    }

                    m_logger.trace("host: {} plugin: {} plugin-instance: {} type: {} "
                            + "type-instance: {} interval: {} timestamp: {}",
                            host, plugin, pluginInstance, type, typeInstance,
                            interval, timestamp);
                    // 
                    // values
                    //
                    JSONArray values = (JSONArray) map.get(COLLECTD_KEY_VALUES);
                    JSONArray dstypes = (JSONArray) map.get(COLLECTD_KEY_DSTYPES);
                    JSONArray dsnames = (JSONArray) map.get(COLLECTD_KEY_DSNAMES);

                    String destAddr = m_filter.getDestinationAddress();

                    if (values != null
                            && destAddr != null && destAddr.length() > 0) {

                        for (int i = 0; i < values.size(); i++) {

                            Object value = values.get(i);
                            String dstype = ((String) dstypes.get(i)).toUpperCase();
                            String dsname = (String) dsnames.get(i);

                            //
                            // Create new event per value
                            //
                            JsonObject event = Events.createMetricEvent(m_filter,
                                    timestamp,
                                    TIMEZONE_UTC,
                                    host,
                                    dsname,
                                    dstype,
                                    precision,
                                    "-", // subgroup
                                    "-", // instance
                                    interval.intValue(),
                                    CollectdMeasurement.resolveNumValue(dstype, value));

                            //
                            // Resolve and set dsname, subgroup and instance
                            //
                            CollectdMapping mapping = m_mappings.get(plugin);

                            // Use default mapping?
                            if (mapping == null) {
                                mapping = m_mappings.get(DEF_MAPPING);
                            }

                            mapping.resolveAndSetFields(
                                    event,
                                    plugin,
                                    pluginInstance,
                                    type,
                                    typeInstance,
                                    dsname,
                                    dstype);

                            // Add tags
                            event.putArray(EVENT_FIELD_TAGS, m_tags);

                            m_eventBus.publish(destAddr, event);
                        }
                    }
                }
            }
        } catch (ParseException e) {
            m_logger.error("Failed to parse JSON: {}", body, e);
        }
    }
}
