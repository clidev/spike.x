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
import com.google.common.base.Strings;
import io.spikex.core.AbstractFilter;
import static io.spikex.core.AbstractVerticle.SHARED_METRICS_KEY;
import io.spikex.core.helper.Events;
import static io.spikex.core.helper.Events.DSTIME_PRECISION_SEC;
import static io.spikex.core.helper.Events.TIMEZONE_UTC;
import io.spikex.core.util.HostOs;
import io.spikex.filter.internal.Modifier;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import org.vertx.java.core.json.JsonObject;

/**
 *
 * @author cli
 */
public final class Metrics extends AbstractFilter {

    private String m_selector;
    private String m_dsnamePrefix; // Empty by default

    private final Map<String, Modifier> m_actions; // action-id => action

    private static final String DEFAULT_MODIFIER = "*"; // Always performed (not rule based)

    private static final String CONF_KEY_METRIC_SELECTOR = "metric-selector";
    private static final String CONF_KEY_DSNAME_PREFIX = "dsname-prefix";

    private static final String DEF_METRIC_SELECTOR = "*";
    private static final String DEF_DSNAME_PREFIX = "";

    private static final String METRIC_DSTYPE = ".dstype";

    public Metrics() {
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
        // Metric selector and datasource name
        //
        m_selector = config().getString(CONF_KEY_METRIC_SELECTOR, DEF_METRIC_SELECTOR);
        m_dsnamePrefix = config().getString(CONF_KEY_DSNAME_PREFIX, DEF_DSNAME_PREFIX);
    }

    @Override
    protected void handleTimerEvent() {
        //
        // Select metrics
        //
        String host = HostOs.hostName();
        String selector = m_selector;
        Map<String, Object> values = vertx.sharedData().getMap(SHARED_METRICS_KEY);
        for (Entry<String, Object> entry : values.entrySet()) {

            String metric = entry.getKey();
            if ((metric.startsWith(selector)
                    && !metric.endsWith(METRIC_DSTYPE))
                    || DEF_METRIC_SELECTOR.equals(selector)) {
                //
                // Datasource type (if any)
                //
                Object value = entry.getValue();
                String dstype = (String) values.get(metric + METRIC_DSTYPE);
                if (dstype == null) {
                    dstype = "";
                }
                //
                // Parse instance (if any)
                //
                // system.cpu.total.time#cpu8 => cpu8
                //
                String instance = "-";
                int n = metric.lastIndexOf("#");
                if (n > 0) {
                    instance = metric.substring(n + 1);
                    metric = metric.substring(0, n); // Remove instance from metric name
                } else {
                    n = metric.length();
                }
                //
                // Parse subgroup (if any)
                //
                // selector: system.cpu
                // system.cpu.total.time#8 => total_time
                //
                // selector: system.memory
                // system.memory.free => free
                //
                // selector: filesystem
                // filesystem.used => used
                //
                // selector: jvm
                // jvm.cpu.user.time => cpu_user_time
                //
                String subgroup = "-";
                int len = selector.length();
                if (len > 1
                        && metric.length() > len) {

                    subgroup = metric.substring(len + 1, n);
                    // Translate "." to "_"
                    subgroup = CharMatcher.is('.').replaceFrom(subgroup, '_');
                }
                //
                // Datasource name
                //
                String dsname = selector;
                if (!Strings.isNullOrEmpty(m_dsnamePrefix)) {
                    dsname = m_dsnamePrefix + "." + selector;
                }
                //
                // Create new event per value
                //
                JsonObject event = Events.createMetricEvent(
                        this,
                        System.currentTimeMillis(),
                        TIMEZONE_UTC,
                        host,
                        dsname,
                        dstype,
                        DSTIME_PRECISION_SEC, // Always in seconds
                        subgroup,
                        instance,
                        updateInterval(),
                        value);
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
    }
}
