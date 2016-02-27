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
package io.spikex.filter;

import com.google.common.base.Preconditions;
import io.spikex.core.AbstractFilter;
import io.spikex.filter.internal.Modifier;
import io.spikex.filter.internal.Rule;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import org.vertx.java.core.json.JsonArray;
import org.vertx.java.core.json.JsonObject;

/**
 * <p>
 * This filter has been tested on Linux, Windows, FreeBSD and OS X.
 * <p>
 * Alias: <b>MutateField</b><br>
 * Name: <b>io.spikex.filter.MutateField</b><br>
 * <p>
 * Built-in variables:
 * <p>
 * <ul>
 * <li>#nodeid : the node identifier</li>
 * <li>#node : the node name</li>
 * <li>#clusterid : the cluster identifier</li>
 * <li>#cluster : the cluster name</li>
 * <li>#host : host name as returned by operating system</li>
 * <li>#hostip : the first host ip address as returned by operating system</li>
 * <li>#date : ISO8601 date</li>
 * <li>#timestamp : ISO8601 high-precision timestamp</li>
 * <li>#+&lt;simple date format&gt; : Arbitrarily formatted UTC date and
 * time</li>
 * <li>#env.&lt;environment variable name&gt; : environment variable value</li>
 * <li>#sensor.&lt;sensor name&gt; : sensor value</li>
 * </ul>
 * <p>
 * Example:
 * <pre>
 *  "chain": [
 *              {"Mutate": {
 *                      "update-interval": 15000,
 *                      "add-fields": {
 *                          "@fields/@cpu.total": "%{#sensor.cpu.total.perc}",
 *                          "@fields/@io.total": "%{#sensor.cpu.total.perc}",
 *                          "@fields/@mem.free": "%{#sensor.mem.free.perc}",
 *                          "@fields/@mem.jvm.free": "%{#sensor.mem.jvm.free.perc}",
 *                          "@type": "server"
 *                      },
 *                      "add-tags": ["system", "load", "cpu", "mem"],
 *                      "modifiers": {
 *                          "low-resource": {
 *                                  "add-tags": "ALARM",
 *                                  "@alarm": "low resource"
 *                              }
 *                          }
 *                      },
 *                      "rules": [
 *                          {
 *                              "match-field": "@mem.free",
 *                              "value-lte": 10,
 *                              "modifier": "low-resource"
 *                          },
 *                          {
 *                              "match-field": "@cpu.total",
 *                              "value-gt": 95,
 *                              "modifier": "low-resource"
 *                          }
 *                      ]
 *                  }
 *              }
 *          ]
 * </pre>
 *
 * @author cli
 */
public final class Mutate extends AbstractFilter {

    private final Map<String, Modifier> m_actions; // action-id => action
    private final List<Rule> m_rules;

    private static final String CONF_KEY_ID = "id";
    private static final String CONF_KEY_RULES = "rules";
    private static final String CONF_KEY_MODIFIERS = "modifiers";

    private static final String DEFAULT_MODIFIER = "*"; // Always performed (not rule based)

    public Mutate() {
        m_actions = new HashMap();
        m_rules = new ArrayList();
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
        // Rule based modifiers
        //
        JsonObject modifiers = config().getObject(CONF_KEY_MODIFIERS, new JsonObject());
        Iterator<String> fields = modifiers.getFieldNames().iterator();
        while (fields.hasNext()) {

            String name = fields.next();
            JsonObject def = modifiers.getObject(name);
            Modifier modifier = Modifier.create(name, variables(), def);
            if (!modifier.isEmpty()) {
                m_actions.put(name, modifier);
            }
        }
        // Some modifiers must be defined
        Preconditions.checkState(m_actions.size() > 0, "No modifiers have been defined");
        //
        // Rules (optional)
        //
        m_rules.clear();
        JsonArray rules = config().getArray(CONF_KEY_RULES, new JsonArray());
        for (int i = 0; i < rules.size(); i++) {
            JsonObject ruleDef = rules.get(i);
            String id = ruleDef.getString(CONF_KEY_ID, "rule-" + (i + 1));
            Rule rule = Rule.create(id, ruleDef);
            m_rules.add(rule);
            Preconditions.checkState(m_actions.containsKey(rule.getAction()),
                    "rule \"" + rule.getId() + "\" is referencing a missing modifier: "
                    + rule.getAction());
        }
    }

    @Override
    protected void handleEvent(final JsonObject event) {
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
        // Try to match rule based actions
        //
        String outputAddress = "";
        List<Rule> rules = m_rules;
        for (Rule rule : rules) {
            logger().trace("Evaluating rule: {}", rule);
            if (rule.match(event)) {
                Modifier modifier = actions.get(rule.getAction());
                if (modifier != null) {
                    logger().trace("Applying modifier: {}", modifier);
                    outputAddress = modifier.handle(event);
                }
                break; // First match breaks loop
            }
        }

        //
        // Forward event
        //
        if (outputAddress.length() == 0) {
            emitEvent(event);
        } else {
            emitEvent(event, outputAddress);
        }
    }
}
