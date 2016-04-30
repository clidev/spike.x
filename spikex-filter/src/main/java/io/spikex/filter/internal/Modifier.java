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

import com.google.common.base.Strings;
import static io.spikex.core.helper.Events.EVENT_FIELD_TAGS;
import io.spikex.core.helper.Variables;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.vertx.java.core.json.JsonArray;
import org.vertx.java.core.json.JsonObject;
import parsii.eval.Parser;

/**
 *
 * @author cli
 */
public final class Modifier {

    private final String m_id;
    private final String m_outputAddress;
    private final Variables m_variables;
    private final Map<String, Object> m_addFields;
    private final Map<String, String> m_renames;
    private final List<String> m_addTags;
    private final List<String> m_delFields;
    private final List<String> m_delTags;
    private final String m_expression;

    private static final String CONFIG_FIELD_ADD_FIELDS = "add-fields";
    private static final String CONFIG_FIELD_ADD_TAGS = "add-tags";
    private static final String CONFIG_FIELD_DEL_FIELDS = "del-fields";
    private static final String CONFIG_FIELD_DEL_TAGS = "del-tags";
    private static final String CONFIG_FIELD_RENAMES = "renames";
    private static final String CONFIG_FIELD_EXPRESSION = "expression";
    private static final String CONFIG_FIELD_OUTPUT_ADDRESS = "output-address";

    private static final String EXPR_EQUAL_SIGN = "=";

    private static final String VAR_PREFIX = "%{";

    private static final Logger m_logger = LoggerFactory.getLogger(Modifier.class);

    private Modifier(
            final String id,
            final String outputAddress,
            final Variables variables,
            final Map<String, Object> addFields,
            final Map<String, String> renames,
            final List<String> addTags,
            final List<String> delFields,
            final List<String> delTags,
            final String expression) {

        m_id = id;
        m_outputAddress = outputAddress;
        m_variables = variables;
        m_addFields = addFields;
        m_renames = renames;
        m_addTags = addTags;
        m_delFields = delFields;
        m_delTags = delTags;
        m_expression = expression;
    }

    public boolean isEmpty() {
        return m_addFields.isEmpty()
                && m_renames.isEmpty()
                && m_addTags.isEmpty()
                && m_delFields.isEmpty()
                && m_delTags.isEmpty()
                && Strings.isNullOrEmpty(m_expression);
    }

    public String getId() {
        return m_id;
    }

    public String handle(final JsonObject event) {

        //
        // add-fields
        //
        Map<String, Object> addFields = m_addFields;
        if (!addFields.isEmpty()) {
            addFields(m_variables, event, addFields);
        }

        //
        // renames
        //
        Map<String, String> renames = m_renames;
        if (!renames.isEmpty()) {
            renames(event, renames);
        }

        //
        // add-tags
        //
        List<String> addTags = m_addTags;
        if (!addTags.isEmpty()) {
            addTags(event, addTags);
        }

        //
        // del-fields
        //
        List<String> delFields = m_delFields;
        if (!delFields.isEmpty()) {
            delFields(event, delFields);
        }

        //
        // del-tags
        //
        List<String> delTags = m_delTags;
        if (!delTags.isEmpty()) {
            delTags(event, delTags);
        }

        //
        // expression (parsii)
        //
        if (!Strings.isNullOrEmpty(m_expression)) {
            evaluate(event, m_expression, m_variables);
        }

        //
        // output-address
        //
        return m_outputAddress;
    }

    @Override
    public String toString() {
        String sname = getClass().getSimpleName();
        StringBuilder sb = new StringBuilder(sname);
        sb.append("[");
        sb.append(hashCode());
        sb.append("] id: ");
        sb.append(getId());
        sb.append(" add-fields: ");
        sb.append(m_addFields);
        sb.append(" renames: ");
        sb.append(m_renames);
        sb.append(" add-tags: ");
        sb.append(m_addTags);
        sb.append(" del-fields: ");
        sb.append(m_delFields);
        sb.append(" del-tags: ");
        sb.append(m_delTags);
        return sb.toString();
    }

    private void addFields(
            final Variables variables,
            final JsonObject event,
            final Map<String, Object> fields) {

        Set<Map.Entry<String, Object>> entries = fields.entrySet();
        for (Map.Entry<String, Object> entry : entries) {

            String field = entry.getKey();
            Object valueDef = entry.getValue(); // Support arrays
            if (valueDef instanceof List) {
                JsonArray jsonArray = new JsonArray();
                List valueDefs = (List) valueDef;
                for (Object def : valueDefs) {
                    String value = String.valueOf(def);
                    jsonArray.add(variables.translate(event, value));
                }
                event.putValue(field, jsonArray);
            } else {
                event.putValue(field, variables.translate(event, valueDef));
            }
        }
    }

    private void renames(
            final JsonObject event,
            final Map<String, String> fields) {

        Set<Map.Entry<String, String>> entries = fields.entrySet();
        for (Map.Entry<String, String> entry : entries) {

            String oldName = entry.getKey();
            if (event.containsField(oldName)) {

                String newName = entry.getValue();
                Object value = event.removeField(oldName);
                if (value instanceof Map) {
                    event.putObject(newName, new JsonObject((Map<String, Object>) value));
                } else if (value instanceof List) {
                    event.putArray(newName, new JsonArray((List<Object>) value));
                } else {
                    event.putValue(newName, value);
                }
            }
        }
    }

    private void addTags(
            final JsonObject event,
            final List<String> tags) {

        JsonArray curTags = event.getArray(EVENT_FIELD_TAGS, new JsonArray());
        for (String tag : tags) {
            curTags.addString(tag);
        }
        event.putArray(EVENT_FIELD_TAGS, curTags);
    }

    private void delFields(
            final JsonObject event,
            final List<String> fields) {

        for (String field : fields) {
            if (event.containsField(field)) {
                event.removeField(field);
            }
        }
    }

    private void delTags(
            final JsonObject event,
            final List<String> tags) {

        JsonArray curTags = event.getArray(EVENT_FIELD_TAGS, new JsonArray());
        for (String tag : tags) {
            if (curTags.contains(tag)) {
                JsonArray newTags = new JsonArray();
                for (int i = 0; i < curTags.size(); i++) {
                    String oldTag = curTags.get(i);
                    if (!tag.equals(oldTag)) {
                        newTags.addString(oldTag);
                    }
                }
                curTags = newTags;
            }
        }
        event.putArray(EVENT_FIELD_TAGS, curTags);
    }

    private void evaluate(
            final JsonObject event,
            final String expression,
            final Variables variables) {

        try {
            int n = expression.indexOf(EXPR_EQUAL_SIGN);
            if (n > 0) {
                //
                // Resolve field name of assignment
                //
                String field = expression.substring(0, n).trim();
                int len = field.length();

                if (len > 0) {
                    int pfxPos = field.indexOf(VAR_PREFIX);
                    field = field.substring(pfxPos + VAR_PREFIX.length(), len - 1);

                    //
                    // Translate Spike.x field references (to values)
                    //
                    String expr = expression.substring(n + 1);
                    expr = variables.translate(event, expr);

                    //
                    // Evaluate expression and assing result to field
                    //
                    double value = Parser.parse(expr).evaluate();
                    m_logger.trace("Evaluated \"{}\": {}", expr, value);
                    event.putValue(field, value);

                } else {
                    m_logger.error("Valid assignment missing from expression: {}", expression);
                }
            } else {
                m_logger.error("Field name of assignment missing from expression: {}", expression);
            }
        } catch (Exception e) {
            m_logger.error("Failed to evaluate expression: {}", expression, e);
        }
    }

    public static Modifier create(
            final String id,
            final Variables variables,
            final JsonObject config) {

        Map<String, Object> addFields = new HashMap();
        Map<String, String> renames = new HashMap();
        List<String> addTags = new ArrayList();
        List<String> delFields = new ArrayList();
        List<String> delTags = new ArrayList();

        //
        // add-fields
        //
        {
            JsonObject addFieldDef = config.getObject(CONFIG_FIELD_ADD_FIELDS, new JsonObject());
            Iterator<String> fields = addFieldDef.getFieldNames().iterator();
            while (fields.hasNext()) {
                String name = fields.next();
                Object valueDef = addFieldDef.getValue(name);
                if (valueDef instanceof JsonArray) {
                    JsonArray jsonArray = (JsonArray) valueDef;
                    addFields.put(name, jsonArray.toList());
                } else if (valueDef instanceof String) {
                    addFields.put(name, addFieldDef.getString(name, ""));
                } else {
                    addFields.put(name, addFieldDef.getValue(name));
                }
            }
        }
        //
        // renames
        //
        {
            JsonObject renameDef = config.getObject(CONFIG_FIELD_RENAMES, new JsonObject());
            Iterator<String> fields = renameDef.getFieldNames().iterator();
            while (fields.hasNext()) {
                String name = fields.next();
                renames.put(name, renameDef.getString(name, ""));
            }
        }
        //
        // add-tags
        //
        if (config.containsField(CONFIG_FIELD_ADD_TAGS)) {
            Object addTagDef = config.getValue(CONFIG_FIELD_ADD_TAGS);
            if (addTagDef instanceof String) {
                addTags.add((String) addTagDef);
            } else {
                JsonArray tags = (JsonArray) addTagDef;
                for (int i = 0; i < tags.size(); i++) {
                    addTags.add((String) tags.get(i));
                }
            }
        }

        //
        // del-fields
        //
        if (config.containsField(CONFIG_FIELD_DEL_FIELDS)) {
            Object delFieldDef = config.getValue(CONFIG_FIELD_DEL_FIELDS);
            if (delFieldDef instanceof String) {
                delFields.add((String) delFieldDef);
            } else {
                JsonArray fields = (JsonArray) delFieldDef;
                for (int i = 0; i < fields.size(); i++) {
                    delFields.add((String) fields.get(i));
                }
            }
        }

        //
        // del-tags
        //
        if (config.containsField(CONFIG_FIELD_DEL_TAGS)) {
            Object delTagDef = config.getValue(CONFIG_FIELD_DEL_TAGS);
            if (delTagDef instanceof String) {
                delTags.add((String) delTagDef);
            } else {
                JsonArray tags = (JsonArray) delTagDef;
                for (int i = 0; i < tags.size(); i++) {
                    delTags.add((String) tags.get(i));
                }
            }
        }

        //
        // expression
        //
        String expression = config.getString(CONFIG_FIELD_EXPRESSION, "");

        //
        //  output-address
        //
        String outputAddress = config.getString(CONFIG_FIELD_OUTPUT_ADDRESS, "");

        return new Modifier(
                id,
                outputAddress,
                variables,
                addFields,
                renames,
                addTags,
                delFields,
                delTags,
                expression);
    }
}
