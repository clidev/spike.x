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

import com.google.common.base.Preconditions;
import io.spikex.core.helper.Events;
import io.spikex.core.helper.Variables;
import io.spikex.core.util.Numbers;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.regex.Pattern;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.ISODateTimeFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.vertx.java.core.json.JsonArray;
import org.vertx.java.core.json.JsonObject;

/**
 *
 * @author cli
 */
public final class Rule {

    private final String m_id;
    private final String m_field;
    private final String m_tag;
    private final String m_schedule;
    private final String m_action; // throttle or modifier
    private final String m_expression; // Parsii expression
    private final Object m_fmt; // format of event value
    private final int m_constraint;

    private final Object m_value;
    private final List<? extends Object> m_values;

    private static final String CONFIG_FIELD_MATCH_FIELD = "match-field";
    private static final String CONFIG_FIELD_MATCH_TAG = "match-tag";
    private static final String CONFIG_FIELD_SCHEDULE = "schedule";
    private static final String CONFIG_FIELD_THROTTLE = "throttle";
    private static final String CONFIG_FIELD_MODIFIER = "modifier";
    private static final String CONFIG_FIELD_VARIABLES = "variables";
    private static final String CONFIG_FIELD_EXPRESSION = "expression";
    private static final String CONFIG_FIELD_VALUE_EQUALS = "value-equals";
    private static final String CONFIG_FIELD_VALUE_CONTAINS = "value-contains";
    private static final String CONFIG_FIELD_VALUE_IN = "value-in"; // Array
    private static final String CONFIG_FIELD_VALUE_NOT_IN = "value-not-in"; // Array
    private static final String CONFIG_FIELD_VALUE_LT = "value-lt"; // Less than
    private static final String CONFIG_FIELD_VALUE_LTE = "value-lte"; // Less than or equal to
    private static final String CONFIG_FIELD_VALUE_GT = "value-gt"; // Greater than
    private static final String CONFIG_FIELD_VALUE_GTE = "value-gte"; // Greater than or equal to
    private static final String CONFIG_FIELD_DATE_LT = "date-lt"; // Less than
    private static final String CONFIG_FIELD_DATE_LTE = "date-lte"; // Less than or equal to
    private static final String CONFIG_FIELD_DATE_GT = "date-gt"; // Greater than
    private static final String CONFIG_FIELD_DATE_GTE = "date-gte"; // Greater than or equal to
    private static final String CONFIG_FIELD_DATE_FMT = "date-fmt"; // Date format

    private static final String DEF_DATE_FMT = "yyyy-MM-dd'T'HH:mm:ssZZ";

    // Built-ins
    private static final String BUILTIN_NOW = "#now";
    private static final Pattern REGEXP_NOW
            = Pattern.compile("#now[(]?"
                    + "([A-Z][0-9\\\\w\\\\-\\\\+_/]+)?,?" // Timezone
                    + "([\\\\+\\\\-]?[0-9]+h)?,?" // Hours
                    + "([\\\\+\\\\-]?[0-9]+m)?,?" // Minutes
                    + "([\\\\+\\\\-]?[0-9]+s)?" // Seconds
                    + "[)]");

    public static final int CONSTRAINT_EQUALS = 100;
    public static final int CONSTRAINT_CONTAINS = 101; // String only
    public static final int CONSTRAINT_IN = 102;
    public static final int CONSTRAINT_NOT_IN = 103;
    public static final int CONSTRAINT_LT = 104;
    public static final int CONSTRAINT_LTE = 105;
    public static final int CONSTRAINT_GT = 106;
    public static final int CONSTRAINT_GTE = 107;

    private static final Logger m_logger = LoggerFactory.getLogger(Rule.class);

    private Rule(
            final String id,
            final String field,
            final String tag,
            final String schedule,
            final String action,
            final String expression,
            final Object fmt,
            final int constraint,
            final Object value,
            final List<? extends Object> values) {

        // Sanity checks
        Preconditions.checkArgument(id != null && id.length() > 0,
                "id is null or empty");
        Preconditions.checkArgument(action != null && action.length() > 0,
                "action is null or empty");
        Preconditions.checkArgument(schedule != null && schedule.length() > 0,
                "schedule is null or empty");

        m_id = id;
        m_field = field;
        m_tag = tag;
        m_schedule = schedule;
        m_action = action;
        m_expression = expression;
        m_fmt = fmt;
        m_constraint = constraint;
        m_value = value;
        m_values = values;
    }

    @Override
    public int hashCode() {
        int hash = 5;
        hash = 59 * hash + Objects.hashCode(m_id);
        hash = 59 * hash + Objects.hashCode(m_field);
        hash = 59 * hash + Objects.hashCode(m_tag);
        hash = 59 * hash + Objects.hashCode(m_value);
        hash = 59 * hash + Objects.hashCode(m_values);
        return hash;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        final Rule other = (Rule) obj;
        if (!Objects.equals(m_id, other.m_id)) {
            return false;
        }
        if (!Objects.equals(m_field, other.m_field)) {
            return false;
        }
        if (!Objects.equals(m_tag, other.m_tag)) {
            return false;
        }
        if (!Objects.equals(m_value, other.m_value)) {
            return false;
        }
        if (!Objects.equals(m_values, other.m_values)) {
            return false;
        }
        return true;
    }

    public String getId() {
        return m_id;
    }

    public String getField() {
        return m_field;
    }

    public String getTag() {
        return m_tag;
    }

    public String getSchedule() {
        return m_schedule;
    }

    public String getAction() {
        return m_action;
    }

    public Object getFormat() {
        return m_fmt;
    }

    public String getExpression() {
        return m_expression;
    }

    public boolean match(final JsonObject event) {
        boolean match = true;
        boolean matchTag = isMatchTag();
        boolean matchField = isMatchField();
        //
        // Matching tag and field (and value)?
        //
        if (matchTag && matchField) {

            match = matchesTag(event)
                    && matchesFieldAndValue(event);

        } // Matching tag only?
        else if (matchTag) {

            match = matchesTag(event);

        } // Matching field and value only?
        else if (matchField) {

            match = matchesFieldAndValue(event);
        }
        return match;
    }

    @Override
    public String toString() {
        String sname = getClass().getSimpleName();
        StringBuilder sb = new StringBuilder(sname);
        sb.append("[");
        sb.append(hashCode());
        sb.append("] id: ");
        sb.append(getId());
        sb.append(" field: ");
        sb.append(getField());
        sb.append(" tag: ");
        sb.append(getTag());
        sb.append(" schedule: ");
        sb.append(getSchedule());
        sb.append(" action: ");
        sb.append(getAction());
        sb.append(" format: ");
        sb.append(getFormat());
        sb.append(" constraint: ");
        sb.append(m_constraint);
        sb.append(" value: ");
        sb.append(m_value);
        sb.append(" values: ");
        sb.append(m_values);
        return sb.toString();
    }

    private boolean matchesTag(final JsonObject event) {

        boolean match = false;
        JsonArray tags = event.getArray(Events.EVENT_FIELD_TAGS);
        if (tags != null && tags.contains(getTag())) {
            match = true;
        }
        return match;
    }

    private boolean matchesFieldAndValue(final JsonObject event) {

        boolean match = false;

        String field = getField();
        if (field.length() > 0) {

            Object value = null;
            int pos = field.indexOf('/'); // Sorry, only one level of nesting...
            if (pos >= 0) {

                String field1 = field.substring(0, pos);
                String field2 = field.substring(pos + 1);

                if (event.containsField(field1)) {
                    JsonObject map = event.getObject(field1);
                    if (map != null
                            && map.containsField(field2)) {
                        value = map.getValue(field2);
                    }
                }

            } else {
                if (event.containsField(field)) {
                    value = event.getValue(field);
                }
            }

            if (value != null) {
                match = matchValue(value);
            }
        }
        return match;
    }

    private boolean matchValue(final Object eventValue) {
        boolean match = true;
        //
        // Handle array and single value
        //
        if (eventValue instanceof JsonArray) {
            JsonArray array = (JsonArray) eventValue;
            for (int i = 0; i < array.size(); i++) {
                Object singleValue = array.get(i);
                if (!matchEventValue(singleValue)) {
                    match = false;
                    break;
                }
            }
        } else {
            // Single value
            match = matchEventValue(eventValue);
        }
        return match;
    }

    private boolean isMatchField() {
        return (m_field.length() > 0);
    }

    private boolean isMatchTag() {
        return (m_tag.length() > 0);
    }

    private boolean matchEventValue(final Object eventValue) {
        boolean match = false;
        Object value = m_value;
        switch (m_constraint) {

            // String, date numerical
            case CONSTRAINT_EQUALS: {
                if (value != null) {
                    match = (compareToValue(value, eventValue) == 0);
                } else {
                    match = true;
                    List<? extends Object> values = m_values;
                    for (Object val : values) {
                        if (compareToValue(val, eventValue) != 0) {
                            match = false;
                            break;
                        }
                    }
                }
                break;
            }

            // String (array)
            case CONSTRAINT_CONTAINS: {
                match = true;
                List<? extends Object> values = m_values;
                for (Object val : values) {
                    if (!containsString((String) val, eventValue)) {
                        match = false;
                        break;
                    }
                }
                break;
            }

            // String or numerical
            case CONSTRAINT_NOT_IN:
                match = true;
            case CONSTRAINT_IN: {
                List<? extends Object> values = m_values;
                for (Object val : values) {
                    String strValue;
                    if (val instanceof String) {
                        strValue = (String) val;
                    } else {
                        strValue = String.valueOf(val);
                    }
                    if (compareToString(strValue, eventValue) == 0) {
                        match = !match;
                        break;
                    }
                }
                break;
            }

            // Numerical, date or string
            case CONSTRAINT_LT: {
                match = (compareToValue(value, eventValue) > 0);
                break;
            }

            // Numerical, date or string
            case CONSTRAINT_LTE: {
                match = (compareToValue(value, eventValue) >= 0);
                break;
            }

            // Numerical, date or string
            case CONSTRAINT_GT: {
                int diff = compareToValue(value, eventValue);
                match = (diff < 0 && diff != Integer.MIN_VALUE);
                break;
            }

            // Numerical, date or string
            case CONSTRAINT_GTE: {
                int diff = compareToValue(value, eventValue);
                match = (diff <= 0 && diff != Integer.MIN_VALUE);
                break;
            }
        }
        return match;
    }

    private boolean containsString(
            final String str,
            final Object obj) {

        boolean contains = false;
        if (obj != null) {

            String value;

            if (obj instanceof String) {
                value = (String) obj;
            } else {
                value = String.valueOf(obj);
            }
            contains = value.contains(str);
        }
        return contains;
    }

    private int compareToValue(
            final Object value,
            final Object obj) {
        if (value instanceof DateTime) {
            // DateTime
            return compareToDateTime((DateTime) value, obj);
        } else if (value instanceof BigDecimal) {
            // BigDecimal
            return compareToNumber((BigDecimal) value, obj);
        } else {
            return compareToString(String.valueOf(value), obj);
        }
    }

    private int compareToString(
            final String str,
            final Object obj) {

        if (obj instanceof String) {
            return str.compareTo((String) obj);
        } else {
            return str.compareTo(String.valueOf(obj));
        }
    }

    private int compareToNumber(
            final BigDecimal value,
            final Object obj) {

        int diff = Integer.MIN_VALUE;
        if (obj instanceof Number) {
            diff = compareToBigDecimal(value, obj);
        } else {
            String strValue = String.valueOf(obj);
            if (Numbers.isDecimal(strValue)
                    || Numbers.isInteger(strValue)) {
                diff = compareToBigDecimal(value, obj);
            }
        }
        return diff;
    }

    private int compareToBigDecimal(
            final BigDecimal n1,
            final Object obj) {

        String n2;
        if (obj instanceof String) {
            n2 = (String) obj;
        } else {
            n2 = String.valueOf(obj);
        }
        return n1.compareTo(new BigDecimal(n2.toString()));
    }

    private int compareToDateTime(
            final DateTime d1,
            final Object obj) {

        DateTimeFormatter fmt = (DateTimeFormatter) m_fmt;
        String dateStr = String.valueOf(obj);
        DateTime d2 = fmt.parseDateTime(dateStr);
        // https://github.com/JodaOrg/joda-time/issues/73
        return d1.toLocalDateTime().compareTo(d2.toLocalDateTime());
    }

    public static Rule create(
            final String id,
            final JsonObject config) {

        int constraint = -1;
        Object value = null;
        List values = new ArrayList();
        Object fmt = "";

        if (config.containsField(CONFIG_FIELD_VALUE_EQUALS)) {
            constraint = CONSTRAINT_EQUALS;
            value = createBigDecimal(config, CONFIG_FIELD_VALUE_EQUALS);
            if (value == null) {
                values.add(config.getString(CONFIG_FIELD_VALUE_EQUALS));
            }
        } else if (config.containsField(CONFIG_FIELD_VALUE_CONTAINS)) {
            constraint = CONSTRAINT_CONTAINS;
            Object strings = config.getValue(CONFIG_FIELD_VALUE_CONTAINS);
            if (!(strings instanceof JsonArray)) {
                strings = new JsonArray().add(strings);
            }
            int len = ((JsonArray) strings).size();
            for (int i = 0; i < len; i++) {
                String str = String.valueOf(((JsonArray) strings).get(i));
                Preconditions.checkArgument(str != null && str.length() > 0,
                        CONFIG_FIELD_VALUE_CONTAINS + " must not be null or empty");
                values.add(str);
            }
        } else if (config.containsField(CONFIG_FIELD_VALUE_IN)) {
            constraint = CONSTRAINT_IN;
            JsonArray array = config.getArray(CONFIG_FIELD_VALUE_IN);
            values = array.toList();

        } else if (config.containsField(CONFIG_FIELD_VALUE_NOT_IN)) {
            constraint = CONSTRAINT_NOT_IN;
            JsonArray array = config.getArray(CONFIG_FIELD_VALUE_NOT_IN);
            values = array.toList();

        } else if (config.containsField(CONFIG_FIELD_VALUE_LT)) {
            constraint = CONSTRAINT_LT;
            value = createBigDecimal(config, CONFIG_FIELD_VALUE_LT);
            Preconditions.checkNotNull(value,
                    "Non-numeric value defined for: " + CONFIG_FIELD_VALUE_LT);

        } else if (config.containsField(CONFIG_FIELD_VALUE_LTE)) {
            constraint = CONSTRAINT_LTE;
            value = createBigDecimal(config, CONFIG_FIELD_VALUE_LTE);
            Preconditions.checkNotNull(value,
                    "Non-numeric value defined for: " + CONFIG_FIELD_VALUE_LTE);

        } else if (config.containsField(CONFIG_FIELD_VALUE_GT)) {
            constraint = CONSTRAINT_GT;
            value = createBigDecimal(config, CONFIG_FIELD_VALUE_GT);
            Preconditions.checkNotNull(value,
                    "Non-numeric value defined for: " + CONFIG_FIELD_VALUE_GT);

        } else if (config.containsField(CONFIG_FIELD_VALUE_GTE)) {
            constraint = CONSTRAINT_GTE;
            value = createBigDecimal(config, CONFIG_FIELD_VALUE_GTE);
            Preconditions.checkNotNull(value,
                    "Non-numeric value defined for: " + CONFIG_FIELD_VALUE_GTE);

        } else if (config.containsField(CONFIG_FIELD_DATE_LT)) {
            constraint = CONSTRAINT_LT;
            value = createDateTime(config, CONFIG_FIELD_DATE_LT);
            fmt = DateTimeFormat.forPattern(
                    config.getString(CONFIG_FIELD_DATE_FMT, DEF_DATE_FMT));

        } else if (config.containsField(CONFIG_FIELD_DATE_LTE)) {
            constraint = CONSTRAINT_LTE;
            value = createDateTime(config, CONFIG_FIELD_DATE_LTE);
            fmt = DateTimeFormat.forPattern(
                    config.getString(CONFIG_FIELD_DATE_FMT, DEF_DATE_FMT));

        } else if (config.containsField(CONFIG_FIELD_DATE_GT)) {
            constraint = CONSTRAINT_GT;
            value = createDateTime(config, CONFIG_FIELD_DATE_GT);
            fmt = DateTimeFormat.forPattern(
                    config.getString(CONFIG_FIELD_DATE_FMT, DEF_DATE_FMT));

        } else if (config.containsField(CONFIG_FIELD_DATE_GTE)) {
            constraint = CONSTRAINT_GTE;
            value = createDateTime(config, CONFIG_FIELD_DATE_GTE);
            fmt = DateTimeFormat.forPattern(
                    config.getString(CONFIG_FIELD_DATE_FMT, DEF_DATE_FMT));
        }

        String action = "";
        if (config.containsField(CONFIG_FIELD_THROTTLE)) {
            action = config.getString(CONFIG_FIELD_THROTTLE);
        } else if (config.containsField(CONFIG_FIELD_MODIFIER)) {
            action = config.getString(CONFIG_FIELD_MODIFIER);
        }

        String expression = config.getString(CONFIG_FIELD_EXPRESSION);

        return new Rule(
                id,
                config.getString(CONFIG_FIELD_MATCH_FIELD, ""),
                config.getString(CONFIG_FIELD_MATCH_TAG, ""),
                config.getString(CONFIG_FIELD_SCHEDULE, "* * * *"),
                action,
                expression,
                fmt,
                constraint,
                value,
                values);
    }

    private static BigDecimal createBigDecimal(
            final JsonObject config,
            final String field) {

        BigDecimal bd = null;
        Object numValue = config.getValue(field);
        if (numValue instanceof Number) {
            bd = new BigDecimal(String.valueOf(numValue));
        } else {
            String strValue = config.getString(field);
            if (Numbers.isDecimal(strValue)
                    || Numbers.isInteger(strValue)) {
                bd = new BigDecimal(strValue);
            }
        }
        return bd;
    }

    private static DateTime createDateTime(
            final JsonObject config,
            final String field) {

        DateTime dt;
        String dateStr = config.getString(field);
        m_logger.trace("date string: {}", dateStr);

        if (dateStr != null && dateStr.startsWith(BUILTIN_NOW)) {
            dt = Variables.createDateTimeNow(dateStr);
        } else {
            DateTimeFormatter fmt = ISODateTimeFormat.dateTime();
            dt = fmt.parseDateTime(dateStr);
        }

        return dt;
    }
}
