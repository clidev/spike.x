/* $Id: BaseAgent.java 365 2014-02-10 22:47:05Z cli-dev $
 *
 * Copyright (c) 2015 NG Modular Oy.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */
package io.spikex.core.util;

import java.io.Serializable;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;

/**
 * The parsing follows the man page definitions below, with the exception of the
 * day of week field which only accepts values 1-7 instead of 0-7.
 * <p>
 * Field explanations as described in the man page of crontab:
 * <pre>
 *   field        allowed values
 *   -----        --------------
 *   minute       0-59
 *   hour         0-23
 *   day of month 1-31
 *   month        1-12 (or names, see below)
 *   day of week  0-7 (0 or 7 is Sun, or use names)
 *
 *   A field may be an asterisk (*), which always stands for "first-last".
 *
 *   Ranges of numbers are allowed. Ranges are two numbers separated with a
 *   hyphen. The specified range is inclusive. For example, 8-11 for an
 *   "hours" entry specifies execution at hours 8, 9, 10 and 11.
 *
 *   Lists are allowed. A list is a set of numbers (or ranges) separated by
 *   commas. Examples: "1,2,5,9", "0-4,8-12".
 *
 *   Step values can be used in conjunction with ranges. Following a range
 *   with "/number" specifies skips of the number's value through the
 *   range. For example, "0-23/2" can be used in the hours field to specify
 *   command execution every other hour (the alternative in the V7 standard is
 *   "0,2,4,6,8,10,12,14,16,18,20,22"). Steps are also permitted after an
 *   asterisk, so if you want to say "every two hours", just use "*&#047;2".
 *
 *   Names can also be used for the "month" and "day of week" fields. Use
 *   the first three letters of the particular day or month (case does not
 *   matter). Ranges or lists of names are not supported.
 * </pre>
 *
 * Examples:
 * <ul>
 * <li>* * * * *</li>
 * <li>5,10,15-30 *&#047;2 1-31 1-5,Jun,8-12 Mon,TUE,Sun</li>
 * </ul>
 *
 * @author cli
 */
public final class CronEntry implements Serializable {

    private static final Pattern REGEXP_ENTRY = Pattern.compile("([0-9\\w,-/*]+)");
    private static final Pattern REGEXP_FIELD = Pattern.compile("([0-9\\w-/*]+)");
    private static final String[] TAG_MONTHS = {"", "JAN", "FEB", "MAR", "APR",
        "MAY", "JUN", "JUL", "AUG", "SEP", "OCT", "NOV", "DEC"};
    private static final String[] TAG_DOWS = {"", "MON", "TUE", "WED", "THU",
        "FRI", "SAT", "SUN"};
    private static final int[] MINUTES = {0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11,
        12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29,
        30, 31, 32, 33, 34, 35, 36, 37, 38, 39, 40, 41, 42, 43, 44, 45, 46, 47,
        48, 49, 50, 51, 52, 53, 54, 55, 56, 57, 58, 59};
    private boolean m_isEveryMinute;
    private boolean m_isEveryDay;
    private boolean m_isEveryDow;
    private int[] m_months;
    private int[] m_dows;
    private int[] m_days;
    private int[] m_hours;
    private int[] m_minutes;
    private String m_timezone;
    private final String m_def;

    private CronEntry(final String def) {
        // -1 means that any value is acceptable (same as "*)
        m_months = new int[]{-1};
        m_dows = new int[]{-1};
        m_days = new int[]{-1};
        m_hours = new int[]{-1};
        m_minutes = new int[]{-1};
        m_timezone = "UTC";
        m_isEveryMinute = true;
        m_isEveryDay = true;
        m_isEveryDow = true;
        m_def = def; // Store original entry definition for toString() method
    }

    public boolean isDefined(final DateTime tm) {

        boolean defined = false;

        int month = tm.getMonthOfYear();
        int day = tm.getDayOfMonth();
        int dow = tm.getDayOfWeek();
        int hour = tm.getHourOfDay();
        int minute = tm.getMinuteOfHour();

        boolean dayDowMatch = true;

        if (!isEveryDay()) {
            dayDowMatch = isDayDefined(day);
        }

        if (!isEveryDow()) {
            dayDowMatch = isDowDefined(dow);
        }

        if (!isEveryDay() && !isEveryDow()) {
            dayDowMatch = (isDayDefined(day) || isDowDefined(dow));
        }

        if (isMonthDefined(month)
                && dayDowMatch
                && isHourDefined(hour)
                && isMinuteDefined(minute)) {

            defined = true;
        }
//        System.out.println("Defined " + defined + " - month: " + month + " day: " + day + " dow: " + dow
//                +" hour: " + hour + " minute: " + minute + " dayOrDowMatch: " + dayDowMatch);
//        System.out.println("dayDowMatch: " + dayDowMatch + " isMonthDefined: " + isMonthDefined(month) 
//                + " isHourDefined: " + isHourDefined(hour) + " isMinuteDefined: " + isMinuteDefined(minute));
        return defined;
    }

    public boolean isMonthDefined(final int month) {
        return isDefined(m_months, month);
    }

    public boolean isDowDefined(final int dow) {
        return isDefined(m_dows, dow);
    }

    public boolean isDayDefined(final int day) {
        return isDefined(m_days, day);
    }

    public boolean isHourDefined(final int hour) {
        return isDefined(m_hours, hour);
    }

    public boolean isMinuteDefined(final int minute) {
        return isDefined(m_minutes, minute);
    }

    public boolean isEveryDay() {
        return m_isEveryDay;
    }

    public boolean isEveryDow() {
        return m_isEveryDow;
    }

    public boolean isEveryMinute() {
        return m_isEveryMinute;
    }

    public int[] getMonths() {
        return m_months;
    }

    public int[] getDows() {
        return m_dows;
    }

    public int[] getDays() {
        return m_days;
    }

    public int[] getHours() {
        return m_hours;
    }

    public int[] getMinutes() {
        return m_minutes;
    }

    public String getTimezone() {
        return m_timezone;
    }

    public int nextMonth(final int month) {
        return nextDefined(m_months, month);
    }

    public int nextDow(final int dow) {
        return nextDefined(m_dows, dow);
    }

    public int nextDay(final int day) {
        return nextDefined(m_days, day);
    }

    public int nextHour(final int hour) {
        return nextDefined(m_hours, hour);
    }

    public int nextMinute(final int minute) {
        // Minutes are special: always return the next one
        int next = -1;

        // Consider shortcut (optimization)
        int[] values = m_minutes;
        if (values[0] == -1) {
            values = MINUTES; // Every minute
        }
        //System.out.println("===== Values: " + Arrays.toString(values));
        // Find next minute
        for (int i = 0; i < values.length; i++) {
            if (minute < values[i]) {
                next = values[i];
                break;
            }
        }
        // No match => next is the first in the array
        if (next == -1) {
            next = values[0];
        }

        return next;
    }

    @Override
    public String toString() {
        return m_def;
    }

    private boolean isDefined(
            final int[] values,
            final int value) {

        boolean defined = false;
        // Consider shortcut (optimization)
        if (values[0] == -1) {
            defined = true;
        } else {
            for (int val : values) {
                if (val == value) {
                    defined = true;
                    break;
                }
            }
        }
        return defined;
    }

    private int nextDefined(
            final int[] values,
            final int value) {

        int next = -1;

        //System.out.println("===== Values: " + Arrays.toString(values));
        // Consider shortcut (optimization)
        if (values[0] != -1) {
            int prev = -1;
            for (int i = 0; i < values.length; i++) {
                if (value >= prev && value < values[i]) {
                    next = values[i];
                    break;
                }
                prev = values[i];
            }
            // No match => next is the first in the array
            if (next == -1) {
                next = values[0];
            }
        } else {
            next = value; // * => current value
        }

        return next;
    }

    public static CronEntry create(final String def) {
        CronEntry entry = new CronEntry(def);
        Matcher m = REGEXP_ENTRY.matcher(def);
        boolean everyMinute = true;
        //
        // 1. Minute field
        //
        if (!m.find()) {
            throw new IllegalArgumentException("Could not find minute field: "
                    + def);
        }
        entry.m_minutes = parseField(m.group(), 0, 59, entry.m_minutes, null);
        if (entry.m_minutes[0] != -1) {
            everyMinute = false;
        }
        //
        // 2. Hour field
        //
        if (!m.find()) {
            throw new IllegalArgumentException("Could not find hour field: "
                    + def);
        }
        entry.m_hours = parseField(m.group(), 0, 23, entry.m_hours, null);
        if (entry.m_hours[0] != -1) {
            everyMinute = false;
        }
        //
        // 3. Day of month field
        //
        if (!m.find()) {
            throw new IllegalArgumentException("Could not find day of month field: "
                    + def);
        }
        entry.m_days = parseField(m.group(), 1, 31, entry.m_days, null);
        if (entry.m_days[0] != -1) {
            everyMinute = false;
            entry.m_isEveryDay = false;
        }
        //
        // 4. Month field
        //
        if (!m.find()) {
            throw new IllegalArgumentException("Could not find month field: "
                    + def);
        }
        entry.m_months = parseField(m.group(), 1, 12, entry.m_months, TAG_MONTHS);
        if (entry.m_months[0] != -1) {
            everyMinute = false;
        }
        //
        // 5. Day of week field
        //
        if (!m.find()) {
            throw new IllegalArgumentException("Could not find month field: "
                    + def);
        }
        entry.m_dows = parseField(m.group(), 1, 7, entry.m_dows, TAG_DOWS);
        if (entry.m_dows[0] != -1) {
            everyMinute = false;
            entry.m_isEveryDow = false;
        }
        //
        // 6. Timezone
        //
        if (m.find()) {
            String tz = m.group();
            // Sanity check (throws an exception if unknown timezone)
            DateTimeZone.forID(tz);
            entry.m_timezone = tz;
        }

        entry.m_isEveryMinute = everyMinute;
        return entry;
    }

    private static int[] parseField(
            final String field,
            final int min,
            final int max,
            final int[] values,
            final String[] tags) {

        int[] result = values;
        //
        // Sanity checks
        //
        if (field.contains("-") && field.contains("*/")) {
            throw new IllegalArgumentException("Invalid field definition: " + field);
        }
        //
        // Ignore asterisk (default is -1 which represents the asterisk)
        //
        if (!"*".equals(field)) {
            result = new int[0];
            Matcher m = REGEXP_FIELD.matcher(field);
            while (m.find()) {
                String item = m.group();
                result = parseItem(item, min, max, result, tags);
                // -1 ends the loop since it represents any value
                if (result[0] == -1) {
                    break;
                }
            }
            //
            // Sanity check
            //
            if (result.length == 0) {
                throw new IllegalArgumentException("Could not parse "
                        + "crontab field: " + field);
            }
        }
        return result;
    }

    private static int[] parseItem(
            final String item,
            final int min,
            final int max,
            final int[] values,
            final String[] tags) {

        int[] result = null;
        //
        // 1. Step range?
        //
        if (item.contains("/")) {
            result = parseStepRange(item, min, max, values);
        } //
        // 2. Range?
        //
        else if (item.contains("-")) {
            int[] range = getRange(item, min, max);
            //
            // Special case (shortcut for all values in range)
            //
            if (range[0] == min && range[range.length - 1] == max) {
                result = new int[]{-1};
            } else {
                result = addItems(range, values);
            }
        } //
        // 3. One item
        //
        else {
            // Convert tag to value
            int value = -1;
            if (tags != null && Character.isLetter(item.charAt(0))) {
                value = convertToValue(item, tags);
            } else {
                value = Integer.parseInt(item);
            }
            //
            // Sanity check
            //
            if (value < min || value > max) {
                throw new IllegalArgumentException("Single value is too small "
                        + " or too big (min: " + min + " max: " + max + "): "
                        + item);
            }
            result = addItems(new int[]{value}, values);
        }
        return result;
    }

    private static int[] parseStepRange(
            final String item,
            final int min,
            final int max,
            final int[] values) {
        //
        // Consider special case (shortcut for all values in range)
        //
        int[] range = new int[]{-1};
        if (!"*/1".equals(item)) {
            int pos = item.indexOf("/");
            int step = Integer.parseInt(item.substring(pos + 1));
            // Sanity check
            int tot = max - min + 1;
            if (step == 0 || step > tot) {
                throw new IllegalArgumentException("Step value is zero or out"
                        + " of range: " + item);
            }
            int[] rangeFull = getRange(item.substring(0, pos), min, max);
            range = new int[rangeFull.length / step];
            for (int i = 0, j = 0; j < range.length; i += step, j++) {
                range[j] = rangeFull[i];
            }
        }
        return addItems(range, values);
    }

    private static int[] getRange(
            final String item,
            final int min,
            final int max) {

        int[] range = null;
        if ("*".equals(item)) {
            range = new int[max - min + 1];
            for (int i = min, j = 0; i <= max; i++, j++) {
                range[j] = i;
            }
        } else {
            int pos = item.indexOf("-");
            int valMin = Integer.parseInt(item.substring(0, pos));
            int valMax = Integer.parseInt(item.substring(pos + 1));
            //
            // Sanity checks
            //
            if (valMin < min) {
                throw new IllegalArgumentException("Range minimum is too small (min: "
                        + min + "): " + item);
            }
            if (valMax > max) {
                throw new IllegalArgumentException("Range maximum is too big (max: "
                        + max + "): " + item);
            }
            range = new int[valMax - valMin + 1];
            for (int i = valMin, j = 0; i <= valMax; i++, j++) {
                range[j] = i;
            }
        }
        return range;
    }

    private static int[] addItems(
            final int[] items,
            final int[] values) {
        //
        // Do not add duplicates
        //
        int[] uniques = new int[items.length];
        int len = items.length;
        if (values.length == 0) {
            // First iteration (all values are unique)
            System.arraycopy(items, 0, uniques, 0, len);
        } else {
            for (int i = 0, k = 0; i < items.length; i++) {
                boolean duplicate = false;
                for (int j = 0; j < values.length; j++) {
                    if (values[j] == items[i]) {
                        len--;
                        duplicate = true;
                        break;
                    }
                }
                if (!duplicate) {
                    uniques[k++] = items[i];
                }
            }
        }
        //
        // Create new array with extra space and copy original to it
        //
        int[] result = new int[values.length + len];
        System.arraycopy(values, 0, result, 0, values.length);
        //
        // Copy only unique values to the new array
        //
        System.arraycopy(uniques, 0, result, values.length, len);
        return result;
    }

    private static int convertToValue(
            final String item,
            final String[] tags) {

        int value = -1;
        boolean match = false;
        for (int i = 0; i < tags.length; i++) {
            if (tags[i].equalsIgnoreCase(item)) {
                value = i;
                match = true;
                break;
            }
        }
        if (!match) {
            throw new IllegalArgumentException("Invalid tag in field definition: " + item);
        }
        return value;
    }
}
