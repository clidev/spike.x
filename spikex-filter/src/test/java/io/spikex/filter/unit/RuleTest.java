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
package io.spikex.filter.unit;

import junit.framework.Assert;
import io.spikex.core.helper.Events;
import io.spikex.filter.internal.Rule;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.junit.Test;
import org.vertx.java.core.json.JsonArray;
import org.vertx.java.core.json.JsonObject;

/**
 * Rule tester.
 *
 * @author cli
 */
public class RuleTest {

    @Test
    public void testTagRule() {
        // -------------- Rule r1 ---------------
        JsonObject config1 = new JsonObject();
        config1.putString("match-tag", "blue");
        config1.putString("schedule", "*");
        config1.putString("modifier", "*");

        JsonObject event1 = new JsonObject();
        event1.putArray(Events.EVENT_FIELD_TAGS,
                new JsonArray("[\"red\",\"blue\"]"));

        Rule r1 = Rule.create("r1", config1);
        Assert.assertTrue("Rule r1 did not match event", r1.match(event1));

        // -------------- Rule r2 ---------------
        JsonObject event2 = new JsonObject();
        event2.putArray(Events.EVENT_FIELD_TAGS,
                new JsonArray("[\"yellow\",\"green\"]"));

        Rule r2 = Rule.create("r2", config1);
        Assert.assertFalse("Rule r2 did match event - event though no tags match",
                r2.match(event2));

        // -------------- Rule r3 ---------------
        JsonObject event3 = new JsonObject();
        event3.putString("no-tags", "must fail");

        Rule r3 = Rule.create("r3", config1);
        Assert.assertFalse("Rule r3 did match event - event though no tags available",
                r3.match(event3));
    }

    @Test
    public void testTagAndFieldRule() {

        // -------------- Rule r1 ---------------
        JsonObject config1 = new JsonObject();
        config1.putString("match-tag", "blue");
        config1.putString("match-field", "@test");
        config1.putString("value-equals", "What we think, we become.");
        config1.putString("schedule", "*");
        config1.putString("modifier", "*");

        JsonObject event1 = new JsonObject();
        event1.putArray(Events.EVENT_FIELD_TAGS,
                new JsonArray("[\"red\",\"blue\"]"));
        event1.putString("@test", "What we think, we become.");

        Rule r1 = Rule.create("r1", config1);
        Assert.assertTrue("Rule r1 did not match event", r1.match(event1));

        // -------------- Rule r2 ---------------
        JsonObject event2 = new JsonObject();
        event2.putArray(Events.EVENT_FIELD_TAGS,
                new JsonArray("[\"yellow\",\"green\",\"blue\"]"));
        event2.putString("@test", "");

        Rule r2 = Rule.create("r2", config1);
        Assert.assertFalse("Rule r2 did match event - event though no tags match",
                r2.match(event2));

        // -------------- Rule r3 ---------------
        JsonObject event3 = new JsonObject();
        event3.putArray(Events.EVENT_FIELD_TAGS,
                new JsonArray("[\"blue\"]"));
        event3.putString("no-field", "must fail");

        Rule r3 = Rule.create("r3", config1);
        Assert.assertFalse("Rule r3 did match event - event though no field available",
                r3.match(event3));

        // -------------- Rule r4 ---------------
        JsonObject event4 = new JsonObject();
        event4.putArray(Events.EVENT_FIELD_TAGS,
                new JsonArray("[\"blue\"]"));
        event4.putString("@test", null);

        Rule r4 = Rule.create("r4", config1);
        Assert.assertFalse("Rule r4 did match event - event though field is null",
                r4.match(event4));
    }

    @Test
    public void testEqualsStringRule() {

        // -------------- Rule r1 ---------------
        JsonObject config1 = new JsonObject();
        config1.putString("match-field", "@test");
        config1.putString("value-equals", "What we think, we become.");
        config1.putString("schedule", "*");
        config1.putString("modifier", "*");

        JsonObject event1 = new JsonObject();
        event1.putString("@test", "What we think, we become.");

        Rule r1 = Rule.create("r1", config1);
        Assert.assertTrue("Rule r1 did not match event", r1.match(event1));

        // -------------- Rule r2 ---------------
        JsonObject event2 = new JsonObject();
        event2.putString("@testos", "What we think, we become.");

        Rule r2 = Rule.create("r2", config1);
        Assert.assertFalse("Rule r2 did match event - even though field @test is missing",
                r2.match(event2));

        // -------------- Rule r3 ---------------
        JsonObject event3 = new JsonObject();
        JsonArray array = new JsonArray();
        array.addString("What we think, we become.");
        array.addString("A never ending journey.");
        event3.putArray("@test", array);

        Rule r3 = Rule.create("r3", config1);
        Assert.assertFalse("Rule r3 did match event - even though field @test contains a mismatch",
                r3.match(event3));

        // -------------- Rule r4 ---------------
        JsonObject event4 = new JsonObject();
        event4.putNumber("@test", 23899283);

        Rule r4 = Rule.create("r4", config1);
        Assert.assertFalse("Rule r4 did match event - even though field @test contains a number",
                r4.match(event4));

        // -------------- Rule r5 ---------------
        JsonObject event5 = new JsonObject();
        JsonObject objValue = new JsonObject();
        objValue.putString("@test", "What we think, we become.");
        event5.putObject("@test", objValue);

        Rule r5 = Rule.create("r5", config1);
        Assert.assertFalse("Rule r5 did match event - even though field @test contains a JsonObject",
                r5.match(event5));
    }

    @Test
    public void testEqualsNumberRule() {

        // -------------- Rule r1 ---------------
        JsonObject config1 = new JsonObject();
        config1.putString("match-field", "@cpu-load");
        config1.putNumber("value-equals", 23.01223d);
        config1.putString("schedule", "*");
        config1.putString("throttle", "*");

        JsonObject event1 = new JsonObject();
        event1.putString("@cpu-load", "23.01223");

        Rule r1 = Rule.create("r1", config1);
        Assert.assertTrue("Rule r1 did not match event", r1.match(event1));

        // -------------- Rule r2 ---------------
        JsonObject event2 = new JsonObject();
        event2.putNumber("@cpu-load", 23.01223);

        Rule r2 = Rule.create("r2", config1);
        Assert.assertTrue("Rule r2 did not match event", r2.match(event2));

        // -------------- Rule r3 ---------------
        JsonObject config2 = new JsonObject();
        config2.putString("match-field", "@cpu-load");
        config2.putString("value-equals", "0.02382111");
        config2.putString("schedule", "*");
        config2.putString("modifier", "*");

        JsonObject event3 = new JsonObject();
        event3.putNumber("@cpu-load", 0.02382111d);

        Rule r3 = Rule.create("r3", config2);
        Assert.assertTrue("Rule r3 did not match event", r3.match(event3));

        // -------------- Rule r4 ---------------
        JsonObject event4 = new JsonObject();
        event4.putString("@cpu-load", "0.02382111");

        Rule r4 = Rule.create("r4", config2);
        Assert.assertTrue("Rule r4 did not match event", r4.match(event4));

        // -------------- Rule r5 ---------------
        JsonObject event5 = new JsonObject();
        JsonArray array = new JsonArray();
        array.addNumber(0.02382111);
        array.addNumber(0.02382111);
        event5.putArray("@cpu-load", array);

        Rule r5 = Rule.create("r5", config2);
        Assert.assertTrue("Rule r5 did not match event", r5.match(event5));

        // -------------- Rule r6 ---------------
        JsonObject event6 = new JsonObject();
        array = new JsonArray();
        array.addNumber(0.02382111);
        array.addNumber(28923);
        event6.putArray("@cpu-load", array);

        Rule r6 = Rule.create("r6", config2);
        Assert.assertFalse("Rule r6 did match event - even though field @cpu-load contains a mismatch",
                r6.match(event6));
    }

    @Test
    public void testInValuesRule() {

        // -------------- Rule r1 ---------------
        JsonObject config1 = new JsonObject();
        JsonArray values = new JsonArray();
        values.addString("apple");
        values.addString("orange");
        values.addString("peach");
        config1.putString("match-field", "@fruit");
        config1.putArray("value-in", values);
        config1.putString("schedule", "*");
        config1.putString("modifier", "*");

        JsonObject event1 = new JsonObject();
        event1.putString("@fruit", "peach");

        Rule r1 = Rule.create("r1", config1);
        Assert.assertTrue("Rule r1 did not match event", r1.match(event1));

        // -------------- Rule r2 ---------------
        JsonObject event2 = new JsonObject();
        event2.putString("@fruit", "melon");

        Rule r2 = Rule.create("r2", config1);
        Assert.assertFalse("Rule r2 did match event - even though @fruit contains no match",
                r2.match(event2));

        // -------------- Rule r3 ---------------
        values = new JsonArray();
        values.addNumber(89378872);
        values.addString("orange");
        values.addString("0.09");
        values.addBoolean(Boolean.FALSE);
        values.addObject(new JsonObject());
        values.addArray(new JsonArray());
        values.addNumber(700700);
        config1.putArray("value-in", values);

        JsonObject event3 = new JsonObject();
        event3.putNumber("@fruit", 700700);

        Rule r3 = Rule.create("r3", config1);
        Assert.assertTrue("Rule r3 did not match event", r3.match(event3));
    }

    @Test
    public void testNotInValuesRule() {

        // -------------- Rule r1 ---------------
        JsonObject config1 = new JsonObject();
        JsonArray values = new JsonArray();
        values.addString("steel");
        values.addString("mercury");
        values.addNumber(0.23);
        values.addString("iron");
        config1.putString("match-field", "@material");
        config1.putArray("value-not-in", values);
        config1.putString("schedule", "*");
        config1.putString("modifier", "*");

        JsonObject event1 = new JsonObject();
        event1.putString("@material", "aluminium");

        Rule r1 = Rule.create("r1", config1);
        Assert.assertTrue("Rule r1 did not match event", r1.match(event1));

        // -------------- Rule r2 ---------------
        JsonObject event2 = new JsonObject();
        event2.putNumber("@material", 0.23);

        Rule r2 = Rule.create("r2", config1);
        Assert.assertFalse("Rule r2 did match event - even though @material contains a match",
                r2.match(event2));

        // -------------- Rule r3 ---------------
        JsonObject event3 = new JsonObject();
        event3.putString("@material", "steel");

        Rule r3 = Rule.create("r3", config1);
        Assert.assertFalse("Rule r3 did match event - even though @material contains a match",
                r3.match(event3));
    }

    @Test
    public void testLessThanRule() {

        // -------------- Rule r1 ---------------
        JsonObject config1 = new JsonObject();
        config1.putString("match-field", "@mem");
        config1.putNumber("value-lt", 10);
        config1.putString("schedule", "*");
        config1.putString("modifier", "*");

        JsonObject event1 = new JsonObject();
        event1.putNumber("@mem", 9.9999);

        Rule r1 = Rule.create("r1", config1);
        Assert.assertTrue("Rule r1 did not match event", r1.match(event1));

        // -------------- Rule r2 ---------------
        JsonObject event2 = new JsonObject();
        event2.putNumber("@mem", 10);

        Rule r2 = Rule.create("r2", config1);
        Assert.assertFalse("Rule r2 did match event - even though field @mem is equal to value-lt",
                r2.match(event2));

        // -------------- Rule r3 ---------------
        JsonObject event3 = new JsonObject();
        event3.putNumber("@mem", 10.1);

        Rule r3 = Rule.create("r3", config1);
        Assert.assertFalse("Rule r2 did match event - even though field @mem is larger than value-lt",
                r3.match(event3));

        // -------------- Rule r4 ---------------
        JsonObject event4 = new JsonObject();
        event4.putString("@mem", "7");

        Rule r4 = Rule.create("r4", config1);
        Assert.assertTrue("Rule r4 did not match event", r4.match(event4));

        // -------------- Rule r5 ---------------
        JsonObject event5 = new JsonObject();
        event5.putString("@mem", "Out of memory");

        try {
            Rule r5 = Rule.create("r5", config1);
            Assert.assertFalse("Rule r5 did match event - even though field @mem contains a string",
                    r5.match(event5));
        } catch (NumberFormatException e) {
            // OK
        }
    }

    @Test
    public void testLessThanOrEqualRule() {

        // -------------- Rule r1 ---------------
        JsonObject config1 = new JsonObject();
        config1.putString("match-field", "@mem");
        config1.putNumber("value-lte", 10);
        config1.putString("schedule", "*");
        config1.putString("modifier", "*");

        JsonObject event1 = new JsonObject();
        event1.putNumber("@mem", 10.0);

        Rule r1 = Rule.create("r1", config1);
        Assert.assertTrue("Rule r1 did not match event", r1.match(event1));

        // -------------- Rule r2 ---------------
        JsonObject event2 = new JsonObject();
        event2.putNumber("@mem", 10.1);

        Rule r2 = Rule.create("r2", config1);
        Assert.assertFalse("Rule r2 did match event - even though field @mem is larger than value-lte",
                r2.match(event2));

        // -------------- Rule r3 ---------------
        JsonObject event3 = new JsonObject();
        event3.putString("@mem", "9.9999999999999998");

        Rule r3 = Rule.create("r3", config1);
        Assert.assertTrue("Rule r3 did not match event", r3.match(event3));
    }

    @Test
    public void testGreaterThanRule() {

        // -------------- Rule r1 ---------------
        JsonObject config1 = new JsonObject();
        config1.putString("match-field", "@temp");
        config1.putString("value-gt", "-8.2341");
        config1.putString("schedule", "*");
        config1.putString("modifier", "*");

        JsonObject event1 = new JsonObject();
        event1.putNumber("@temp", -7.239f);

        Rule r1 = Rule.create("r1", config1);
        Assert.assertTrue("Rule r1 did not match event", r1.match(event1));

        // -------------- Rule r2 ---------------
        JsonObject event2 = new JsonObject();
        event2.putNumber("@temp", -8.2341);

        Rule r2 = Rule.create("r2", config1);
        Assert.assertFalse("Rule r2 did match event - even though field @temp is equal to value-gt",
                r2.match(event2));

        // -------------- Rule r3 ---------------
        JsonObject event3 = new JsonObject();
        event3.putString("@temp", "23.234");

        Rule r3 = Rule.create("r3", config1);
        Assert.assertTrue("Rule r3 did not match event", r3.match(event3));
    }

    @Test
    public void testGreaterThanOrEqualRule() {

        // -------------- Rule r1 ---------------
        JsonObject config1 = new JsonObject();
        config1.putString("match-field", "@io");
        config1.putString("value-gte", "3228932");
        config1.putString("schedule", "*");
        config1.putString("modifier", "*");

        JsonObject event1 = new JsonObject();
        event1.putNumber("@io", 3228932);

        Rule r1 = Rule.create("r1", config1);
        Assert.assertTrue("Rule r1 did not match event", r1.match(event1));

        // -------------- Rule r2 ---------------
        JsonObject event2 = new JsonObject();
        event2.putString("@io", "1228");

        Rule r2 = Rule.create("r2", config1);
        Assert.assertFalse("Rule r2 did match event - even though field @io is less than value-gte",
                r2.match(event2));

        // -------------- Rule r3 ---------------
        JsonObject event3 = new JsonObject();
        event3.putNumber("@io", 1200020321);

        Rule r3 = Rule.create("r3", config1);
        Assert.assertTrue("Rule r3 did not match event", r3.match(event3));
    }

    @Test
    public void testContainsRule() {

        // -------------- Rule r1 ---------------
        JsonObject config1 = new JsonObject();
        config1.putString("match-field", "@value");
        config1.putString("value-contains", "ABBA");
        config1.putString("schedule", "*");
        config1.putString("modifier", "*");
        JsonObject event1 = new JsonObject();
        event1.putString("@value", "12343657 9823 ABBA 17712");

        Rule r1 = Rule.create("r1", config1);
        Assert.assertTrue("Rule r1 did not match event", r1.match(event1));

        // -------------- Rule r2 ---------------
        config1.putString("value-contains", "12.3");
        JsonObject event2 = new JsonObject();
        event2.putNumber("@value", 12.30);
        Rule r2 = Rule.create("r2", config1);
        Assert.assertTrue("Rule r2 did not match event", r2.match(event2));
    }

    @Test
    public void testDateGreaterThanRule() {

        // -------------- Rule r1 ---------------
        String fmt = "EEE MMM dd HH:mm:ss yyyy";
        JsonObject config1 = new JsonObject();
        config1.putString("match-field", "@submitted");
        config1.putString("date-lt", "#now(UTC,0h,-10m,0s)");
        config1.putString("date-fmt", fmt);
        config1.putString("schedule", "*");
        config1.putString("modifier", "*");
        JsonObject event1 = new JsonObject();

        DateTime now = new DateTime(DateTimeZone.UTC).minusMinutes(11);
        DateTimeFormatter formatter = DateTimeFormat.forPattern(fmt);
        event1.putString("@submitted", formatter.print(now));

        Rule r1 = Rule.create("r1", config1);
        Assert.assertTrue("Rule r1 did not match event", r1.match(event1));
    }
}
