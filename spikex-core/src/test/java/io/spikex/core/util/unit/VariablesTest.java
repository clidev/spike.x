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
package io.spikex.core.util.unit;

import static io.spikex.core.AbstractFilter.CONF_KEY_CHAIN_NAME;
import static io.spikex.core.AbstractVerticle.CONF_KEY_CLUSTER_NAME;
import static io.spikex.core.AbstractVerticle.CONF_KEY_CONF_PATH;
import static io.spikex.core.AbstractVerticle.CONF_KEY_DATA_PATH;
import static io.spikex.core.AbstractVerticle.CONF_KEY_HOME_PATH;
import static io.spikex.core.AbstractVerticle.CONF_KEY_NODE_NAME;
import static io.spikex.core.AbstractVerticle.CONF_KEY_TMP_PATH;
import io.spikex.core.helper.Variables;
import java.util.Map;
import junit.framework.Assert;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.junit.Test;
import org.vertx.java.core.json.JsonObject;

/**
 * Variables tester.
 * <p>
 * @author cli
 */
public class VariablesTest {

    @Test
    public void testPrimitives() {
        // byte, short, int, long, float, double, boolean, char, string
        Variables vars = createVariables();

        Byte b1 = Byte.decode("0x1F");
        Byte b2 = Byte.decode("0x08");

        Byte r1 = vars.translate(b1);
        Byte r2 = vars.translate(b2);

        Assert.assertEquals(b1, r1);
        Assert.assertEquals(b2, r2);

        Integer i3 = 1001;
        Integer i4 = 0;

        Integer r3 = vars.translate(i3);
        Integer r4 = vars.translate(i4);

        Assert.assertEquals(i3, r3);
        Assert.assertEquals(i4, r4);

        Double d5 = -323.24313245456;
        Double d6 = 0.0;

        Double r5 = vars.translate(d5);
        Double r6 = vars.translate(d6);

        Assert.assertEquals(d5, r5);
        Assert.assertEquals(d6, r6);

        Long l7 = -1001L;
        Long l8 = 0L;

        Long r7 = vars.translate(l7);
        Long r8 = vars.translate(l8);

        Assert.assertEquals(l7, r7);
        Assert.assertEquals(l8, r8);

        Boolean b9 = Boolean.TRUE;
        Boolean b10 = Boolean.FALSE;

        Boolean r9 = vars.translate(b9);
        Boolean r10 = vars.translate(b10);

        Assert.assertEquals(b9, r9);
        Assert.assertEquals(b10, r10);
    }

    @Test
    public void testEvent() {

        Variables vars = createVariables();

        JsonObject event = new JsonObject("{"
                + " \"A\": \"ABBA\","
                + " \"COMP1\": 255,"
                + " \"isOn\": true,"
                + " \"obj\": { \"train\": 722343 },"
                + " \"List\": [ \"Bus\", \"Car\" ],"
                + " \"B\": 10.23"
                + "}");

        JsonObject config = new JsonObject("{"
                + " \"Rule1\": \"%{B}\","
                + " \"Rule2\": \"%{A}%{h}%{}\","
                + " \"Rule3\": \"%{A}%{B}\","
                + " \"Rule4\": \"%{A}%{A}\","
                + " \"Rule5\": \" %{} %{B} == %{A} \","
                + " \"Rule6\": \"%{%{A}\","
                + " \"Rule7\": \"%{A}}}\","
                + " \"Rule8\": \"%{A} }B%{\","
                + " \"Rule9\": \"%{F}}F%{\","
                + " \"Rule10\": \"%{CAR}\","
                + " \"Rule11\": \" %{B}\","
                + " \"Rule12\": \"%{B} \","
                + " \"Rule13\": \"|%{A}|%{B}|%{A}|%{B}|%{A}|%{B}|%{A}|%{B}|\","
                + " \"Rule14\": \"Title: %{B}.\","
                + " \"Rule15\": \"%{COMP1}\","
                + " \"Rule16\": \"%{isOn}\""
                + " }");

        Map<String, Object> rules = config.toMap();

        Object r1 = vars.translate(event, rules.get("Rule1"));
        Assert.assertEquals(Double.valueOf("10.23d"), r1);

        Object r2 = vars.translate(event, rules.get("Rule2"));
        Assert.assertEquals("ABBA%{h}%{}", r2);

        Object r3 = vars.translate(event, rules.get("Rule3"));
        Assert.assertEquals("ABBA10.23", r3);

        Object r4 = vars.translate(event, rules.get("Rule4"));
        Assert.assertEquals("ABBAABBA", r4);

        Object r5 = vars.translate(event, rules.get("Rule5"));
        Assert.assertEquals(" %{} 10.23 == ABBA ", r5);

        Object r6 = vars.translate(event, rules.get("Rule6"));
        Assert.assertEquals("%{%{A}", r6);

        Object r7 = vars.translate(event, rules.get("Rule7"));
        Assert.assertEquals("ABBA}}", r7);

        Object r8 = vars.translate(event, rules.get("Rule8"));
        Assert.assertEquals("ABBA }B%{", r8);

        Object r9 = vars.translate(event, rules.get("Rule9"));
        Assert.assertEquals("%{F}}F%{", r9);

        Object r10 = vars.translate(event, rules.get("Rule10"));
        Assert.assertEquals("%{CAR}", r10);

        Object r11 = vars.translate(event, rules.get("Rule11"));
        Assert.assertEquals(" 10.23", r11);

        Object r12 = vars.translate(event, rules.get("Rule12"));
        Assert.assertEquals("10.23 ", r12);

        Object r13 = vars.translate(event, rules.get("Rule13"));
        Assert.assertEquals("|ABBA|10.23|ABBA|10.23|ABBA|10.23|ABBA|10.23|", r13);

        Object r14 = vars.translate(event, rules.get("Rule14"));
        Assert.assertEquals("Title: 10.23.", r14);

        Object r15 = vars.translate(event, rules.get("Rule15"));
        Assert.assertEquals(Integer.valueOf("255"), r15);

        Object r16 = vars.translate(event, rules.get("Rule16"));
        Assert.assertEquals(Boolean.TRUE, r16);
    }

    @Test
    public void testBuiltin() throws Exception {
        Variables vars = createVariables();

        JsonObject config = new JsonObject("{"
                + " \"Rule1\": \"%{#node}\","
                + " \"Rule2\": \"%{#cluster}\","
                + " \"Rule3\": \"%{#chain}\","
                + " \"Rule4\": \"%{#host}\","
                + " \"Rule5\": \"%{#date}\","
                + " \"Rule6\": \"%{#timestamp}\","
                + " \"Rule7\": \"%{#now}\","
                + " \"Rule8\": \"%{#sensor.cpu.load}\","
                + " \"Rule9\": \"%{#prop.java.home}\","
                + " \"Rule10\": \"%{#spikex.home}\","
                + " \"Rule11\": \"%{#spikex.conf}\","
                + " \"Rule12\": \"%{#spikex.data}\","
                + " \"Rule13\": \"%{#spikex.tmp}\","
                + " \"Rule14\": \"%{#+YYYY-MM-dd}\","
                + " \"Rule15\": \"file:%{#spikex.data}/notifications\","
                + " \"Rule16\": \"%{#now(UTC,-1m)}\"," // UTC is set just for testing parsing...
                + " \"Rule17\": \"%{#now(5h)}\""
                + " }");

        Map<String, Object> rules = config.toMap();

        Object r1 = vars.translate(rules.get("Rule1"));
        Assert.assertEquals("NodeName", r1);

        Object r2 = vars.translate(rules.get("Rule2"));
        Assert.assertEquals("ClusterName", r2);

        String r4 = vars.translate(rules.get("Rule4"));
        Assert.assertTrue(r4.length() > 0);

        String r5 = vars.translate(rules.get("Rule5"));
        Assert.assertTrue(r5.length() > 0);

        Long r7 = vars.translate(rules.get("Rule7"));
        Assert.assertTrue(r7 <= System.currentTimeMillis());

        String r8 = vars.translate(rules.get("Rule8"));
        Assert.assertEquals("%{#sensor.cpu.load}", r8);

        String r10 = vars.translate(rules.get("Rule10"));
        Assert.assertEquals("HomePath", r10);

        String r11 = vars.translate(rules.get("Rule11"));
        Assert.assertEquals("ConfPath", r11);

        String r12 = vars.translate(rules.get("Rule12"));
        Assert.assertEquals("DatPath", r12);

        String r13 = vars.translate(rules.get("Rule13"));
        Assert.assertEquals("TmpPath", r13);

        String r14 = vars.translate(rules.get("Rule14"));
        Assert.assertTrue(r14.length() == 10);

        String r15 = vars.translate(rules.get("Rule15"));
        Assert.assertEquals("file:DatPath/notifications", r15);
    
        Long r16 = vars.translate(rules.get("Rule16")); 
        DateTime dt16 = new DateTime(r16,DateTimeZone.UTC); // Millis is not related to any timezone
        DateTime dt = DateTime.now(DateTimeZone.UTC);
        dt = dt.minusMinutes(1);
        DateTimeFormatter formatter = DateTimeFormat.forPattern("MM-dd-yyyy HH:mm");
        System.out.println("Expected: " + formatter.print(dt) + " dt16: " + formatter.print(dt16));
        Assert.assertEquals(formatter.print(dt), formatter.print(dt16));

        Long r17 = vars.translate(rules.get("Rule17")); 
        DateTime dt17 = new DateTime(r17);
        dt = DateTime.now();
        dt = dt.plusHours(5);
        System.out.println("Expected: " + formatter.print(dt) + " dt17: " + formatter.print(dt17));
        Assert.assertEquals(formatter.print(dt), formatter.print(dt17));
    }

    private Variables createVariables() {

        JsonObject config = new JsonObject();
        config.putString(CONF_KEY_NODE_NAME, "NodeName");
        config.putString(CONF_KEY_CLUSTER_NAME, "ClusterName");
        config.putString(CONF_KEY_HOME_PATH, "HomePath");
        config.putString(CONF_KEY_CONF_PATH, "ConfPath");
        config.putString(CONF_KEY_DATA_PATH, "DatPath");
        config.putString(CONF_KEY_TMP_PATH, "TmpPath");
        config.putString(CONF_KEY_CHAIN_NAME, "ChainName");
        return new Variables(config);
    }
}
