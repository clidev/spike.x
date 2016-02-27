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
package io.spikex.filter.integration;

import static io.spikex.core.AbstractFilter.CONF_KEY_CHAIN_NAME;
import static io.spikex.core.AbstractFilter.CONF_KEY_DEST_ADDRESS;
import static io.spikex.core.AbstractFilter.CONF_KEY_SOURCE_ADDRESS;
import static io.spikex.core.AbstractVerticle.CONF_KEY_CLUSTER_NAME;
import static io.spikex.core.AbstractVerticle.CONF_KEY_CONF_PATH;
import static io.spikex.core.AbstractVerticle.CONF_KEY_DATA_PATH;
import static io.spikex.core.AbstractVerticle.CONF_KEY_HOME_PATH;
import static io.spikex.core.AbstractVerticle.CONF_KEY_LOCAL_ADDRESS;
import static io.spikex.core.AbstractVerticle.CONF_KEY_NODE_NAME;
import static io.spikex.core.AbstractVerticle.CONF_KEY_TMP_PATH;
import static io.spikex.core.AbstractVerticle.CONF_KEY_UPDATE_INTERVAL;
import static io.spikex.core.AbstractVerticle.CONF_KEY_USER;
import static io.spikex.core.AbstractVerticle.SHARED_METRICS_KEY;
import static io.spikex.core.helper.Events.EVENT_FIELD_TAGS;
import io.spikex.core.util.HostOs;
import io.spikex.filter.Mutate;
import java.util.HashMap;
import java.util.Map;
import junit.framework.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.vertx.java.core.AsyncResult;
import org.vertx.java.core.AsyncResultHandler;
import org.vertx.java.core.Handler;
import org.vertx.java.core.eventbus.Message;
import org.vertx.java.core.json.JsonArray;
import org.vertx.java.core.json.JsonObject;
import org.vertx.testtools.TestVerticle;
import org.vertx.testtools.VertxAssert;

/**
 * MutateField tester.
 *
 * @author cli
 */
@SuppressWarnings("PMD.AvoidUsingHardCodedIP")
public class MutateFieldTest extends TestVerticle {

    private static final String EVENT_SRC_ADDRESS = "mutate-field-src";
    private static final String EVENT_DEST_ADDRESS = "mutate-field-dest";
    private static final String TEST_IP = "192.168.11.34";

    private final Logger m_logger = LoggerFactory.getLogger(MutateFieldTest.class);

    @Test
    public void testRules() {

        // MutateField configuration
        final JsonObject config = createBaseConfig();
        config.putNumber(CONF_KEY_UPDATE_INTERVAL, 1000L);

        vertx.sharedData().getMap(SHARED_METRICS_KEY).put("mem.free.perc", 13.48d);

        JsonObject fields = new JsonObject();
        fields.putString("@cpu.total", "%{#metric.cpu.total.perc}");
        fields.putString("@mem.free", "%{#metric.mem.free.perc}");
        config.putObject("add-fields", fields);

        JsonObject modifiers = new JsonObject();
        JsonObject alert = new JsonObject();
        alert.putString("add-tags", "ALARM");
        JsonObject addFields = new JsonObject();
        addFields.putString("@alarm", "Wake up, we have an alarm!");
        addFields.putArray("empty-list", new JsonArray());
        addFields.putArray("list-of-values", new JsonArray(new String[]{"value1", "value2", "%{#metric.mem.free.perc}"}));
        alert.putObject("add-fields", addFields);
        modifiers.putObject("alert", alert);
        config.putObject("modifiers", modifiers);

        JsonArray rules = new JsonArray();
        JsonObject rule1 = new JsonObject();
        rule1.putString("match-field", "mapOfStuff/int");
        rule1.putNumber("value-lt", 0L);
        rule1.putString("modifier", "alert");
        rules.add(rule1);
        config.putArray("rules", rules);

        // Receive events
        vertx.eventBus().registerLocalHandler(EVENT_DEST_ADDRESS,
                new Handler<Message<JsonObject>>() {

                    @Override
                    public void handle(final Message<JsonObject> event) {
                        JsonObject evn = event.body();
                        m_logger.info(evn.encodePrettily());
                        Assert.assertTrue("HOSTIP field is missing", evn.containsField("HOSTIP"));
                        Assert.assertTrue("@alarm field is missing", evn.containsField("@alarm"));
                        JsonArray tags = evn.getArray(EVENT_FIELD_TAGS);
                        Assert.assertTrue("ALARM tag is missing", tags.contains("ALARM"));
                        Assert.assertTrue("@mem.free is not 15.48", evn.getValue("@mem.free").equals(Double.valueOf("15.48")));
                        //
                        // Stop test
                        //
                        VertxAssert.testComplete();
                    }
                });

        container.deployVerticle(Mutate.class.getName(), config, 1,
                new AsyncResultHandler<String>() {
                    @Override
                    public void handle(final AsyncResult<String> ar) {
                        if (ar.succeeded()) {
                            //
                            // Generate test event
                            //                            
                            vertx.sharedData().getMap(SHARED_METRICS_KEY).put("mem.free.perc", 15.48d);

                            Map<String, Object> extraFields = new HashMap();
                            extraFields.put("HOSTIP", TEST_IP);
                            extraFields.put(TEST_IP, TEST_IP);

                            JsonObject event = EventCreator.create("Mutate", extraFields);
                            vertx.eventBus().publish(EVENT_SRC_ADDRESS, event);

                        } else {
                            m_logger.error("Failed to deploy verticle", ar.cause());
                            Assert.fail();
                        }
                    }
                }
        );
    }

    @Test
    public void testRenames() {

        // MutateField configuration
        final JsonObject config = createBaseConfig();

        JsonObject fields = new JsonObject();
        fields.putString("HOSTIP", "@source"); // String (192.168.11.34)
        fields.putString("bignumber", "@number"); // Long (
        fields.putString("arrayOfStuff", "@words"); // JsonArray
        fields.putString("Light1OnOff", "@lightonoff"); // Boolean
        fields.putString("mapOfStuff", "@struct"); // JsonObject
        fields.putString("Filler1", "1288723787823");
        fields.putString("Filler2", "1288723787823");
        fields.putString("Filler3", "1288723787823");
        config.putObject("renames", fields);

        // Receive events
        vertx.eventBus().registerLocalHandler(EVENT_DEST_ADDRESS,
                new Handler<Message<JsonObject>>() {

                    @Override
                    public void handle(final Message<JsonObject> event) {
                        JsonObject evn = event.body();
                        //
                        Assert.assertTrue("rename of HOSTIP failed", evn.containsField("@source"));
                        Assert.assertFalse("rename did not remove HOSTIP", evn.containsField("HOSTIP"));
                        Assert.assertTrue("failed to get value of @source", TEST_IP.equals(evn.getString("@source")));
                        //
                        Assert.assertTrue("rename of bignumber failed", evn.containsField("@number"));
                        Assert.assertFalse("rename did not remove bignumber", evn.containsField("bignumber"));
                        Assert.assertTrue("failed to get value of @number", Long.MAX_VALUE == evn.getLong("@number"));
                        //
                        Assert.assertTrue("rename of arrayOfStuff failed", evn.containsField("@words"));
                        Assert.assertFalse("rename did not remove arrayOfStuff", evn.containsField("arrayOfStuff"));
                        Assert.assertTrue("failed to get blue value", "blue".equals(evn.getArray("@words").get(1)));
                        Assert.assertTrue("failed to get purple value", "purple".equals(evn.getArray("@words").get(2)));
                        //
                        Assert.assertTrue("rename of Light1OnOff failed", evn.containsField("@lightonoff"));
                        Assert.assertFalse("rename did not remove Light1OnOff", evn.containsField("Light1OnOff"));
                        Assert.assertTrue("failed to get value of @lightonoff", Boolean.TRUE.equals(evn.getBoolean("@lightonoff")));
                        //
                        Assert.assertTrue("rename of mapOfStuff failed", evn.containsField("@struct"));
                        Assert.assertFalse("rename did not remove mapOfStuff", evn.containsField("mapOfStuff"));
                        Assert.assertTrue("failed to get string value of @struct", TEST_IP.equals(evn.getObject("@struct").getString(TEST_IP)));
                        Assert.assertTrue("failed to get int value of @struct", Integer.MIN_VALUE == evn.getObject("@struct").getInteger("int"));
                        //
                        // Stop test
                        //
                        VertxAssert.testComplete();
                    }
                }
        );

        container.deployVerticle(Mutate.class.getName(), config, 1,
                new AsyncResultHandler<String>() {
                    @Override
                    public void handle(final AsyncResult<String> ar) {
                        if (ar.succeeded()) {
                            //
                            // Generate test event
                            //                            
                            Map<String, Object> extraFields = new HashMap();
                            extraFields.put("HOSTIP", TEST_IP);
                            extraFields.put(TEST_IP, TEST_IP);
                            extraFields.put("bignumber", Long.MAX_VALUE);

                            JsonObject event = EventCreator.create("Mutate", extraFields);
                            JsonObject json = new JsonObject();
                            json.putString(TEST_IP, TEST_IP);
                            json.putNumber("int", Integer.MIN_VALUE);
                            event.putObject("mapOfStuff", json);

                            vertx.eventBus().publish(EVENT_SRC_ADDRESS, event);

                        } else {
                            m_logger.error("Failed to deploy verticle", ar.cause());
                            Assert.fail();
                        }
                    }
                }
        );
    }

    private JsonObject createBaseConfig() {
        JsonObject config = new JsonObject();
        config.putString(CONF_KEY_CHAIN_NAME, "mutatefield-test");
        config.putString(CONF_KEY_LOCAL_ADDRESS, "my-local-address");
        config.putString(CONF_KEY_SOURCE_ADDRESS, EVENT_SRC_ADDRESS);
        config.putString(CONF_KEY_DEST_ADDRESS, EVENT_DEST_ADDRESS);
        config.putString(CONF_KEY_NODE_NAME, "node-name");
        config.putString(CONF_KEY_CLUSTER_NAME, "cluster-name");
        config.putString(CONF_KEY_HOME_PATH, "build");
        config.putString(CONF_KEY_CONF_PATH, "build");
        config.putString(CONF_KEY_DATA_PATH, "build");
        config.putString(CONF_KEY_TMP_PATH, "build");
        config.putString(CONF_KEY_USER, "spikex");
        m_logger.info("Host operating system: {}", HostOs.operatingSystemFull());
        return config;
    }
}
