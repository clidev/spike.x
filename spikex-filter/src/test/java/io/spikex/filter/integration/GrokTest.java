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
import static io.spikex.core.AbstractVerticle.CONF_KEY_USER;
import static io.spikex.core.helper.Events.EVENT_FIELD_SOURCE;
import static io.spikex.core.helper.Events.EVENT_FIELD_TAGS;
import io.spikex.core.util.HostOs;
import io.spikex.filter.Grok;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
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
 * Grok tester.
 *
 * Grok pattern tester: http://grokdebug.herokuapp.com
 *
 * @author cli
 */
public class GrokTest extends TestVerticle {

    private static final String EVENT_SRC_ADDRESS = "grok-field-src";
    private static final String EVENT_DEST_ADDRESS = "grok-field-dest";
    private static final String LOG_LINE_JBOSS1
            = "2007-08-09 12:10:01,834 [http-0.0.0.0-8080-1] ERROR "
            + "org.apache.catalina.core.ContainerBase.[jboss.web].[localhost].[/webadmin].[action]"
            + " - Servlet.service() for servlet action threw exception \n";
    private static final String LOG_LINE_JBOSS2
            = "2009-09-29 15:43:41,112 ERROR [org.jboss.deployment.MainDeployer] "
            + "Could not create deployment: file:/D:/Java/jboss-4.2.3.GA/server/grmbs/deploy/ejb-management.jar";
    private static final String LOG_LINE_JBOSS3
            = "12:37:25,546 INFO [Server] Release ID: JBoss EAP] 4.3.0.GA_CP02"
            + " (build: SVNTag=JBPAPP_4_3_0_GA_CP02 date=200808051050)";
    private static final String[] LOG_LINES_EXCEPTION = {
        "14:01:06,380 ERROR [HostConfig:576] Error waiting for multi-thread deployment of context descriptors to complete java.util.concurrent.ExecutionException: ",
        "java.lang.StackOverflowError",
        "    at java.util.concurrent.FutureTask$Sync.innerGet(FutureTask.java:252)",
        "    at java.util.concurrent.FutureTask.get(FutureTask.java:111)",
        "    at org.apache.catalina.startup.HostConfig.deployDescriptors(HostConfig.java:574)",
        "    at org.apache.catalina.startup.HostConfig.deployApps(HostConfig.java:470)",
        "    at org.apache.catalina.startup.HostConfig.start(HostConfig.java:1413)",
        "    at java.util.concurrent.ThreadPoolExecutor$Worker.run(ThreadPoolExecutor.java:603)",
        "    at java.lang.Thread.run(Thread.java:722)",
        "",
        "Caused by: java.lang.StackOverflowError at java.util.HashSet.(HashSet.java:103)",
        " at org.apache.catalina.startup.ContextConfig.populateSCIsForCacheEntry(ContextConfig.java:2243)",
        " at org.apache.catalina.startup.ContextConfig.populateSCIsForCacheEntry(ContextConfig.java:2260)",
        " at org.apache.catalina.startup.ContextConfig.populateSCIsForCacheEntry(ContextConfig.java:2260)",
        "12:37:25,546 INFO [Server] Release ID: JBoss EAP] 4.3.0.GA_CP02"
        + " (build: SVNTag=JBPAPP_4_3_0_GA_CP02 date=200808051050)"};

    private final Logger m_logger = LoggerFactory.getLogger(GrokTest.class);

    @Test
    public void testMatchLines() {

        // Grok configuration
        final JsonObject config = createBaseConfig();

        // group
        JsonObject fields = new JsonObject();
        JsonArray groupFields = new JsonArray(
                new String[]{"class", "method", "message", "thread", "level",
                    "year", "month", "day", "hour", "minute", "second"});
        fields.putArray("fields", groupFields); // On match, add the following fields to their own group
        config.putObject("group", fields);

        // match-lines
        JsonArray matchLines = new JsonArray();

        // Logline
        fields = new JsonObject();
        fields.putString("pattern", "%{JAVAJBOSS4LOG:line}"); // Java log line
        JsonArray tags = new JsonArray(new String[]{"log", "java"});
        fields.putArray("tags", tags); // On match, add the following tags
        JsonArray ignore = new JsonArray(new String[]{"JAVALVLCLS"});
        fields.putArray("ignore", ignore);
        matchLines.add(fields);

        // Duration
        fields = new JsonObject();
        fields.putString("pattern", "%{DURATIONMS:line}"); // Java log line
        tags = new JsonArray(new String[]{"log", "duration"});
        fields.putArray("tags", tags); // On match, add the following tags
        ignore = new JsonArray(new String[]{"DURATIONMS"});
        fields.putArray("ignore", ignore);
        matchLines.add(fields);

        config.putArray("match-lines", matchLines);

        // multi-line (does not support "ignore")
        fields = new JsonObject();
        fields.putString("pattern", "%{JAVAERRORSTACK:line}"); // Java exception line
        tags = new JsonArray(new String[]{"log", "java", "error", "exception"});
        fields.putArray("tags", tags); // On match, add the following tags
        // We want each exception class/segment in its own JsonObject
        fields.putString("segment-field", "class");
        config.putObject("multi-line", fields);

        // Receive events
        final AtomicInteger counter = new AtomicInteger(1);
        vertx.eventBus().registerLocalHandler(EVENT_DEST_ADDRESS,
                new Handler<Message<JsonObject>>() {

                    @Override
                    public void handle(final Message<JsonObject> event) {
                        JsonObject evn = event.body();
                        //
                        m_logger.info("Received event: {}", evn);
                        //
                        Assert.assertTrue("Grok".equals(evn.getString(EVENT_FIELD_SOURCE)));
                        Assert.assertTrue(evn.containsField("arrayOfStuff"));
                        Assert.assertTrue(evn.containsField("Light1OnOff"));
                        Assert.assertTrue(evn.containsField("BigNumber"));
                        //
                        JsonArray tags = evn.getArray(EVENT_FIELD_TAGS);
                        Assert.assertTrue(tags.contains("spikex"));
                        Assert.assertTrue(tags.contains("log"));
                        Assert.assertTrue(tags.contains("java"));
                        //
                        Assert.assertFalse("JAVALVLCLS was not ignored", evn.containsField("JAVALVLCLS"));
                        m_logger.info("--------- Counter: {} ----------", counter.get());
                        switch (counter.getAndIncrement()) {
                            case 1: {
                                Assert.assertTrue(evn.containsField("timestamp"));
                                JsonObject fields = evn.getObject("@fields");
                                Assert.assertTrue("ERROR".equals(fields.getString("level")));
                                Assert.assertTrue(fields.getInteger("year") == 2007);
                                Assert.assertTrue(fields.getInteger("day") == 9);
                                Assert.assertTrue(fields.getInteger("hour") == 12);
                                Assert.assertTrue(fields.getInteger("minute") == 10);
                                String message = evn.getString("@message");
                                Assert.assertTrue("2007-08-09 12:10:01,834 [http-0.0.0.0-8080-1] ERROR org.apache.catalina.core.ContainerBase.[jboss.web].[localhost].[/webadmin].[action] - Servlet.service() for servlet action threw exception ".equals(message));
                                break;
                            }
                            case 2: {
                                Assert.assertTrue(evn.containsField("timestamp"));
                                JsonObject fields = evn.getObject("@fields");
                                Assert.assertTrue("ERROR".equals(fields.getString("level")));
                                Assert.assertTrue(fields.getInteger("year") == 2009);
                                Assert.assertTrue(fields.getInteger("day") == 29);
                                Assert.assertTrue(fields.getInteger("month") == 9);
                                Assert.assertTrue(fields.getInteger("hour") == 15);
                                Assert.assertTrue(fields.getInteger("minute") == 43);
                                Assert.assertTrue("41,112".equals(fields.getString("second")));
                                String message = evn.getString("@message");
                                Assert.assertTrue("2009-09-29 15:43:41,112 ERROR [org.jboss.deployment.MainDeployer] Could not create deployment: file:/D:/Java/jboss-4.2.3.GA/server/grmbs/deploy/ejb-management.jar".equals(message));
                                break;
                            }
                            case 3: {
                                Assert.assertTrue(evn.containsField("timestamp"));
                                JsonObject fields = evn.getObject("@fields");
                                Assert.assertTrue("INFO".equals(fields.getString("level")));
                                Assert.assertNull(fields.getString("year"));
                                Assert.assertNull(fields.getString("month"));
                                Assert.assertNull(fields.getString("day"));
                                Assert.assertTrue(fields.getInteger("hour") == 12);
                                Assert.assertTrue(fields.getInteger("minute") == 37);
                                Assert.assertTrue("Server".equals(fields.getString("thread")));
                                String message = evn.getString("@message");
                                Assert.assertTrue("12:37:25,546 INFO [Server] Release ID: JBoss EAP] 4.3.0.GA_CP02 (build: SVNTag=JBPAPP_4_3_0_GA_CP02 date=200808051050)".equals(message));
                                break;
                            }
                            case 4: {
                                Assert.assertTrue(evn.containsField("@fields"));
//                                String message = evn.getString("@message");
//                                Assert.assertTrue(message.contains("catalina.startup.HostConfig.deployApps(HostConfig.java:470)"));
                                m_logger.info(evn.encodePrettily());
                                break;
                            }
                        }
                        //
                        // Stop test
                        //
                        VertxAssert.testComplete();
                    }
                }
        );

        container.deployVerticle(Grok.class.getName(), config, 1,
                new AsyncResultHandler<String>() {
                    @Override
                    public void handle(final AsyncResult<String> ar) {
                        if (ar.succeeded()) {
                            //
                            // Generate test events
                            // 1                           
                            JsonObject event = createEvent(LOG_LINE_JBOSS1);
                            vertx.eventBus().publish(EVENT_SRC_ADDRESS, event);
                            // 2                           
                            event = createEvent(LOG_LINE_JBOSS2);
                            vertx.eventBus().publish(EVENT_SRC_ADDRESS, event);
                            // 3                           
                            event = createEvent(LOG_LINE_JBOSS3);
                            vertx.eventBus().publish(EVENT_SRC_ADDRESS, event);
                            // 4
                            for (String line : LOG_LINES_EXCEPTION) {
                                event = createEvent(line);
                                vertx.eventBus().publish(EVENT_SRC_ADDRESS, event);
                            }
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
        config.putString(CONF_KEY_CHAIN_NAME, "grok-test");
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

    private JsonObject createEvent(final String line) {
        Map<String, Object> fields = new HashMap();
        fields.put("@message", line);
        return EventCreator.create("Grok", fields);
    }
}
