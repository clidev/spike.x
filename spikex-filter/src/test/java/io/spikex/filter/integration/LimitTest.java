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
import io.spikex.core.util.HostOs;
import io.spikex.filter.Limit;
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
 * Limit tester.
 *
 * @author cli
 */
public class LimitTest extends TestVerticle {

    private long m_timerId;

    private static final String EVENT_SRC_ADDRESS = "limit-field-src";
    private static final String EVENT_DEST_ADDRESS = "limit-field-dest";

    private final Logger m_logger = LoggerFactory.getLogger(LimitTest.class);

    @Test
    public void testLimitSchedule() {

        // Limit configuration
        final JsonObject config = createBaseConfig();
        config.putBoolean("discard-on-mismatch", true);

        final JsonObject schedules = new JsonObject();
        schedules.putString("anytime", "* * * * *");

        final JsonObject throttle1 = new JsonObject();
        throttle1.putNumber("rate", 1L);
        throttle1.putNumber("interval", 2L);
        throttle1.putString("unit", "sec");

        final JsonObject throttles = new JsonObject();
        throttles.putObject("one-per-two-secs", throttle1);

        final JsonObject rule1 = new JsonObject();
        rule1.putString("match-tag", "ERROR");
        rule1.putString("schedule", "anytime");
        rule1.putString("throttle", "one-per-two-secs");

        final JsonArray rules = new JsonArray();
        rules.addObject(rule1);

        config.putObject("schedules", schedules);
        config.putObject("throttles", throttles);
        config.putArray("rules", rules);

        final AtomicInteger counter = new AtomicInteger(0);
        vertx.eventBus().registerLocalHandler(EVENT_DEST_ADDRESS,
                new Handler<Message<JsonObject>>() {

                    @Override
                    public void handle(final Message<JsonObject> event) {
                        JsonObject evn = event.body();
                        //
                        m_logger.info("Received event: {}", evn);
                        //
                        // Stop test
                        //
                        if (counter.getAndIncrement() >= 4) {
                            vertx.cancelTimer(m_timerId);
                            Assert.assertTrue("Received too many events",
                                    counter.get() <= 5);
                            VertxAssert.testComplete();
                        }
                    }
                });

        container.deployVerticle(Limit.class.getName(), config, 1,
                new AsyncResultHandler<String>() {
                    @Override
                    public void handle(final AsyncResult<String> ar) {
                        if (ar.succeeded()) {
                            m_timerId = vertx.setPeriodic(200L, new Handler<Long>() {

                                @Override
                                public void handle(Long timerId) {
                                    //
                                    // Generate test event
                                    //
                                    JsonObject event = EventCreator.create("ERROR");
                                    vertx.eventBus().publish(EVENT_SRC_ADDRESS, event);
                                }
                            });
                        } else {
                            m_logger.error("Failed to deploy verticle", ar.cause());
                            Assert.fail();
                        }
                    }
                });
    }

    private JsonObject createBaseConfig() {
        JsonObject config = new JsonObject();
        config.putString(CONF_KEY_CHAIN_NAME, "limit-test");
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
