/* $Id: EncryptionTester.java 343 2014-02-08 11:17:12Z cli-dev $
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
package io.spikex.notifier.integration;

import com.eaio.uuid.UUID;
import static io.spikex.core.AbstractVerticle.CONF_KEY_CLUSTER_NAME;
import static io.spikex.core.AbstractVerticle.CONF_KEY_CONF_PATH;
import static io.spikex.core.AbstractVerticle.CONF_KEY_DATA_PATH;
import static io.spikex.core.AbstractVerticle.CONF_KEY_HOME_PATH;
import static io.spikex.core.AbstractVerticle.CONF_KEY_LOCAL_ADDRESS;
import static io.spikex.core.AbstractVerticle.CONF_KEY_NODE_NAME;
import static io.spikex.core.AbstractVerticle.CONF_KEY_TMP_PATH;
import static io.spikex.core.AbstractVerticle.CONF_KEY_USER;
import io.spikex.core.helper.Commands;
import static io.spikex.core.helper.Events.EVENT_FIELD_MESSAGE;
import static io.spikex.core.helper.Events.EVENT_FIELD_TAGS;
import static io.spikex.core.helper.Events.EVENT_FIELD_TITLE;
import static io.spikex.core.helper.Events.EVENT_FIELD_TYPE;
import static io.spikex.core.helper.Events.SPIKEX_ORIGIN_TAG;
import io.spikex.core.util.HostOs;
import io.spikex.notifier.Activator;
import java.util.HashMap;
import java.util.Map;
import java.util.logging.Level;
import junit.framework.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.vertx.java.core.AsyncResult;
import org.vertx.java.core.AsyncResultHandler;
import org.vertx.java.core.Handler;
import org.vertx.java.core.eventbus.Message;
import org.vertx.java.core.json.JsonObject;
import org.vertx.testtools.TestVerticle;
import org.vertx.testtools.VertxAssert;

/**
 * NotifierAgent tester.
 *
 * @author cli
 */
public class ActivatorTest extends TestVerticle implements Handler<Long> {

    private int m_counter;

    private static final String LOCAL_ADDRESS = "ActivatorTestAddress." + new UUID().toString();
    private static final String EVENT_NOTIFIER_ADDRESS = "spikex-notifier";

    private final Logger m_logger = LoggerFactory.getLogger(ActivatorTest.class);

    @Test
    public void testDeployment() throws InterruptedException {

        JsonObject config = createBaseConfig();
        config.putString(CONF_KEY_LOCAL_ADDRESS, LOCAL_ADDRESS);

        container.deployWorkerVerticle(Activator.class.getName(), config, 1, false,
                new AsyncResultHandler<String>() {
                    @Override
                    public void handle(final AsyncResult<String> ar) {
                        if (ar.succeeded()) {
                            initActivator(ar.result());
                        } else {
                            m_logger.error("Failed to deploy verticle: {}", ar.cause());
                            Assert.fail();
                        }
                    }
                }
        );

        vertx.setPeriodic(500L, this); // Generate events once every 500 ms
    }

    @Override
    public void handle(final Long timerId) {
        if (m_counter++ > 1) {
            // Stop test
            VertxAssert.testComplete();
        } else {
            m_logger.info("Emitting events");
            //
            // Generate test event (INFO)
            //                            
            Map<String, Object> fields = new HashMap();
            fields.put(EVENT_FIELD_TAGS, new String[]{SPIKEX_ORIGIN_TAG, "INFO"});

            StringBuilder msg = new StringBuilder();
            msg.append("\"");
            msg.append("Jag älskar döda linjer.\n");
            msg.append("Jag gillar det whooshing ljudet de gör när de flyger förbi.");
            msg.append("\" - Douglas Adams");
            fields.put(EVENT_FIELD_MESSAGE, msg.toString());

            StringBuilder title = new StringBuilder();
            title.append(fields.get(EVENT_FIELD_TYPE));
            title.append(" from Host78238");
            fields.put(EVENT_FIELD_TITLE, title.toString());
            fields.put("@success", true);

            JsonObject event = EventCreator.create("notifier", fields);
            vertx.eventBus().publish(EVENT_NOTIFIER_ADDRESS, event);

            //
            // Generate test event (ERROR)
            //                            
            fields = new HashMap();
            fields.put(EVENT_FIELD_TAGS, new String[]{SPIKEX_ORIGIN_TAG, "ERROR"});
            fields.put(EVENT_FIELD_TYPE, "Program Error");

            msg = new StringBuilder();
            msg.append("Severe program error on line 463 in CoreEngine.\n");
            msg.append("Please restart the whole operating system. Darn.");
            fields.put(EVENT_FIELD_MESSAGE, msg.toString());

            title = new StringBuilder();
            title.append(fields.get(EVENT_FIELD_TYPE));
            title.append(" from Host23723");
            fields.put(EVENT_FIELD_TITLE, title.toString());

            fields.put("@alert-danger", true);

            event = EventCreator.create("notifier", fields);
            vertx.eventBus().publish(EVENT_NOTIFIER_ADDRESS, event);
            try {
                Thread.sleep(3000);
            } catch (InterruptedException ex) {
                java.util.logging.Logger.getLogger(ActivatorTest.class.getName()).log(Level.SEVERE, null, ex);
            }
        }
    }

    private void initActivator(final String deploymentId) {
        m_logger.info("Initializing activator: {}", deploymentId);
        Commands.call(
                vertx.eventBus(),
                LOCAL_ADDRESS,
                Commands.cmdInitVerticle(),
                new Handler<Message<JsonObject>>() {

                    @Override
                    public void handle(Message<JsonObject> message) {
                        m_logger.info("Activator initialized");
                    }
                });
    }

    private JsonObject createBaseConfig() {
        JsonObject config = new JsonObject();
        config.putString(CONF_KEY_LOCAL_ADDRESS, EVENT_NOTIFIER_ADDRESS);
        config.putString(CONF_KEY_NODE_NAME, "node-name");
        config.putString(CONF_KEY_CLUSTER_NAME, "cluster-name");
        config.putString(CONF_KEY_HOME_PATH, "build/resources/test");
        config.putString(CONF_KEY_CONF_PATH, "build/resources/test");
        config.putString(CONF_KEY_DATA_PATH, "build/resources/test");
        config.putString(CONF_KEY_TMP_PATH, "build/resources/test");
        config.putString(CONF_KEY_USER, "spikex");
        m_logger.info("Host operating system: {}", HostOs.operatingSystemFull());
        return config;
    }
}
