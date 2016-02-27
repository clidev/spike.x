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
import static io.spikex.core.AbstractVerticle.CONF_KEY_CLUSTER_NAME;
import static io.spikex.core.AbstractVerticle.CONF_KEY_CONF_PATH;
import static io.spikex.core.AbstractVerticle.CONF_KEY_DATA_PATH;
import static io.spikex.core.AbstractVerticle.CONF_KEY_HOME_PATH;
import static io.spikex.core.AbstractVerticle.CONF_KEY_LOCAL_ADDRESS;
import static io.spikex.core.AbstractVerticle.CONF_KEY_NODE_NAME;
import static io.spikex.core.AbstractVerticle.CONF_KEY_TMP_PATH;
import static io.spikex.core.AbstractVerticle.CONF_KEY_USER;
import static io.spikex.core.helper.Events.EVENT_FIELD_HOST;
import static io.spikex.core.helper.Events.EVENT_FIELD_ID;
import static io.spikex.core.helper.Events.EVENT_FIELD_MESSAGE;
import static io.spikex.core.helper.Events.EVENT_FIELD_PRIORITY;
import static io.spikex.core.helper.Events.EVENT_FIELD_SOURCE;
import static io.spikex.core.helper.Events.EVENT_FIELD_TAGS;
import static io.spikex.core.helper.Events.EVENT_FIELD_TIMESTAMP;
import static io.spikex.core.helper.Events.EVENT_FIELD_TIMEZONE;
import static io.spikex.core.helper.Events.EVENT_FIELD_TYPE;
import io.spikex.core.util.HostOs;
import io.spikex.filter.input.TailFile;
import java.util.Random;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
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
 * TailFile tester.
 *
 * @author cli
 */
public class TailFileTest extends TestVerticle implements Handler<Long> {
    
    private long m_timerId;
    private int m_iter;
    private int m_maxIterCount;
    private int m_count;
    private Random m_rand;
    private AtomicBoolean m_deployed;
    
    private static final String EVENT_ADDRESS = "tail-file";
    private static final String LOG_FILE = "build/spikex.log";
    
    private static final String[] LOG_LINES = new String[]{
        "Peace comes from within. Do not seek it without.",
        "You will not be punished for your anger, you will be punished by your anger.",
        "The mind is everything. What you think you become.",
        "Those who are free of resentful thoughts surely find peace.",
        "To understand everything is to forgive everything.",
        "The only real failure in life is not to be true to the best one knows.",
        "You cannot travel the path until you have become the path itself",
        "An idea that is developed and put into action is more important than an idea that exists only as an idea.",
        "A jug fills drop by drop.",
        "Believe nothing, no matter where you read it, or who said it, no matter if I have said it, unless it agrees with your own reason and your own common sense.",
        "A dog is not considered a good dog because he is a good barker. A man is not considered a good man because he is a good talker.",
        "The way is not in the sky. The way is in the heart.",
        "The secret of health for both mind and body is not to mourn for the past, worry about the future, or anticipate troubles, but to live in the present moment wisely and earnestly.",
        "Your work is to discover your work and then with all your heart to give yourself to it.",
        "To conquer oneself is a greater task than conquering others.",
        "Better than a thousand hollow words, is one word that brings peace.",
        "Do not dwell in the past, do not dream of the future, concentrate the mind on the present moment.",};
    
    private final Logger m_logger = LoggerFactory.getLogger(TailFileTest.class);
    private final Logger m_tailLogger = LoggerFactory.getLogger("io.spikex.tailtest");
    
    @Override
    public void start() {
        // Start generating log
        m_rand = new Random();
        m_deployed = new AtomicBoolean(false);
        m_timerId = vertx.setPeriodic(50L, this);
        // Start tests
        super.start();
    }
    
    @Override
    public void stop() {
        // Stop timer
        vertx.cancelTimer(m_timerId);
        // Stop tests
        super.stop();
    }
    
    @Test
    public void testRollingFile() throws InterruptedException {
        
        m_iter = 0;
        m_maxIterCount = 400;
        m_count = 0;
        
        final JsonObject config = createBaseConfig();

        // Tail the log file
        JsonArray paths = new JsonArray();
        paths.addString(LOG_FILE);
        config.putArray("paths", paths);

        // This is only for testing! 
        // Normally we want to read around 1-8K per cycle or whatever is a sufficient read block size...
        config.putNumber("min-read-size", 1); // 1 byte ~ catch all or most log lines,  800 bytes ~ 4 log lines
        config.putNumber("interval", 50); // Increase interval to catch more lines
        config.putNumber("max-read-size", 2048);
        config.putBoolean("read-chunks", true);

        container.deployWorkerVerticle(TailFile.class.getName(), config, 1, false,
                new AsyncResultHandler<String>() {
                    @Override
                    public void handle(final AsyncResult<String> ar) {
                        if (ar.failed()) {
                            m_logger.error("Failed to deploy verticle", ar.cause());
                            Assert.fail();
                        } else {
                            m_deployed.set(true);
                        }
                    }
                });

        // Keep track of log lines (ascending)
        final Pattern pattern = Pattern.compile("\\[([ 0-9]+)\\]");

        // Receive tail events
        vertx.eventBus().registerLocalHandler(EVENT_ADDRESS, new Handler<Message<JsonObject>>() {
            
            private int _prevIdx = -1;
            
            @Override
            public void handle(final Message<JsonObject> event) {
                JsonObject evn = event.body();
                assertTailEvent(evn);
                
                String line = evn.getString(EVENT_FIELD_MESSAGE);
                m_logger.debug("line: {}", line);
                m_count++;

                //
                // Verify that log lines are emitted in order
                //
                Matcher m = pattern.matcher(line);
                if (m.find()) {
                    String idx = m.group(1);
                    int index = Integer.parseInt(idx);
                    Assert.assertTrue("Log line order mismatch - index: "
                            + index + " prevIndex: " + _prevIdx, index > _prevIdx);
                    int step = index - _prevIdx;
                    if (step > 1) {
                        m_logger.warn(">>>>>>>>> Jumped over {} log lines during log rollover...", step);
                        _prevIdx = index - 1;
                    }
                    _prevIdx++;
                }
            }
        });
    }
    
    @Override
    public void handle(final Long event) {
        if (m_deployed.get()) {
            for (int i = 0; i < 10; i++) {
                int n = m_rand.nextInt(LOG_LINES.length);
                m_tailLogger.info("[" + m_iter++ + "] {}", LOG_LINES[n]);
            }
            if (m_iter >= m_maxIterCount) {

                // Stop test
                VertxAssert.testComplete();

                // Success, if we caught more than 300 log lines
                m_logger.info("Caught {} log lines...", m_count);
                Assert.assertTrue("Received less than 300 log line events", m_count > 300);
            }
        }
    }
    
    private void assertTailEvent(final JsonObject evn) {
        Assert.assertTrue(evn.containsField(EVENT_FIELD_ID));
        Assert.assertTrue(evn.containsField(EVENT_FIELD_SOURCE));
        Assert.assertTrue(evn.containsField(EVENT_FIELD_HOST));
        Assert.assertTrue(evn.containsField(EVENT_FIELD_PRIORITY));
        Assert.assertTrue(evn.containsField(EVENT_FIELD_TAGS));
        Assert.assertTrue(evn.containsField(EVENT_FIELD_TIMESTAMP));
        Assert.assertTrue(evn.containsField(EVENT_FIELD_TIMEZONE));
        Assert.assertTrue(evn.containsField(EVENT_FIELD_TYPE));
        Assert.assertTrue(evn.containsField(EVENT_FIELD_MESSAGE));
    }
    
    private JsonObject createBaseConfig() {
        JsonObject config = new JsonObject();
        config.putString(CONF_KEY_CHAIN_NAME, "tailfile-test");
        config.putString(CONF_KEY_LOCAL_ADDRESS, "my-local-address");
        config.putString(CONF_KEY_DEST_ADDRESS, EVENT_ADDRESS);
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
