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
package io.spikex.filter.input;

import com.github.brainlag.nsq.NSQConsumer;
import com.github.brainlag.nsq.NSQMessage;
import com.github.brainlag.nsq.callbacks.NSQErrorCallback;
import com.github.brainlag.nsq.callbacks.NSQMessageCallback;
import com.github.brainlag.nsq.exceptions.NSQException;
import com.google.common.base.Preconditions;
import io.spikex.core.AbstractFilter;
import io.spikex.filter.internal.NsqClientConfig;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.vertx.java.core.eventbus.EventBus;
import org.vertx.java.core.json.JsonObject;

/**
 *
 * @author cli
 */
public final class Nsq extends AbstractFilter {

    private List<NSQConsumer> m_consumers;

    @Override
    protected void startFilter() {

        NsqClientConfig config = NsqClientConfig.builder(config()).build();
        List<String> topics = config.getTopics();

        // Sanity check
        Preconditions.checkArgument(topics.size() > 0, "No topics defined");
        m_consumers = new ArrayList();

        for (String topic : topics) {

            String channel = "";
            int pos = topic.indexOf(":");

            if (pos != -1) {
                channel = topic.substring(pos + 1);
                topic = topic.substring(0, pos);
            } else {
                throw new IllegalArgumentException("No channel definied for topic: " + topic);
            }

            logger().info("Creating consumer for topic: {} channel: {}", topic, channel);
            NSQConsumer consumer = new NSQConsumer(
                    config.buildNSQLookup(),
                    topic,
                    channel,
                    new NsqMessageHandler(eventBus(), getDestinationAddress()),
                    config.buildNSQConfig(variables()),
                    new NSQErrorCallback() {

                        @Override
                        public void error(final NSQException e) {
                            logger().error("NSQ consumer error", e);

                        }
                    });

            m_consumers.add(consumer);
            consumer.setMessagesPerBatch(config.getMessagesPerBatch());
            consumer.setLookupPeriod(config.getLookupPeriod());

            BlockingQueue<Runnable> messageQueue = new LinkedBlockingQueue(config.getMessagesQueueSize());
            consumer.setExecutor(
                    new ThreadPoolExecutor(
                            config.getCorePoolSize(),
                            config.getMaxPoolSize(),
                            config.getIdleThreadKeepAliveTime(),
                            TimeUnit.MILLISECONDS,
                            messageQueue));
            consumer.start();
        }
    }

    @Override
    protected void stopFilter() {
        if (m_consumers != null) {
            for (NSQConsumer consumer : m_consumers) {
                try {
                    consumer.shutdown();
                } catch (Exception e) {
                    logger().error("Failed to shutdown NSQ consumer", e);
                }
            }
        }
    }

    private static class NsqMessageHandler implements NSQMessageCallback {

        private final EventBus m_eventBus;
        private final String m_address;
        private final Logger m_logger = LoggerFactory.getLogger(NsqMessageHandler.class);

        private NsqMessageHandler(
                final EventBus eventBus,
                final String address) {

            m_eventBus = eventBus;
            m_address = address;
        }

        @Override
        public void message(final NSQMessage message) {

            String id = new String(message.getId(), StandardCharsets.UTF_8);
            String body = new String(message.getMessage(), StandardCharsets.UTF_8);
            JsonObject json = new JsonObject(body);

            m_logger.trace("Received NSQ message {}: {} - publishing on {}", id, json, m_address);
            m_eventBus.publish(m_address, json);
            message.finished(); // Signal that we're done...
        }
    }
}
