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
package io.spikex.filter.output;

import com.github.brainlag.nsq.NSQConfig;
import com.github.brainlag.nsq.NSQProducer;
import com.github.brainlag.nsq.exceptions.NSQException;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import io.spikex.core.AbstractFilter;
import static io.spikex.core.helper.Events.EVENT_FIELD_BATCH_EVENTS;
import static io.spikex.core.helper.Events.EVENT_FIELD_BATCH_SIZE;
import static io.spikex.core.helper.Events.EVENT_FIELD_ID;
import io.spikex.filter.internal.NsqClientConfig;
import static io.spikex.filter.internal.NsqClientConfig.NSQ_HTTP_PORT;
import io.spikex.filter.internal.Rule;
import java.security.Provider;
import java.security.Security;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeoutException;
import org.bouncycastle.jce.provider.BouncyCastleProvider;
import org.vertx.java.core.json.JsonArray;
import org.vertx.java.core.json.JsonObject;

/**
 *
 * @author cli
 */
public final class Nsq extends AbstractFilter {

    private ArrayList<Topic> m_topics;
    private NSQProducer m_producer;

    private static final String CONF_KEY_TOPICS = "topics";
    private static final int DEF_MAX_MSG_SIZE = 1024768; // Default max_msg_size in NSQ configuration
    private static final int DEF_MAX_BODY_SIZE = 5123840; // Default max_body_size in NSQ configuration

    @Override
    protected void startFilter() {

        //
        // Use Bouncy Castle as our security provider
        // Needed for PKCS#8 parsing
        //
        Provider bcProvider = Security.getProvider(BouncyCastleProvider.PROVIDER_NAME);
        if (bcProvider == null) {
            Security.addProvider(new BouncyCastleProvider());
        }

        NsqClientConfig config = NsqClientConfig.builder(config()).build();
        List<String> nodes = config.getNodes();

        // Sanity checks
        Preconditions.checkArgument(nodes.size() > 0, "No nodes defined");

        //
        // Topics
        //
        m_topics = new ArrayList();
        JsonArray topics = config().getArray(CONF_KEY_TOPICS, new JsonArray());
        for (int i = 0; i < topics.size(); i++) {
            JsonObject topicConfig = topics.get(i);
            m_topics.add(Topic.create(topicConfig));
        }

        //
        // Producer
        //
        NSQConfig nsqConfig = config.buildNSQConfig(variables());
        m_producer = new NSQProducer();
        m_producer.setConfig(nsqConfig);

        for (String host : nodes) {

            int port = NSQ_HTTP_PORT;
            int pos = host.lastIndexOf(":");

            if (pos != -1) {
                port = Integer.parseInt(host.substring(pos + 1));
                host = host.substring(0, pos);
            }

            m_producer.addAddress(host, port);
        }

        m_producer.start();
    }

    @Override
    protected void stopFilter() {

        if (m_producer != null) {
            m_producer.shutdown();
            m_producer = null;
        }
    }

    @Override
    protected void handleEvent(final JsonObject batchEvent) {

        try {
            if (m_producer != null) {

                logger().trace("Received event: {} batch-size: {}",
                        batchEvent.getString(EVENT_FIELD_ID),
                        batchEvent.getInteger(EVENT_FIELD_BATCH_SIZE, 0));

                //
                // Operate on arrays only (batches)
                //
                JsonArray batch = batchEvent.getArray(EVENT_FIELD_BATCH_EVENTS, new JsonArray());
                if (!batchEvent.containsField(EVENT_FIELD_BATCH_EVENTS)) {
                    batch.addObject(batchEvent);
                }

                int totalSize = 0;
                Map<String, List<byte[]>> topicMessages = new HashMap();
                for (int i = 0; i < batch.size(); i++) {
                    //
                    // Does event match any topic definition
                    //
                    JsonObject event = batch.get(i);

                    for (Nsq.Topic topic : m_topics) {

                        if (topic.isMatch(event)) {

                            String name = String.valueOf(variables().translate(event, topic.getName()));
                            List<byte[]> messages = topicMessages.get(name);
                            if (messages == null) {
                                messages = new ArrayList();
                            }
                            byte[] data = event.toString().getBytes();
                            int size = data.length;
                            totalSize += size;
                            if (size > DEF_MAX_MSG_SIZE) {
                                // NSQ log: E_BAD_MESSAGE PUB message too big
                                logger().warn("Danger! Message size is bigger than the default NSQ max_msg_size: {}",
                                        DEF_MAX_MSG_SIZE);
                            }
                            logger().trace("Message size: {}", size);
                            messages.add(data);
                            topicMessages.put(name, messages);
                            break; // Match found, handle next event
                        }
                    }
                }

                logger().trace("Message body size: {}", totalSize);
                if (totalSize > DEF_MAX_BODY_SIZE) {
                    // NSQ log: E_BAD_BODY MPUB body too big
                    logger().warn("Danger! Total message body size is bigger than the default NSQ max_body_size: {}",
                            DEF_MAX_BODY_SIZE);
                }

                // Publish multiple messages at once / topic
                for (Map.Entry<String, List<byte[]>> pair : topicMessages.entrySet()) {

                    String topic = pair.getKey();
                    List<byte[]> messages = pair.getValue();

                    m_producer.produceMulti(topic, messages);
                }
            }
        } catch (TimeoutException | NSQException e1) {
            logger().error("Failed to publish event: {}",
                    batchEvent.getString(EVENT_FIELD_ID), e1);
            try {
                if (m_producer != null) {
                    logger().info("Restaring NSQ producer");
                    m_producer.shutdown();
                    m_producer.start();
                }
            } catch (Exception e2) {
                logger().error("Failed to restart NSQ prducer", e2);
            }
        }
    }

    private static class Topic {

        private final String m_name;
        private final Rule m_rule;

        private static final String CONF_KEY_NAME = "name";
        private static final String CONF_KEY_RULE = "rule";

        private static final String RULE_KEY_ACTION = "modifier";
        private static final String RULE_KEY_SCHEDULE = "schedule";

        private Topic(
                final String name,
                final Rule rule) {

            m_name = name;
            m_rule = rule;
        }

        public String getName() {
            return m_name;
        }

        private boolean isMatch(final JsonObject event) {
            return m_rule.match(event);
        }

        @Override
        public String toString() {
            StringBuilder sb = new StringBuilder("Topic: ");
            sb.append(m_name);
            sb.append("rule: ");
            sb.append(m_rule);
            return sb.toString();
        }

        private static Topic create(final JsonObject config) {

            String name = config.getString(CONF_KEY_NAME);

            //
            // Sanity checks
            // 
            org.msgpack.core.Preconditions.checkArgument(!Strings.isNullOrEmpty(name),
                    "topic name is null or empty");

            // Rule
            JsonObject ruleConfig = config.getObject(CONF_KEY_RULE, new JsonObject());
            ruleConfig.putString(RULE_KEY_ACTION, "*");
            ruleConfig.putString(RULE_KEY_SCHEDULE, "*");

            return new Topic(
                    name,
                    Rule.create(name, ruleConfig)
            );
        }
    }
}
