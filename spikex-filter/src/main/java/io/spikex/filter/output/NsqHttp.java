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

import com.google.common.base.Preconditions;
import static io.spikex.core.helper.Events.EVENT_FIELD_BATCH_EVENTS;
import static io.spikex.core.helper.Events.EVENT_FIELD_BATCH_SIZE;
import static io.spikex.core.helper.Events.EVENT_FIELD_ID;
import io.spikex.core.util.connection.AsbtractHttpClient;
import io.spikex.core.util.connection.ConnectionException;
import io.spikex.core.util.connection.DefaultConnectionExceptionHandler;
import io.spikex.core.util.connection.HttpClientAdapter;
import io.spikex.core.util.connection.HttpClientResponseAdapter;
import io.spikex.core.util.connection.IConnection;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.vertx.java.core.buffer.Buffer;
import org.vertx.java.core.http.HttpClient;
import org.vertx.java.core.http.HttpClientRequest;
import org.vertx.java.core.http.HttpClientResponse;
import static org.vertx.java.core.http.HttpHeaders.CONTENT_LENGTH;
import org.vertx.java.core.json.JsonArray;
import org.vertx.java.core.json.JsonObject;

/**
 *
 * @author cli
 */
public class NsqHttp extends AsbtractHttpClient {

    private String m_topicSelector;

    private static final String CONF_KEY_TOPIC_SELECTOR = "topic-selector";
    private static final String CONF_KEY_NSQ_CLIENT_ENGINE = "nsq-client-engine";

    //
    // Configuration defaults
    //
    private static final String DEF_TOPIC_SELECTOR = "%{@nsq.topic}";
    private static final String DEF_NSQ_CLIENT_ENGINE = "JavaNSQClient";

    //
    // NSQ URIs
    //
    private static final String NSQ_STATUS_URI = "/info";
    private static final String NSQ_PUBLISH_URI = "/mpub";

    @Override
    protected void startClient() {
        m_topicSelector = config().getString(CONF_KEY_TOPIC_SELECTOR, DEF_TOPIC_SELECTOR);
        //
        // Sanity check
        //
        Preconditions.checkArgument(m_topicSelector.length() > 0, "Topic selector is empty");
    }

    @Override
    protected void handleEvent(final JsonObject batchEvent) {

        try {
            if (isStarted()) {

                int available = connections().getAvailableCount();
                logger().trace("Received event: {} batch-size: {} available servers: {}",
                        batchEvent.getString(EVENT_FIELD_ID),
                        batchEvent.getInteger(EVENT_FIELD_BATCH_SIZE, 0),
                        available);

                if (available > 0) {

                    IConnection<HttpClient> connection = connections().next();

                    // Group posts per topic
                    Map<String, List<JsonObject>> topics = groupEventsPerTopic(batchEvent, m_topicSelector);
                    for (Map.Entry<String, List<JsonObject>> pair : topics.entrySet()) {

                        String topic = pair.getKey();
                        List<JsonObject> events = pair.getValue();

                        NsqTopicPublisher handler = new NsqTopicPublisher(
                                connection,
                                topic,
                                events);

                        connection.doRequest(handler);
                    }
                }
            }
        } catch (ConnectionException e) {
            logger().error("Failed to publish event: {}",
                    batchEvent.getString(EVENT_FIELD_ID), e);
        }
    }

    private Map<String, List<JsonObject>> groupEventsPerTopic(
            final JsonObject batchEvent,
            final String topicSelector) {
        //
        // Operate on arrays only (batches)
        //
        JsonArray batch = batchEvent.getArray(EVENT_FIELD_BATCH_EVENTS, new JsonArray());
        if (!batchEvent.containsField(EVENT_FIELD_BATCH_EVENTS)) {
            batch.addObject(batchEvent);
        }
        Map<String, List<JsonObject>> topics = new HashMap();

        for (int i = 0; i < batch.size(); i++) {

            JsonObject event = batch.get(i);
            String topic = String.valueOf(variables().translate(event, topicSelector));
            List<JsonObject> events = topics.get(topic);

            if (events == null) {
                events = new ArrayList();
            }

            events.add(event);
            topics.put(topic, events);
        }
        return topics;
    }

    private static class NsqTopicPublisher extends HttpClientAdapter {

        private final String m_topic;
        private final List<JsonObject> m_events;
        private final Logger m_logger = LoggerFactory.getLogger(NsqTopicPublisher.class);

        private NsqTopicPublisher(
                final IConnection<HttpClient> connection,
                final String topic,
                final List<JsonObject> events) {

            super(connection);
            m_topic = topic;
            m_events = events;
        }

        @Override
        protected void doRequest(final HttpClient client) {

            String uri = NSQ_PUBLISH_URI + "?topic=" + m_topic;
            HttpClientRequest request = doPost(uri, new HttpClientResponseAdapter() {

                @Override
                protected void handleFailure(final HttpClientResponse response) {
                    if (response.statusCode() != 200) {
                        m_logger.error("Failed to publish events to topic: {} (host: {})",
                                m_topic,
                                getConnection().getAddress(),
                                new IllegalStateException("HTTP post failure: "
                                        + response.statusCode()
                                        + "/"
                                        + response.statusMessage()));
                    }
                }
            });

            StringBuilder bulk = new StringBuilder();
            for (JsonObject event : m_events) {
                bulk.append(event.toString());
                bulk.append("\n"); // http://nsq.io/components/nsqd.html#mpub
            }

            byte[] body = bulk.toString().getBytes();
            request.putHeader(CONTENT_LENGTH, String.valueOf(body.length));
            request.exceptionHandler(new DefaultConnectionExceptionHandler(
                    getConnection()));
            request.write(new Buffer(body));
            request.end();
        }
    }
}
