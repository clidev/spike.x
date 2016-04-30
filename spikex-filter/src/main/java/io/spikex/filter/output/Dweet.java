/**
 *
 * Copyright (c) 2016 NG Modular Oy.
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
import static io.spikex.core.helper.Events.EVENT_FIELD_DSNAME;
import static io.spikex.core.helper.Events.EVENT_FIELD_ID;
import static io.spikex.core.helper.Events.EVENT_FIELD_VALUE;
import io.spikex.core.util.connection.AsbtractHttpClient;
import io.spikex.core.util.connection.ConnectionException;
import io.spikex.core.util.connection.DefaultConnectionExceptionHandler;
import io.spikex.core.util.connection.HttpClientAdapter;
import io.spikex.core.util.connection.HttpClientResponseAdapter;
import io.spikex.core.util.connection.IConnection;
import java.util.HashMap;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.vertx.java.core.buffer.Buffer;
import org.vertx.java.core.http.HttpClient;
import org.vertx.java.core.http.HttpClientRequest;
import org.vertx.java.core.http.HttpClientResponse;
import static org.vertx.java.core.http.HttpHeaders.CONTENT_LENGTH;
import static org.vertx.java.core.http.HttpHeaders.CONTENT_TYPE;
import org.vertx.java.core.json.JsonArray;
import org.vertx.java.core.json.JsonObject;

/**
 *
 * @author cli
 */
public class Dweet extends AsbtractHttpClient {
    
    private String m_thingName;
    private String m_dweetPath;
    private Map<String, Object> m_dweets; // We keep state and simplify events for Dweet
    
    private static final String CONF_KEY_THING_NAME = "thing-name";
    private static final String CONF_KEY_DWEET_URI = "dweet-path";

    //
    // Configuration defaults
    //
    private static final String DEF_DWEET_PATH = "/dweet/quietly/for/";
    
    @Override
    protected void startClient() {
        m_thingName = config().getString(CONF_KEY_THING_NAME, "");
        m_dweetPath = config().getString(CONF_KEY_DWEET_URI, DEF_DWEET_PATH);
        m_dweets = new HashMap();
        //
        // Sanity check
        //
        Preconditions.checkArgument(m_thingName.length() > 0, "Dweet thing name is empty");
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

                    //
                    // Operate on arrays only (batches)
                    //
                    JsonArray batch = batchEvent.getArray(EVENT_FIELD_BATCH_EVENTS, new JsonArray());
                    if (!batchEvent.containsField(EVENT_FIELD_BATCH_EVENTS)) {
                        batch.addObject(batchEvent);
                    }

                    // Dweet away
                    for (int i = 0; i < batch.size(); i++) {
                        
                        JsonObject event = batch.get(i);

                        // Dweetify dsname and value for easy access in freeboard
                        m_dweets.put(
                                event.getString(EVENT_FIELD_DSNAME),
                                event.getValue(EVENT_FIELD_VALUE));
                        JsonObject dweet = new JsonObject(m_dweets);
                        
                        DweetPublisher handler = new DweetPublisher(
                                connection,
                                m_thingName,
                                m_dweetPath,
                                dweet);

                        connection.doRequest(handler);
                    }
                }
            }
        } catch (ConnectionException e) {
            logger().error("Failed to publish event: {}",
                    batchEvent.getString(EVENT_FIELD_ID), e);
        }
    }
    
    private static class DweetPublisher extends HttpClientAdapter {
        
        private final String m_thingName;
        private final String m_dweetPath;
        private final JsonObject m_event;
        private final Logger m_logger = LoggerFactory.getLogger(DweetPublisher.class);
        
        private DweetPublisher(
                final IConnection<HttpClient> connection,
                final String thingName,
                final String dweetPath,
                final JsonObject event) {
            
            super(connection);
            m_thingName = thingName;
            m_dweetPath = dweetPath;
            m_event = event;
        }
        
        @Override
        protected void doRequest(final HttpClient client) {
            
            final String uri = m_dweetPath + m_thingName;
            HttpClientRequest request = doPost(uri, new HttpClientResponseAdapter() {
                
                @Override
                protected void handleFailure(final HttpClientResponse response) {
                    if (response.statusCode() != 204) {
                        m_logger.error("Failed to publish event to: {} (host: {})",
                                uri,
                                getConnection().getAddress(),
                                new IllegalStateException("HTTP post failure: "
                                        + response.statusCode()
                                        + "/"
                                        + response.statusMessage()));
                    }
                }
            });
            
            m_logger.trace("Publishing event: {}", m_event);
            byte[] body = m_event.encode().getBytes();
            request.putHeader(CONTENT_TYPE, "application/json");
            request.putHeader(CONTENT_LENGTH, String.valueOf(body.length));
            request.exceptionHandler(new DefaultConnectionExceptionHandler(
                    getConnection()));
            request.write(new Buffer(body));
            request.end();
        }
    }
}
