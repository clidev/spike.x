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

import io.spikex.core.AbstractFilter;
import io.spikex.core.helper.Events;
import static io.spikex.core.helper.Events.EVENT_FIELD_TAGS;
import io.spikex.core.util.HostOs;
import io.spikex.filter.internal.CollectdJsonHandler;
import io.spikex.filter.internal.HttpResponse;
import io.spikex.filter.internal.NagiosNrdpHandler;
import io.spikex.filter.internal.ThingseeHandler;
import org.vertx.java.core.AsyncResult;
import org.vertx.java.core.AsyncResultHandler;
import org.vertx.java.core.Handler;
import org.vertx.java.core.VoidHandler;
import org.vertx.java.core.buffer.Buffer;
import org.vertx.java.core.eventbus.EventBus;
import static org.vertx.java.core.http.HttpHeaders.CONTENT_LENGTH;
import static org.vertx.java.core.http.HttpHeaders.CONTENT_TYPE;
import org.vertx.java.core.http.HttpServerRequest;
import org.vertx.java.core.json.JsonArray;
import org.vertx.java.core.json.JsonObject;

/**
 *
 * @author cli
 */
public final class HttpServer extends AbstractFilter {

    private Handler<HttpResponse> m_handler;
    private org.vertx.java.core.http.HttpServer m_server;

    private static final String CONF_KEY_HOST = "host";
    private static final String CONF_KEY_PORT = "port";
    private static final String CONF_KEY_INPUT_FORMAT = "input-format";
    private static final String CONF_KEY_SSL_ENABLED = "ssl-enabled";
    private static final String CONF_KEY_KEYSTORE_PATH = "keystore-path";
    private static final String CONF_KEY_KEYSTORE_PASSWORD = "keystore-password";
    private static final String CONF_KEY_KEYSTORE_TYPE = "keystore-type";
    private static final String CONF_KEY_TRUSTSTORE_PATH = "truststore-path";
    private static final String CONF_KEY_TRUSTSTORE_PASSWORD = "truststore-password";
    private static final String CONF_KEY_TRUSTSTORE_TYPE = "truststore-type";
    private static final String CONF_KEY_CLIENT_AUTH_REQUIRED = "client-auth-required";
    private static final String CONF_KEY_ADD_TAGS = "add-tags";

    // Input formats
    private static final String INPUT_FORMAT_JSON = "json";
    private static final String INPUT_FORMAT_COLLECTD_JSON = "collectd-json";
    private static final String INPUT_FORMAT_NAGIOS_NRDP = "nagios-nrdp";
    private static final String INPUT_FORMAT_THINGSEE = "thingsee";

    // Configuration defaults
    private static final int DEF_PORT = 44120;
    private static final String DEF_HOST = "localhost";
    private static final String DEF_INPUT_FORMAT = INPUT_FORMAT_JSON;

    @Override
    protected void startFilter() {

        final int port = config().getInteger(CONF_KEY_PORT, DEF_PORT);
        final String host = config().getString(CONF_KEY_HOST, DEF_HOST);
        String format = config().getString(CONF_KEY_INPUT_FORMAT, DEF_INPUT_FORMAT);

        // Tags to add
        JsonArray tags = config().getArray(CONF_KEY_ADD_TAGS, new JsonArray());

        switch (format) {
            case INPUT_FORMAT_JSON: {
                m_handler = new JsonHandler(this, eventBus(), tags);
            }
            break;
            case INPUT_FORMAT_COLLECTD_JSON: {
                CollectdJsonHandler handler = new CollectdJsonHandler(this, config(), eventBus(), tags);
                handler.init();
                m_handler = handler;
            }
            break;
            case INPUT_FORMAT_NAGIOS_NRDP: {
                NagiosNrdpHandler handler = new NagiosNrdpHandler(this, config(), eventBus(), tags);
                handler.init();
                m_handler = handler;
            }
            break;
            case INPUT_FORMAT_THINGSEE: {
                ThingseeHandler handler = new ThingseeHandler(this, config(), eventBus(), tags);
                m_handler = handler;
            }
            break;
            default: {
                m_handler = new JsonHandler(this, eventBus(), tags);
            }
            break;
        }

        m_server = vertx.createHttpServer();
        m_server.requestHandler(new Handler<HttpServerRequest>() {

            @Override
            public void handle(final HttpServerRequest request) {

                final Buffer body = new Buffer(0);

                request.dataHandler(new Handler<Buffer>() {
                    @Override
                    public void handle(final Buffer buffer) {
                        body.appendBuffer(buffer);
                    }
                });
                request.endHandler(new VoidHandler() {
                    @Override
                    public void handle() {
                        // The entire body has now been received (keep it small)
                        String text = body.toString();
                        HttpResponse response = new HttpResponse(
                                request,
                                text);
                        try {
                            logger().trace("Received: {}", text);
                            m_handler.handle(response);

                            if (response.hasContent()) {
                                Buffer content = response.getContent();
                                request.response().putHeader(CONTENT_TYPE, response.getContentType());
                                request.response().putHeader(CONTENT_LENGTH, String.valueOf(content.length()));
                                request.response().end(content);
                            } else {
                                request.response().end();
                            }
                        } catch (Exception e) {
                            logger().error("Failed to parse: {}", text, e);
                            request.response().setStatusCode(500).end();
                        }
                    }
                });
            }
        });

        m_server.listen(port, host,
                new AsyncResultHandler<org.vertx.java.core.http.HttpServer>() {

                    @Override
                    public void handle(AsyncResult<org.vertx.java.core.http.HttpServer> ar) {
                        if (ar.succeeded()) {
                            logger().info("Listening on {}:{}", host, port);
                        } else {
                            logger().error("Failed to bind to {}:{}", host, port);
                        }
                    }
                });
    }

    @Override
    protected void stopFilter() {
        m_server.close();
    }

    private static class JsonHandler implements Handler<HttpResponse> {

        private final AbstractFilter m_filter;
        private final EventBus m_eventBus;
        private final JsonArray m_tags;

        private JsonHandler(
                final AbstractFilter filter,
                final EventBus eventBus,
                final JsonArray tags) {

            m_filter = filter;
            m_eventBus = eventBus;
            m_tags = tags;
        }

        @Override
        public void handle(final HttpResponse response) {

            // Try to parse json and emit new event
            JsonObject data = new JsonObject(response.getBody());
            JsonObject event = Events.createMetricEvent(
                    m_filter,
                    HostOs.hostName(),
                    "-",
                    "-",
                    "-",
                    "-",
                    0,
                    0);

            event.mergeIn(data);

            // Add tags
            event.putArray(EVENT_FIELD_TAGS, m_tags);

            String destAddr = m_filter.getDestinationAddress();
            if (destAddr != null && destAddr.length() > 0) {
                m_eventBus.publish(destAddr, event);
            }
        }
    }
}
