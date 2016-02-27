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
package io.spikex.notifier.internal;

import io.spikex.core.helper.Variables;
import io.spikex.core.util.Base64;
import io.spikex.core.util.connection.DefaultConnectionExceptionHandler;
import io.spikex.core.util.connection.HttpClientAdapter;
import io.spikex.core.util.connection.HttpClientResponseAdapter;
import io.spikex.core.util.connection.HttpConnection;
import io.spikex.core.util.connection.IConnection;
import io.spikex.notifier.NotifierConfig.FlowdockDef;
import java.net.URI;
import java.net.URISyntaxException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.vertx.java.core.Vertx;
import org.vertx.java.core.http.HttpClient;
import org.vertx.java.core.http.HttpClientRequest;
import org.vertx.java.core.http.HttpClientResponse;
import org.vertx.java.core.http.HttpHeaders;
import static org.vertx.java.core.http.HttpHeaders.CONTENT_LENGTH;
import static org.vertx.java.core.http.HttpHeaders.CONTENT_TYPE;

/**
 *
 * @author cli
 */
public final class Flowdock {

    private final FlowdockDef m_config;
    private final Variables m_variables;
    private final Vertx m_vertx;

    public Flowdock(
            final FlowdockDef config,
            final Variables variables,
            final Vertx vertx) {

        m_config = config;
        m_variables = variables;
        m_vertx = vertx;
    }

    public void publish(
            final URI destUri,
            final String subject,
            final String body,
            final String priority) throws URISyntaxException {

        URI apiUri = new URI(
                "https",
                destUri.getRawSchemeSpecificPart(),
                destUri.getRawFragment());

        IConnection<HttpClient> connection
                = HttpConnection.builder(apiUri, m_vertx).build();

        MessagePublisher handler = new MessagePublisher(
                connection,
                m_config.getApiToken(),
                subject,
                body,
                priority);

        connection.doRequest(handler);
    }

    private static class MessagePublisher extends HttpClientAdapter {

        private final String m_apiToken;
        private final String m_subject;
        private final String m_body;
        private final String m_priority;
        private final Logger m_logger = LoggerFactory.getLogger(MessagePublisher.class);

        private MessagePublisher(
                final IConnection<HttpClient> connection,
                final String apiToken,
                final String subject,
                final String body,
                final String priority) {

            super(connection);
            m_apiToken = apiToken;
            m_subject = subject;
            m_body = body;
            m_priority = priority;
        }

        @Override
        protected void doRequest(final HttpClient client) {

            URI apiUri = getConnection().getAddress();
            HttpClientRequest request = doPost(apiUri.getPath(), new HttpClientResponseAdapter() {

                @Override
                protected void handleFailure(final HttpClientResponse response) {
                    if (response.statusCode() != 201) {
                        m_logger.trace("{}", m_body);
                        m_logger.error("Failed to publish event to Flowdock: {} (host: {})",
                                m_subject,
                                getConnection().getAddress(),
                                new IllegalStateException("HTTP post failure: "
                                        + response.statusCode()
                                        + "/"
                                        + response.statusMessage()));
                    }
                }
            });

            request.putHeader(CONTENT_TYPE, "application/json");
            request.putHeader(CONTENT_LENGTH, String.valueOf(m_body.length()));

            // HTTP basic authentication
            request.putHeader(HttpHeaders.AUTHORIZATION, "Basic "
                    + base64Token(m_apiToken));

            request.exceptionHandler(new DefaultConnectionExceptionHandler(
                    getConnection()));

            request.write(m_body);
            request.end();
        }
    }

    private static String base64Token(final String token) {

        StringBuilder sb = new StringBuilder(token);
        sb.append(":");
        sb.append(token);

        return Base64.encodeBytes(sb.toString().getBytes());
    }
}
