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
package io.spikex.core.integration;

import io.spikex.core.util.connection.AsbtractHttpClient;
import io.spikex.core.util.connection.ConnectionException;
import io.spikex.core.util.connection.HttpClientAdapter;
import io.spikex.core.util.connection.HttpClientResponseAdapter;
import io.spikex.core.util.connection.IConnection;
import junit.framework.Assert;
import org.vertx.java.core.buffer.Buffer;
import org.vertx.java.core.http.HttpClient;
import org.vertx.java.core.http.HttpClientRequest;
import org.vertx.java.core.http.HttpClientResponse;
import static org.vertx.java.core.http.HttpHeaders.CONTENT_LENGTH;
import org.vertx.testtools.VertxAssert;

/**
 *
 * @author cli
 */
public final class TestHttpClient extends AsbtractHttpClient {

    private int m_counter;

    @Override
    protected void startClient() {
        try {
            // Initial request
            doPost(
                    connections().next(),
                    "/initial",
                    ++m_counter);
        } catch (ConnectionException e) {
            Assert.fail(e.getMessage());
        }
    }

    @Override
    protected void handleTimerEvent() {
        // Periodic request (if any server is available)
        try {
            if (connections().getAvailableCount() > 0) {
                doPost(
                        connections().next(),
                        "/test",
                        ++m_counter);
            }
        } catch (ConnectionException e) {
            Assert.fail(e.getMessage());
        }
    }

    private void doPost(
            final IConnection<HttpClient> connection,
            final String postUri,
            final int requestNum) {

        HttpClientAdapter adapter = new HttpClientAdapter(connection) {

            private int m_counter;

            @Override
            protected void doRequest(final HttpClient client) {

                HttpClientRequest request = client.post(postUri, new HttpClientResponseAdapter() {
                    @Override
                    protected void handleSuccess(final HttpClientResponse response) {
                        logger().info("handleSuccess");
                        if (requestNum >= 15) {
                            disconnect();
                            // Stop test
                            VertxAssert.testComplete();
                        }
                    }

                    @Override
                    protected void handleFailure(final HttpClientResponse response) {
                        logger().info("handleFailure");
                    }
                });
                String message = "Hello from HTTP client - request: " + requestNum;
                byte[] body = message.getBytes();
                request.putHeader(CONTENT_LENGTH, String.valueOf(body.length));
                request.write(new Buffer(body));
                request.end();
            }
        };

        connection.doRequest(adapter);
    }
}
