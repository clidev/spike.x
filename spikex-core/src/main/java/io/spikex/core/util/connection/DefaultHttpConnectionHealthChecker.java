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
package io.spikex.core.util.connection;

import io.spikex.core.util.connection.ConnectionConfig.LoadBalancingDef;
import static java.lang.Boolean.FALSE;
import static java.lang.Boolean.TRUE;
import java.nio.charset.StandardCharsets;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.vertx.java.core.Handler;
import org.vertx.java.core.VoidHandler;
import org.vertx.java.core.buffer.Buffer;
import org.vertx.java.core.http.HttpClient;
import org.vertx.java.core.http.HttpClientRequest;
import org.vertx.java.core.http.HttpClientResponse;

/**
 *
 * @author cli
 */
public final class DefaultHttpConnectionHealthChecker
        extends AbstractConnectionHealthChecker<HttpConnection> {

    private static final int HTTP_SUCCESS_CODE = 200;
    private static final int HTTP_REDIRECTION_CODE = 300;
    private static final int HTTP_ERR_CODE = 400;

    private final Logger m_logger = LoggerFactory.getLogger(DefaultHttpConnectionHealthChecker.class);

    public DefaultHttpConnectionHealthChecker(
            final String loadBalancingStrategyName,
            final String statusUri,
            final long checkInterval) {

        super(loadBalancingStrategyName,
                statusUri,
                checkInterval);
    }

    public DefaultHttpConnectionHealthChecker(final LoadBalancingDef lbDef) {
        super(lbDef);
    }

    @Override
    protected void healthCheck(
            HttpConnection connection,
            Handler<Boolean> handler) {

        connection.setConnected(true); // Asumme OK
        doGet(connection, loadBalancingDef().getStatusUri(), handler);
    }

    private void doGet(
            final HttpConnection connection,
            final String getUri,
            final Handler<Boolean> handler) {

        HttpClientAdapter adapter = new HttpClientAdapter(connection) {

            @Override
            protected void doRequest(final HttpClient client) {

                HttpClientRequest request = connection.getClient().get(getUri,
                        new Handler<HttpClientResponse>() {

                            @Override
                            public void handle(final HttpClientResponse response) {
                                httpHealthCheck(connection, response, handler);
                            }
                        });

                Handler<Throwable> exceptionHandler = exceptionHandler();
                if (exceptionHandler == null) {
                    exceptionHandler = new DefaultConnectionExceptionHandler(connection);
                }

                request.exceptionHandler(exceptionHandler);
                request.end();
            }
        };
        connection.doRequest(adapter);
    }

    private void httpHealthCheck(
            final HttpConnection connection,
            final HttpClientResponse response,
            final Handler<Boolean> handler) {

        m_logger.trace("Got response: {}/{}",
                response.statusCode(),
                response.statusMessage());

        if (response.statusCode() < HTTP_ERR_CODE) {

            final Buffer body = new Buffer(0);
            response.dataHandler(new Handler<Buffer>() {
                @Override
                public void handle(final Buffer data) {
                    if (response.statusCode() >= HTTP_SUCCESS_CODE
                            && response.statusCode() < HTTP_REDIRECTION_CODE) {

                        body.appendBuffer(data);
                    }
                }
            });
            response.endHandler(new VoidHandler() {
                @Override
                public void handle() {
                    if (response.statusCode() >= HTTP_SUCCESS_CODE
                            && response.statusCode() < HTTP_REDIRECTION_CODE) {

                        String status = new String(body.getBytes(), StandardCharsets.UTF_8);
                        connection.setConnected(true);
                        if (handler != null) {
                            handler.handle(TRUE);
                        }
                        m_logger.trace("Connected to healthy host: {} ({})",
                                connection.getAddress(), status);
                    } else {
                        handleFailure(connection, response, handler);
                    }
                }
            });
        } else {
            handleFailure(connection, response, handler);
        }
    }

    private void handleFailure(
            final HttpConnection connection,
            final HttpClientResponse response,
            final Handler<Boolean> handler) {

        if (response.statusCode() < HTTP_SUCCESS_CODE
                || response.statusCode() >= HTTP_REDIRECTION_CODE) {

            connection.disconnect();
            if (handler != null) {
                handler.handle(FALSE);
            }
            m_logger.error("Ignoring host: {}",
                    connection.getAddress(),
                    new IllegalStateException("Connection failure: "
                            + response.statusCode()
                            + "/"
                            + response.statusMessage()));
        }
    }
}
