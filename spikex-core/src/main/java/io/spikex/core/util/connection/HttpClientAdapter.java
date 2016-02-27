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

import org.vertx.java.core.Handler;
import org.vertx.java.core.http.HttpClient;
import org.vertx.java.core.http.HttpClientRequest;
import org.vertx.java.core.http.HttpClientResponse;

/**
 *
 * @author cli
 */
public abstract class HttpClientAdapter implements Handler<HttpClient> {

    private final IConnection<HttpClient> m_connection;

    public HttpClientAdapter(final IConnection<HttpClient> connection) {
        m_connection = connection;
    }

    public final IConnection<HttpClient> getConnection() {
        return m_connection;
    }

    @Override
    public void handle(final HttpClient client) {
        // Trigger request
        doRequest(client);
    }

    protected abstract void doRequest(final HttpClient client);

    protected HttpClientRequest doGet(
            final String uri,
            final Handler<HttpClientResponse> handler) {

        HttpClient client = m_connection.getClient();
        return client.get(uri, handler);
    }

    protected HttpClientRequest doPut(
            final String uri,
            final Handler<HttpClientResponse> handler) {

        HttpClient client = m_connection.getClient();
        return client.put(uri, handler);
    }

    protected HttpClientRequest doPost(
            final String uri,
            final Handler<HttpClientResponse> handler) {

        HttpClient client = m_connection.getClient();
        return client.post(uri, handler);
    }

    protected HttpClientRequest doDelete(
            final String uri,
            final Handler<HttpClientResponse> handler) {

        HttpClient client = m_connection.getClient();
        return client.delete(uri, handler);
    }

    protected HttpClientRequest doHead(
            final String uri,
            final Handler<HttpClientResponse> handler) {

        HttpClient client = m_connection.getClient();
        return client.head(uri, handler);
    }

    protected HttpClientRequest doOptions(
            final String uri,
            final Handler<HttpClientResponse> handler) {

        HttpClient client = m_connection.getClient();
        return client.options(uri, handler);
    }

    protected HttpClientRequest doConnect(
            final String uri,
            final Handler<HttpClientResponse> handler) {

        HttpClient client = m_connection.getClient();
        return client.connect(uri, handler);
    }

    protected HttpClientRequest doTrace(
            final String uri,
            final Handler<HttpClientResponse> handler) {

        HttpClient client = m_connection.getClient();
        return client.trace(uri, handler);
    }

    protected HttpClientRequest doPatch(
            final String uri,
            final Handler<HttpClientResponse> handler) {

        HttpClient client = m_connection.getClient();
        return client.patch(uri, handler);
    }

    protected void disconnect() {
        m_connection.disconnect();
    }
}
