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

import static io.spikex.core.helper.Events.EVENT_FIELD_BATCH_EVENTS;
import static io.spikex.core.helper.Events.EVENT_FIELD_BATCH_SIZE;
import static io.spikex.core.helper.Events.EVENT_FIELD_ID;
import io.spikex.core.helper.Variables;
import io.spikex.core.util.Version;
import io.spikex.core.util.connection.AsbtractHttpClient;
import io.spikex.core.util.connection.ConnectionException;
import io.spikex.core.util.connection.DefaultConnectionExceptionHandler;
import io.spikex.core.util.connection.HttpClientAdapter;
import io.spikex.core.util.connection.HttpClientResponseAdapter;
import io.spikex.core.util.connection.IConnection;
import io.spikex.core.util.resource.ResourceException;
import io.spikex.core.util.resource.TextResource;
import java.net.URI;
import java.nio.file.Path;
import java.nio.file.Paths;
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
 * <p>
 * This filter has been tested on Linux, Windows, FreeBSD and OS X.
 * <p>
 * Alias: <b>MutateField</b><br>
 * Name: <b>io.spikex.filter.MutateField</b><br>
 * <p>
 * Built-in variables:
 * <p>
 * <ul>
 * <li>#nodeid : the node identifier</li>
 * <li>#node : the node name</li>
 * <li>#clusterid : the cluster identifier</li>
 * <li>#cluster : the cluster name</li>
 * <li>#host : host name as returned by operating system</li>
 * <li>#hostip : the first host ip address as returned by operating system</li>
 * <li>#date : ISO8601 date</li>
 * <li>#timestamp : ISO8601 high-precision timestamp</li>
 * <li>#env.&lt;environment variable name&gt; : environment variable value</li>
 * <li>#sensor.&lt;sensor name&gt; : sensor value</li>
 * </ul>
 * <p>
 * Example:
 * <pre>
 *  "output": {
 *          "Elasticsearch": {
 *                      "nodes": [ "localhost:9200", "remote:9200" ],
 *                      "transport": "http",
 *                      "bulk-size": 10,
 *                      "index": "logstash-%{#segment}-%{+YYYY.MM.dd}",
 *                      "retention-days": 90,
 *                      "template": "file:///home/yuzu/logstash-template.json",
 *                      "template-name": "logstash",
 *                      "template-update": true,
 *                      "match-tags": "*"
 *                      }
 *          }
 * </pre>
 * <p>
 * References:<br>
 * http://www.elasticsearch.org/blog/using-elasticsearch-and-logstash-to-serve-billions-of-searchable-events-for-customers/
 * http://www.elasticsearch.org/blog/new-in-logstash-1-3-elasticsearch-index-template-management/
 * https://gist.github.com/jordansissel/2996677
 * http://untergeek.com/2012/09/20/using-templates-to-improve-elasticsearch-caching-with-logstash/
 *
 * @author cli
 */
public final class Elasticsearch extends AsbtractHttpClient {

    private String m_indexType;
    private String m_indexSelector;

    private static final String CONF_KEY_INDEX_SELECTOR = "index-selector";
    private static final String CONF_KEY_INDEX_TYPE = "index-type";
    private static final String CONF_KEY_TEMPLATE_DIR = "template-dir";
    private static final String CONF_KEY_TEMPLATE_NAME = "template-name";
    private static final String CONF_KEY_TEMPLATE_SUFFIX = "template-suffix";
    private static final String CONF_KEY_TEMPLATE_UPDATE = "template-update";

    //
    // Configuration defaults
    //
    private static final String DEF_INDEX_SELECTOR = "logstash-%{#+YYYY.MM.dd}";
    private static final String DEF_INDEX_TYPE = "%{@type}";
    private static final String DEF_TEMPLATE_DIR = "mapping";
    private static final String DEF_TEMPLATE_NAME = "logstash-template";
    private static final String DEF_TEMPLATE_SUFFIX = ".json";
    private static final boolean DEF_TEMPLATE_UPDATE = false;

    //
    // Elasticsearch URIs
    //
    private static final String ES_STATUS_URI = "/_cluster/state/version";
    private static final String ES_PUBLISH_URI = "/_bulk";
    private static final String ES_TEMPLATE_URI = "/_template";

    @Override
    protected void startClient() {

        m_indexType = config().getString(CONF_KEY_INDEX_TYPE, DEF_INDEX_TYPE);
        m_indexSelector = config().getString(CONF_KEY_INDEX_SELECTOR, DEF_INDEX_SELECTOR);

        //
        // Create/update template
        //
        if (config().getBoolean(CONF_KEY_TEMPLATE_UPDATE, DEF_TEMPLATE_UPDATE)) {

            // Template directory
            Path templatePath = confPath().resolve(DEF_TEMPLATE_DIR);
            String dir = config().getString(CONF_KEY_TEMPLATE_DIR, templatePath.toString());
            URI base = Paths.get(dir).toAbsolutePath().toUri();

            String name = config().getString(CONF_KEY_TEMPLATE_NAME, DEF_TEMPLATE_NAME);
            String suffix = config().getString(CONF_KEY_TEMPLATE_SUFFIX, DEF_TEMPLATE_SUFFIX);

            try {
                // Load resource
                TextResource resource = TextResource.builder(base)
                        .name(name)
                        .suffix(suffix)
                        .version(Version.latest(name))
                        .build()
                        .load();

                // Create or update template in Elasticsearch
                IConnection<HttpClient> connection = connections().next();
                EsTemplateCreator handler = new EsTemplateCreator(connection, name, resource);
                connection.doRequest(handler);

            } catch (ResourceException | ConnectionException e) {
                throw new IllegalStateException("Failed to load or update index template: "
                        + name + suffix, e);
            }
        }
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

                    //
                    // Find next available Elasticsearch host
                    //
                    IConnection<HttpClient> connection = connections().next();
                    EsBulkPublisher handler = new EsBulkPublisher(
                            connection,
                            m_indexType,
                            m_indexSelector,
                            batchEvent,
                            variables());

                    connection.doRequest(handler);
                }
            }
        } catch (ConnectionException e) {
            logger().error("Failed to publish event: {}",
                    batchEvent.getString(EVENT_FIELD_ID), e);
        }
    }

    private static class EsTemplateCreator extends HttpClientAdapter {

        private final String m_templateName;
        private final TextResource m_templateResource;
        private final Logger m_logger = LoggerFactory.getLogger(EsTemplateCreator.class);

        private EsTemplateCreator(
                final IConnection<HttpClient> connection,
                final String templateName,
                final TextResource templateResource) {

            super(connection);
            m_templateName = templateName;
            m_templateResource = templateResource;
        }

        @Override
        protected void doRequest(final HttpClient client) {

            String uri = ES_TEMPLATE_URI + "/" + m_templateName;
            HttpClientRequest request = doPut(uri, new HttpClientResponseAdapter() {

                @Override
                protected void handleSuccess(final HttpClientResponse response) {
                    if (response.statusCode() == 200) {
                        m_logger.debug("Successfully updated template: {} (host: {})",
                                m_templateName, getConnection().getAddress());
                    }
                }

                @Override
                protected void handleFailure(final HttpClientResponse response) {
                    if (response.statusCode() != 200) {
                        m_logger.error("Failed to create or update template: {} (host: {})",
                                m_templateName,
                                getConnection().getAddress(),
                                new IllegalStateException("HTTP post failure: "
                                        + response.statusCode()
                                        + "/"
                                        + response.statusMessage()));
                    }
                }
            });

            String content = m_templateResource.getData();
            byte[] body = content.getBytes();
            request.putHeader(CONTENT_LENGTH, String.valueOf(body.length));
            request.exceptionHandler(new DefaultConnectionExceptionHandler(
                    getConnection()));
            request.write(new Buffer(body));
            request.end();
        }
    }

    private static class EsBulkPublisher extends HttpClientAdapter {

        private final String m_indexType;
        private final String m_indexSelector;
        private final JsonObject m_batchEvent;
        private final Variables m_variables;
        private final Logger m_logger = LoggerFactory.getLogger(EsBulkPublisher.class);

        private EsBulkPublisher(
                final IConnection<HttpClient> connection,
                final String indexType,
                final String indexSelector,
                final JsonObject batchEvent,
                final Variables variables) {

            super(connection);
            m_indexType = indexType;
            m_indexSelector = indexSelector;
            m_batchEvent = batchEvent;
            m_variables = variables;
        }

        @Override
        protected void doRequest(final HttpClient client) {

            String uri = ES_PUBLISH_URI;
            HttpClientRequest request = doPost(uri, new HttpClientResponseAdapter() {

                @Override
                protected void handleFailure(final HttpClientResponse response) {
                    if (response.statusCode() != 200) {
                        m_logger.error("Failed to index events: {} (host: {} index: {})",
                                m_batchEvent.getString(EVENT_FIELD_ID),
                                getConnection().getAddress(),
                                m_indexSelector,
                                new IllegalStateException("HTTP post failure: "
                                        + response.statusCode()
                                        + "/"
                                        + response.statusMessage()));
                    }
                }
            });

            //
            // Operate on arrays only (batches)
            //
            JsonArray batch = m_batchEvent.getArray(EVENT_FIELD_BATCH_EVENTS, new JsonArray());
            if (!m_batchEvent.containsField(EVENT_FIELD_BATCH_EVENTS)) {
                batch.addObject(m_batchEvent);
            }

            //
            // Build bulk of json data to post
            // https://www.elastic.co/guide/en/elasticsearch/reference/current/docs-bulk.html
            //
            StringBuilder bulk = new StringBuilder();
            for (int i = 0; i < batch.size(); i++) {
                //
                // Translate index name and type from event contents (if needed)
                //
                JsonObject event = batch.get(i);
                String index = String.valueOf(m_variables.translate(event, m_indexSelector));
                String type = String.valueOf(m_variables.translate(event, m_indexType));

                // Action and meta data
                bulk.append("{\"index\":{\"_index\":\"");
                bulk.append(index);
                bulk.append("\",\"_type\":\"");
                bulk.append(type);
                bulk.append("\",\"_id\":\"");
                bulk.append(event.getString(EVENT_FIELD_ID));
                bulk.append("\"}}\n");
                // Index data
                bulk.append(event.toString());
                bulk.append("\n");
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
