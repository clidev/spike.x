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
package io.spikex.filter.integration;

import static io.spikex.core.AbstractFilter.CONF_KEY_CHAIN_NAME;
import static io.spikex.core.AbstractFilter.CONF_KEY_SOURCE_ADDRESS;
import static io.spikex.core.AbstractVerticle.CONF_KEY_CLUSTER_NAME;
import static io.spikex.core.AbstractVerticle.CONF_KEY_CONF_PATH;
import static io.spikex.core.AbstractVerticle.CONF_KEY_DATA_PATH;
import static io.spikex.core.AbstractVerticle.CONF_KEY_HOME_PATH;
import static io.spikex.core.AbstractVerticle.CONF_KEY_LOCAL_ADDRESS;
import static io.spikex.core.AbstractVerticle.CONF_KEY_NODE_NAME;
import static io.spikex.core.AbstractVerticle.CONF_KEY_TMP_PATH;
import static io.spikex.core.AbstractVerticle.CONF_KEY_USER;
import io.spikex.core.util.HostOs;
import io.spikex.filter.output.Elasticsearch;
import junit.framework.Assert;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsRequest;
import org.elasticsearch.action.count.CountResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.settings.ImmutableSettings;
import static org.elasticsearch.index.query.QueryBuilders.termQuery;
import org.elasticsearch.node.Node;
import org.elasticsearch.node.NodeBuilder;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.vertx.java.core.AsyncResult;
import org.vertx.java.core.AsyncResultHandler;
import org.vertx.java.core.Handler;
import org.vertx.java.core.json.JsonArray;
import org.vertx.java.core.json.JsonObject;
import org.vertx.testtools.TestVerticle;
import org.vertx.testtools.VertxAssert;

/**
 * Elasticsearch tester.
 *
 * @author cli
 */
public class ElasticsearchTest extends TestVerticle implements Handler<Long> {

    private Node m_node;
    private Client m_client;
    private long m_timerId;
    private int m_iter = 0;
    private int m_maxIterCount = 0;

    private static final String EVENT_SRC_ADDRESS = "elasticsearch-src";
    private static final String INDEX_TYPE = "blue";
    private static final String INDEX_SELECTOR = "spikex";
    private static final String INDEX_TEMPLATE = "logstash-test";
    private static final int INDEX_COUNT = 10000;

    private final Logger m_logger = LoggerFactory.getLogger(ElasticsearchTest.class);

    @Override
    public void start() {
        // Start embedded Elasticsearch node
        ImmutableSettings.Builder settings = ImmutableSettings.settingsBuilder();
        settings.put("node.name", "test-node");
        settings.put("path.data", "build/data");

        m_node = NodeBuilder.nodeBuilder()
                .settings(settings)
                .clusterName("test-cluster")
                .data(true)
                .local(true)
                .node();
        m_client = m_node.client();

        // Start polling for index count
        m_timerId = vertx.setPeriodic(1000L, this);
        // Start tests
        super.start();
    }

    @Override
    public void stop() {
        // Stop timer
        vertx.cancelTimer(m_timerId);
        // Stop embedded Elasticsearch node
        if (m_client != null) {
            m_client.close();
        }
        if (m_node != null) {
            m_node.close();
        }
        // Stop tests
        super.stop();
    }

    @Test
    public void testIndexing() {

        m_iter = 0;
        m_maxIterCount = 10;

        // Elasticsearch configuration
        final JsonObject config = createBaseConfig();
        // Extra ports (9234 and 9299) to verify that test continues if only one port works
        config.putArray("nodes", new JsonArray(
                new String[]{"http://localhost:31234", "http://localhost:9200", "http://localhost:19239"}));
        config.putString("index-selector", INDEX_SELECTOR);
        config.putString("index-type", INDEX_TYPE);
        config.putString("template-name", INDEX_TEMPLATE);
        config.putBoolean("template-update", true);
        config.putNumber("bulk-size", 2000);

        // Load balancing
        JsonObject lb = new JsonObject();
        lb.putString("strategy", "round-robin");
        lb.putString("status-uri", "/_cluster/state/version");
        lb.putNumber("check-interval", 3000L);
        config.putObject("load-balancing", lb);

        container.deployVerticle(Elasticsearch.class.getName(), config, 1,
                new AsyncResultHandler<String>() {
                    @Override
                    public void handle(final AsyncResult<String> ar) {
                        if (ar.failed()) {
                            m_logger.error("Failed to deploy verticle", ar.cause());
                            Assert.fail();
                        }
                    }
                }
        );
    }

    @Override
    public void handle(final Long timerId) {
        //
        // Generate some test events
        //         
        for (int i = 0; i < 100; i++) {
            JsonObject event = EventCreator.createBatch("Elasticsearch");
            vertx.eventBus().publish(EVENT_SRC_ADDRESS, event);
        }
        //
        // Verify index creation
        //
        boolean exists = m_client.admin()
                .indices()
                .exists(new IndicesExistsRequest(INDEX_SELECTOR))
                .actionGet()
                .isExists();

        if (exists) {

            CountResponse countResp = m_client.prepareCount(INDEX_SELECTOR)
                    .setQuery(termQuery("_type", INDEX_TYPE))
                    .execute()
                    .actionGet();

            long count = countResp.getCount();
            if (count >= INDEX_COUNT) {
                //
                // Success
                //
                m_logger.info("Created {} indexes", count);
                VertxAssert.testComplete();

            } else {
                if (m_iter++ > m_maxIterCount) {

                    Assert.assertEquals("Failed to create " + INDEX_COUNT
                            + " indexes", INDEX_COUNT, countResp.getCount());

                    // Stop test
                    VertxAssert.testComplete();
                }
            }
        }
    }

    private JsonObject createBaseConfig() {
        JsonObject config = new JsonObject();
        config.putString(CONF_KEY_CHAIN_NAME, "elastichsearch-test");
        config.putString(CONF_KEY_LOCAL_ADDRESS, "my-local-address");
        config.putString(CONF_KEY_SOURCE_ADDRESS, EVENT_SRC_ADDRESS);
        config.putString(CONF_KEY_NODE_NAME, "node-name");
        config.putString(CONF_KEY_CLUSTER_NAME, "cluster-name");
        config.putString(CONF_KEY_HOME_PATH, "build");
        config.putString(CONF_KEY_CONF_PATH, "build/resources/test");
        config.putString(CONF_KEY_DATA_PATH, "build");
        config.putString(CONF_KEY_TMP_PATH, "build");
        config.putString(CONF_KEY_USER, "spikex");
        m_logger.info("Host operating system: {}", HostOs.operatingSystemFull());
        return config;
    }
}
