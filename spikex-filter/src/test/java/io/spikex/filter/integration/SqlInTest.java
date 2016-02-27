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
import static io.spikex.core.AbstractFilter.CONF_KEY_DEST_ADDRESS;
import static io.spikex.core.AbstractVerticle.CONF_KEY_CLUSTER_NAME;
import static io.spikex.core.AbstractVerticle.CONF_KEY_CONF_PATH;
import static io.spikex.core.AbstractVerticle.CONF_KEY_DATA_PATH;
import static io.spikex.core.AbstractVerticle.CONF_KEY_HOME_PATH;
import static io.spikex.core.AbstractVerticle.CONF_KEY_LOCAL_ADDRESS;
import static io.spikex.core.AbstractVerticle.CONF_KEY_NODE_NAME;
import static io.spikex.core.AbstractVerticle.CONF_KEY_TMP_PATH;
import static io.spikex.core.AbstractVerticle.CONF_KEY_UPDATE_INTERVAL;
import static io.spikex.core.AbstractVerticle.CONF_KEY_USER;
import io.spikex.core.util.HostOs;
import io.spikex.core.util.resource.ResourceException;
import io.spikex.filter.input.Sql;
import io.spikex.filter.internal.FiltersConfig;
import java.nio.file.FileSystems;
import java.nio.file.Path;
import junit.framework.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.vertx.java.core.AsyncResult;
import org.vertx.java.core.AsyncResultHandler;
import org.vertx.java.core.Handler;
import org.vertx.java.core.eventbus.Message;
import org.vertx.java.core.json.JsonObject;
import org.vertx.testtools.TestVerticle;
import org.vertx.testtools.VertxAssert;

/**
 * 
 * 
 * @author cli
 */
public class SqlInTest extends TestVerticle implements Handler<Message<JsonObject>> {

    private static final String FILTER_SQL_NAME = "Sql.in";
    private static final String FILTER_DEST_ADDRESS = "sql-events";
    private static final String CONF_NAME = "filters";

    private final Logger m_logger = LoggerFactory.getLogger(SqlInTest.class);

    @Test
    public void testSqlView() throws ResourceException {

        vertx.eventBus().registerLocalHandler(FILTER_DEST_ADDRESS, this);

        JsonObject config = createBaseConfig();
        config.mergeIn(loadSqlConfig(config));

        // Update interval (how often to read the database)
        config.putNumber(CONF_KEY_UPDATE_INTERVAL, 500L);
        m_logger.info("Update interval: {}", config.getLong(CONF_KEY_UPDATE_INTERVAL));

        container.deployWorkerVerticle(Sql.class.getName(), config, 1, false,
                new AsyncResultHandler<String>() {
                    @Override
                    public void handle(final AsyncResult<String> ar) {
                        if (ar.failed()) {
                            m_logger.error("Failed to deploy verticle", ar.cause());
                            Assert.fail();
                        }
                    }
                });
    }

    @Override
    public void handle(final Message<JsonObject> message) {
        m_logger.info("Received message: {}", message.body().toString());

        // Stop test
        VertxAssert.testComplete();
    }

    private JsonObject loadSqlConfig(final JsonObject baseConfig)
            throws ResourceException {

        Path confPath = FileSystems.getDefault().getPath(
                baseConfig.getString(CONF_KEY_CONF_PATH));

        FiltersConfig.FilterDef filterDef = null;
        FiltersConfig config = new FiltersConfig(CONF_NAME, confPath);
        config.load();
        config.logInputOutputDef();

        for (FiltersConfig.ChainDef chain : config.getChains()) {
            for (FiltersConfig.FilterDef filter : chain.getFilters()) {
                if (FILTER_SQL_NAME.equalsIgnoreCase(filter.getAlias())) {
                    filterDef = filter;
                    break;
                }
            }
        }

        junit.framework.Assert.assertNotNull("Could not find Sql filter from "
                + confPath, filterDef);

        String verticle = filterDef.getVerticle();
        JsonObject sqlConfig = filterDef.getJsonConfig();

        // Output address of filter
        sqlConfig.putString(CONF_KEY_DEST_ADDRESS, FILTER_DEST_ADDRESS);

        // Filter local address
        sqlConfig.putString(CONF_KEY_LOCAL_ADDRESS, verticle);

        return sqlConfig;
    }

    private JsonObject createBaseConfig() {
        JsonObject config = new JsonObject();
        config.putString(CONF_KEY_CHAIN_NAME, "sql-test");
        config.putString(CONF_KEY_LOCAL_ADDRESS, "my-local-address");
        config.putString(CONF_KEY_NODE_NAME, "node-name");
        config.putString(CONF_KEY_CLUSTER_NAME, "cluster-name");
        config.putString(CONF_KEY_HOME_PATH, "build/sql");
        config.putString(CONF_KEY_CONF_PATH, "build/resources/test");
        config.putString(CONF_KEY_DATA_PATH, "build/sql/data");
        config.putString(CONF_KEY_TMP_PATH, "build/sql/tmp");
        config.putString(CONF_KEY_USER, "spikex");
        m_logger.info("Host operating system: {}", HostOs.operatingSystemFull());
        return config;
    }
}
