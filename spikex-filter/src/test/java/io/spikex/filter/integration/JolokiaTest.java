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
import static io.spikex.core.AbstractVerticle.CONF_KEY_USER;
import io.spikex.core.util.HostOs;
import io.spikex.core.util.resource.ResourceException;
import io.spikex.filter.internal.FiltersConfig;
import java.nio.file.FileSystems;
import java.nio.file.Path;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.vertx.java.core.Handler;
import org.vertx.java.core.eventbus.Message;
import org.vertx.java.core.json.JsonObject;
import org.vertx.testtools.TestVerticle;
import org.vertx.testtools.VertxAssert;

/**
 * @TODO use embedded jetty + jolokia war for testing
 * 
 * @author cli
 */
public class JolokiaTest extends TestVerticle implements Handler<Message<JsonObject>> {

    private static final String FILTER_JOLOKIA_NAME = "Jolokia.in";
    private static final String FILTER_DEST_ADDRESS = "jolokia-events";
    private static final String CONF_NAME = "filters";

    private final Logger m_logger = LoggerFactory.getLogger(JolokiaTest.class);

    @Test
    public void testJolokia() throws ResourceException {
/*        
        vertx.eventBus().registerLocalHandler(FILTER_DEST_ADDRESS, this);

        JsonObject config = createBaseConfig();
        config.mergeIn(loadJolokiaConfig(config));

        // Update interval (how often to read the Jolokia values)
        config.putNumber(CONF_KEY_UPDATE_INTERVAL, 1500L);
        m_logger.info("Update interval: {}", config.getLong(CONF_KEY_UPDATE_INTERVAL));

        container.deployWorkerVerticle(Jolokia.class.getName(), config, 1, false,
                new AsyncResultHandler<String>() {
                    @Override
                    public void handle(final AsyncResult<String> ar) {
                        if (ar.failed()) {
                            m_logger.error("Failed to deploy verticle", ar.cause());
                            Assert.fail();
                        }
                    }
                });
*/
        VertxAssert.testComplete();
    }

    @Override
    public void handle(final Message<JsonObject> message) {
        
        m_logger.info("Received message: {}", message.body().toString());

        // Stop test
        VertxAssert.testComplete();
    }

    private JsonObject loadJolokiaConfig(final JsonObject baseConfig)
            throws ResourceException {

        Path confPath = FileSystems.getDefault().getPath(
                baseConfig.getString(CONF_KEY_CONF_PATH));

        FiltersConfig.FilterDef filterDef = null;
        FiltersConfig config = new FiltersConfig(CONF_NAME, confPath);
        config.load();
        config.logInputOutputDef();

        for (FiltersConfig.ChainDef chain : config.getChains()) {
            for (FiltersConfig.FilterDef filter : chain.getFilters()) {
                if (FILTER_JOLOKIA_NAME.equalsIgnoreCase(filter.getAlias())) {
                    filterDef = filter;
                    break;
                }
            }
        }

        junit.framework.Assert.assertNotNull("Could not find Ews filter from "
                + confPath, filterDef);

        String verticle = filterDef.getVerticle();
        JsonObject ewsConfig = filterDef.getJsonConfig();

        // Output address of filter
        ewsConfig.putString(CONF_KEY_DEST_ADDRESS, FILTER_DEST_ADDRESS);

        // Filter local address
        ewsConfig.putString(CONF_KEY_LOCAL_ADDRESS, verticle);

        return ewsConfig;
    }

    private JsonObject createBaseConfig() {
        JsonObject config = new JsonObject();
        config.putString(CONF_KEY_CHAIN_NAME, "ews-test");
        config.putString(CONF_KEY_LOCAL_ADDRESS, "my-local-address");
        config.putString(CONF_KEY_NODE_NAME, "node-name");
        config.putString(CONF_KEY_CLUSTER_NAME, "cluster-name");
        config.putString(CONF_KEY_HOME_PATH, "build/ews");
        config.putString(CONF_KEY_CONF_PATH, "build/resources/test");
        config.putString(CONF_KEY_DATA_PATH, "build/ews/data");
        config.putString(CONF_KEY_TMP_PATH, "build/ews/tmp");
        config.putString(CONF_KEY_USER, "spikex");
        m_logger.info("Host operating system: {}", HostOs.operatingSystemFull());
        return config;
    }
}
