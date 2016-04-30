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
package io.spikex.filter.unit;

import io.spikex.core.util.resource.ResourceException;
import io.spikex.filter.internal.FiltersConfig;
import io.spikex.filter.internal.FiltersConfig.ChainDef;
import io.spikex.filter.internal.FiltersConfig.FilterDef;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.Map;
import junit.framework.Assert;
import org.junit.Test;
import org.vertx.java.core.json.JsonArray;
import org.vertx.java.core.json.JsonObject;

/**
 * Rule tester.
 *
 * @author cli
 */
public class FiltersConfigTest {

    private static final String MODULE = "io.spikex~spikex-filter";
    private static final String UPDATE_INTERVAL = "update-interval";
    private static final String CONF_NAME = "filters";
    private static final String[][] FILTERS = {
        {"Command", "io.spikex.filter.Command"},
        {"Mutate", "io.spikex.filter.Mutate"},
        {"Limit", "io.spikex.filter.Limit"},
        {"Grok", "io.spikex.filter.Grok"},
        {"Batch", "io.spikex.filter.Batch"},
        {"Log.out", "io.spikex.filter.output.Logback"},
        {"Http.in", "io.spikex.filter.input.HttpServer"},
        {"Jolokia.in", "io.spikex.filter.input.Jolokia"},
        {"Collectd.in", "io.spikex.filter.input.Collectd"},
        {"Nsq.in", "io.spikex.filter.input.Nsq"},
        {"Sql.in", "io.spikex.filter.input.Sql"},
        {"Tail.in", "io.spikex.filter.input.TailFile"},
        {"Nsq.out", "io.spikex.filter.output.Nsq"},
        {"NsqHttp.out", "io.spikex.filter.output.NsqHttp"},
        {"Es.out", "io.spikex.filter.output.Elasticsearch"},
        {"InfluxDB.out", "io.spikex.filter.output.InfluxDb"},
        {"Ubidots.out", "io.spikex.filter.output.Ubidots"}    
    };

    @Test
    public void testConfig() throws ResourceException {
        Path confPath = Paths.get("build/resources/test");
        FiltersConfig config = new FiltersConfig(CONF_NAME, confPath);
        Assert.assertTrue(config.hasChanged());
        config.load();

        // Verify filters (module, alias and verticle)
        List<FilterDef> filters = config.getFilters();
        for (int i = 0; i < FILTERS.length; i++) {

            FilterDef filter = filters.get(i);
            Assert.assertEquals(MODULE, filter.getModule());
            Assert.assertEquals(FILTERS[i][0], filter.getAlias());
            Assert.assertEquals(FILTERS[i][1], filter.getVerticle());
        }

        //
        // Verify system chain
        //
        List<ChainDef> chains = config.getChains();
        ChainDef chain = chains.get(0);
        filters = chain.getFilters();

        // Mutate
        FilterDef filter = filters.get(1);
        Assert.assertEquals("Mutate", filter.getAlias());
        Assert.assertFalse(filter.isWorker());
        Assert.assertFalse(filter.isMultiThreaded());
        Assert.assertEquals(1, filter.getInstances());
        Map configMap = filter.getConfig();
        Map addFieldsMap = (Map) configMap.get("add-fields");
        Assert.assertEquals("%{#metric.system.memory.free.perc}",
                addFieldsMap.get("mem.free"));
        List addTagsList = (List) configMap.get("add-tags");
        Assert.assertEquals("system", addTagsList.get(0));
        Assert.assertEquals("mem", addTagsList.get(3));
        Assert.assertTrue(configMap.containsKey("rules"));

        // Test converting to Vert.x Json
        JsonObject json = new JsonObject(configMap);
        json.containsField("rules");
        JsonObject modifiers = json.getElement("modifiers").asObject();
        Assert.assertTrue(modifiers.containsField("low-resource"));

        // Limit
        filter = filters.get(2);
        Assert.assertEquals("Limit", filter.getAlias());
        Assert.assertFalse(filter.isWorker());
        Assert.assertFalse(filter.isMultiThreaded());
        Assert.assertEquals(1, filter.getInstances());
        configMap = filter.getConfig();
        Assert.assertFalse((boolean) configMap.get("discard-on-mismatch"));
        Assert.assertTrue(configMap.containsKey("rules"));
        Assert.assertEquals("spikex-logger", filter.getOutputAddress());

        // Test converting to Vert.x Json
        json = new JsonObject(configMap);
        JsonArray rules = json.getArray("rules");
        JsonObject rule0 = rules.get(0);
        Assert.assertEquals("ALARM", rule0.getString("match-tag"));

        // Output filter input / output addresses
        config.logInputOutputDef();
    }
}
