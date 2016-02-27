/**
 *
 * Copyright (c) 2015 NG Modular Oy.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */
package io.spikex.core.util.resource.unit;

import com.google.common.eventbus.Subscribe;
import java.net.URI;
import java.nio.file.Paths;
import java.nio.file.StandardWatchEventKinds;
import java.nio.file.WatchEvent;
import java.util.List;
import java.util.Map;
import junit.framework.Assert;
import io.spikex.core.util.NioDirWatcher;
import io.spikex.core.util.Version;
import io.spikex.core.util.resource.ResourceChangeEvent;
import io.spikex.core.util.resource.YamlDocument;
import io.spikex.core.util.resource.YamlResource;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * YamlResource test driver.
 *
 * @author cli
 */
public class YamlResourceTest {

    private final Logger m_logger = LoggerFactory.getLogger(YamlResourceTest.class);

    @Subscribe
    public void handleWatchEvent(final WatchEvent event) {
        m_logger.info("Received watch event: {} for context: {}",
                event.kind(), event.context());
    }

    @Subscribe
    public void handleResourceEvent(final ResourceChangeEvent event) {
        m_logger.info("Received change event: {} for resource: {}",
                event.getState(), event.getLocation());
    }

    @Test
    public void testReadAndWriteYaml() throws Exception {
        // Base directory
        URI base = Paths.get("build/resources/test").toAbsolutePath().toUri();

        // Watch base directory
        NioDirWatcher watcher = new NioDirWatcher();
        watcher.register(this);
        watcher.watch(Paths.get(base),
                new WatchEvent.Kind[]{
                    StandardWatchEventKinds.ENTRY_CREATE,
                    StandardWatchEventKinds.ENTRY_MODIFY
                });

        // Load resource
        String name = "hornetq-config";
        YamlResource resource = YamlResource.builder(base)
                .name(name)
                .version(Version.latest(name))
                .listeners(this)
                .build()
                .load();

        YamlDocument config = resource.getData().get(0);
        Map global = config.getMap("global");
        Assert.assertEquals(false, (boolean) global.get("clustered"));
        Assert.assertEquals("io.spikex.hornetq.Slf4jLogDelegateFactory",
                global.get("log-delegate-factory-class-name"));
        Assert.assertEquals(12800, (int) global.get("min-large-message-size"));

        global.put("min-large-message-size", 24800);
        config.setValue("global", global); // Fire off CHANGED event

        resource = resource.save();
        resource = resource.load();

        config = resource.getData().get(0);
        global = config.getValue("global");
        Assert.assertEquals(24800, (int) global.get("min-large-message-size"));
    }

    @Test
    public void testComplexValues() throws Exception {
        // Base directory
        URI base = Paths.get("build/resources/test").toAbsolutePath().toUri();

        // Load resource
        String name = "hornetq-config";
        YamlResource resource = YamlResource.builder(base)
                .name(name)
                .version(Version.latest(name))
                .build()
                .load();

        YamlDocument config = resource.getData().get(0);
        YamlDocument confGlobal = config.getDocument("global");
        Assert.assertEquals(false, (boolean) confGlobal.getValue("clustered"));

        List<Map> queues = config.getList("queues");
        Map queue = queues.get(0);
        Assert.assertEquals("core.testQueue", queue.get("address"));
        Assert.assertEquals("color='red'", queue.get("filter"));
        Assert.assertEquals(true, (boolean) queue.get("durable"));

        List<Map> connectors = config.getList("connectors");
        for (Map connector : connectors) {
            name = (String) connector.get("name");
            Assert.assertEquals(true, name.length() > 0);

            if (connector.containsKey("client-failure-check-period")) {
                int period = (int) connector.get("client-failure-check-period");
                Assert.assertEquals(30000, period);
            }
        }
    }
}
