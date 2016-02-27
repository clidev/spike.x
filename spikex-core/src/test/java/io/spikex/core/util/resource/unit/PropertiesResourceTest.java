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
import java.util.Iterator;
import java.util.Locale;
import java.util.Properties;
import junit.framework.Assert;
import io.spikex.core.util.IVersion;
import io.spikex.core.util.Version;
import io.spikex.core.util.resource.CyclicVersionStrategy;
import io.spikex.core.util.resource.IncreasingVersionStrategy;
import io.spikex.core.util.resource.UnchangingVersionStrategy;
import io.spikex.core.util.resource.PropertiesResource;
import io.spikex.core.util.resource.ResourceChangeEvent;
import io.spikex.core.util.resource.ResourceException;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * PropertyResource test driver.
 *
 * @author cli
 */
public class PropertiesResourceTest {

    private final Logger m_logger = LoggerFactory.getLogger(PropertiesResourceTest.class);

    @Subscribe
    public void handleResourceEvent(final ResourceChangeEvent event) {
        m_logger.info("Received change event: {} for resource: {}",
                event.getState(), event.getLocation());
    }

    @Test
    public void testDirectFileUse() throws Exception {
        // Base directory
        URI base = Paths.get("build/resources/test").toAbsolutePath().toUri();

        // Resource name (part of filename)
        String name = "mainapp";
        Version ver = Version.create(name, 1);
        // Load resource and trigger load event
        PropertiesResource resource = PropertiesResource.builder(base)
                .name(name)
                .version(ver)
                .locale(Locale.UK)
                .listeners(this)
                .build()
                .load();

        // Read some properties
        Properties properties = resource.getData();
        String err1 = properties.getProperty("app.err1");
        Assert.assertEquals("Internal alert", err1);

        // Modify some properties
        properties.setProperty("app.title", "My new title");

        // Verify that key order is retained after load
        Iterator<String> keys = properties.stringPropertyNames().iterator();
        String key1 = keys.next();
        Assert.assertEquals("app.title", key1);
        String key2 = keys.next();
        Assert.assertEquals("app.err1", key2);
        String key3 = keys.next();
        Assert.assertEquals("app.currency.symbol", key3);
        String key4 = keys.next();
        Assert.assertEquals("app.chinese.logograms", key4);

        resource.removeListener(this);
    }

    @Test
    public void testFileUriLoadSave() throws Exception {
        // Base directory
        URI base = Paths.get("build/resources/test").toAbsolutePath().toUri();

        // Resource name (part of filename)
        String name = "testapp";
        Version ver = Version.create(name, 1); // Version 1
        PropertiesResource resource = PropertiesResource.builder(base)
                .name(name)
                .version(ver)
                .locale(Locale.UK)
                .listeners(this)
                .build()
                .load();

        Assert.assertTrue(resource.getLocation().getPath().endsWith("testapp.1.properties"));

        // Read some properties
        Properties properties = resource.getData();
        String title = properties.getProperty("app.title");
        Assert.assertEquals("Color Painter", title);

        // Modify some properties - Version 2
        properties.put("app.version.tm", Long.toString(System.currentTimeMillis()));
        resource = PropertiesResource.builder(base, resource)
                .data(properties)
                .build()
                .save();

        URI location = resource.getLocation();
        Assert.assertTrue(location.toString(), location.getPath().endsWith("testapp_en_GB.2.properties"));

        // Modify some properties - Version 3
        properties.setProperty("app.version.tm", Long.toString(System.currentTimeMillis()));
        properties.setProperty("app.version", "2.0");
        resource = PropertiesResource.builder(base, resource)
                .data(properties)
                .build()
                .save();

        location = resource.getLocation();
        Assert.assertTrue(location.toString(), location.getPath().endsWith("testapp_en_GB.3.properties"));
        resource.removeListener(this);

        // Load latest version and verify content
        ver = Version.latest(name); // Version 3
        resource = PropertiesResource.builder(base)
                .name(name)
                .version(ver)
                .locale(Locale.UK)
                .build()
                .load();

        properties = resource.getData();
        title = properties.getProperty("app.title");
        Assert.assertEquals("Color Painter", title);
        String version = properties.getProperty("app.version");
        Assert.assertEquals("2.0", version);
    }

    @Test
    public void testFileUriSave() throws Exception {
        // Base directory
        URI base = Paths.get("build/resources/test").toAbsolutePath().toUri();

        // Resource name (part of filename)
        String name = "saveapp";
        PropertiesResource resource = PropertiesResource.builder(base)
                .name(name)
                .locale(Locale.UK)
                .strategy(new UnchangingVersionStrategy())
                .build();

        // Add some properties and save first version
        Properties properties = new Properties();
        properties.setProperty("app.title", "Save App #1");
        properties.setProperty("app.version", "1.0");
        resource = PropertiesResource.builder(base, resource)
                .data(properties)
                .version(Version.create(name, 1))
                .build()
                .save();

        Assert.assertEquals(1, resource.getVersion().getSequence());

        // Add some properties and save version 2
        properties = new Properties();
        properties.setProperty("app.title", "Save App #2");
        properties.setProperty("app.version", "2.0");
        resource = PropertiesResource.builder(base, resource)
                .data(properties)
                .version(Version.create(name, 2))
                .build()
                .save();

        Assert.assertEquals(2, resource.getVersion().getSequence());
    }

    @Test
    public void testNonExistenVersion() throws Exception {
        // Base directory
        URI base = Paths.get("build/resources/test").toAbsolutePath().toUri();

        // Resource name (part of filename)
        String name = "mainapp";
        PropertiesResource resource = PropertiesResource.builder(base)
                .name(name)
                .locale(Locale.UK)
                .strategy(new IncreasingVersionStrategy())
                .version(Version.create(name, 123))
                .build();
        try {
            resource.load();
            Assert.fail("Load did not fail for non-existent version");
        } catch (ResourceException e) {
            // OK
        }
    }

    @Test
    public void testNonExistentUri() throws Exception {
        // Base directory
        URI base = new URI("http://dev/null");

        // Resource name (part of filename)
        String name = "config";
        PropertiesResource resource = PropertiesResource.builder(base)
                .name(name)
                .locale(Locale.CANADA)
                .version(Version.latest(name))
                .build();
        try {
            resource.load();
            Assert.fail("Load did not fail for non-existent resource");
        } catch (ResourceException e) {
            // OK
        }
    }

    @Test
    public void testNonVersion() throws Exception {
        // Base directory
        URI base = Paths.get("build/resources/test").toAbsolutePath().toUri();

        // Resource name (part of filename)
        String name = "mainapp";
        Locale loc = new Locale("fi");
        PropertiesResource resource = PropertiesResource.builder(base)
                .name(name)
                .locale(loc)
                .version(Version.create(name, Version.nullVersion()))
                .build()
                .load();

        // Read some properties
        Properties properties = resource.getData();
        String title = properties.getProperty("app.title");
        Assert.assertEquals("Guru Meditaatio", title);
        String err = properties.getProperty("app.err1");
        Assert.assertEquals("Sisäinen hälyytys", err);

        // Modify some properties (save always creates a new version)
        properties.put("app.title", "Testing new title");
        PropertiesResource.builder(base, resource)
                .data(properties)
                .build()
                .save();
    }

    @Test
    public void testUtf8() throws Exception {
        // Base directory
        URI base = Paths.get("build/resources/test").toAbsolutePath().toUri();

        // Resource name (part of filename)
        String name = "mainapp";
        PropertiesResource resource = PropertiesResource.builder(base)
                .name(name)
                .locale(Locale.CHINA)
                .build()
                .load();

        // Read some properties
        Properties properties = resource.getData();
        String err = properties.getProperty("app.err1");
        Assert.assertEquals("内部警报", err);

        // Modify some properties (save always creates a new version)
        properties.put("app.err1", "三個和尚沒水喝");
        resource = PropertiesResource.builder(base, resource)
                .data(properties)
                .build()
                .save();

        // Load latest version and verify contents
        properties = resource.load().getData();
        err = properties.getProperty("app.err1");
        Assert.assertEquals("三個和尚沒水喝", err);
    }

    @Test
    public void testCyclicVersion() throws Exception {
        // Base directory
        URI base = Paths.get("build/resources/test").toAbsolutePath().toUri();

        // Resource name (part of filename)
        String name = "cyclic";
        PropertiesResource resource = PropertiesResource.builder(base)
                .name(name)
                .strategy(new CyclicVersionStrategy())
                .version(Version.create(name, 0))
                .build();

        // Add some properties and save first version
        Properties properties = new Properties();
        properties.setProperty("cyclic.version",
                String.valueOf(resource.getVersionStrategy().getFirstVersion()));
        properties.setProperty("cyclic.color", "rainbow green");
        resource = PropertiesResource.builder(base, resource)
                .data(properties)
                .build()
                .save();
        IVersion ver = resource.getVersion();
        Assert.assertEquals(1, ver.getSequence());

        for (int i = 0; i < 10; i++) {
            // Save second and many more versions (should result in only two files)
            properties.put("cyclic.version", ver.toString());
            properties.put("cyclic.index", String.valueOf(i));
            resource = PropertiesResource.builder(base, resource)
                    .data(properties)
                    .build()
                    .save();
        }
    }

    @Test
    public void testSimpleUse() throws Exception {
        // Base directory
        URI base = Paths.get("build/resources/test").toAbsolutePath().toUri();

        // Resource name (part of filename)
        String name = "myapp";
        Version ver = Version.create(name, 1); // Version 1
        PropertiesResource resource = PropertiesResource.builder(base)
                .name(name)
                .version(ver)
                .listeners(this)
                .build()
                .load();

        Assert.assertTrue(resource.getLocation().getPath().endsWith("myapp.1.properties"));

        // Read property
        String title = resource.getProperty("app.title", "");
        Assert.assertEquals("Color Painter2", title);

        // Modify property
        resource.setProperty("app.title", "Pro Draw Painter");
        resource = resource.save();

        // Read property
        title = resource.getProperty("app.title", "");
        Assert.assertEquals("Pro Draw Painter", title);
        resource.removeListeners();
    }
}
