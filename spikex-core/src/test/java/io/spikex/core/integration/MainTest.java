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

import java.io.IOException;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.commons.io.FileUtils;
import io.spikex.core.Main;
import io.spikex.core.util.Version;
import io.spikex.core.util.resource.ResourceException;
import io.spikex.core.util.resource.YamlDocument;
import io.spikex.core.util.resource.YamlResource;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 *
 * @author cli
 */
public class MainTest {

    @BeforeClass
    public static void init() throws IOException {
        Path nodeRepoPath1 = Paths.get("build/resources/test/node1/repo");
        Path nodeRepoPath2 = Paths.get("build/resources/test/node2/repo");
        Files.createDirectories(nodeRepoPath1);
        Files.createDirectories(nodeRepoPath2);

        // Copy repo to node home directories
        // The repos are defined in repos.txt that is found on the classpath
        Path repoPath = Paths.get("build/resources/test/repo");
        FileUtils.copyDirectory(repoPath.toFile(), nodeRepoPath1.toFile());
        FileUtils.copyDirectory(repoPath.toFile(), nodeRepoPath2.toFile());
        
//        System.out.println(ClassLoader.getSystemClassLoader().getResource(".").getPath());
    }

    @Test
    public void testNonClustered() throws InterruptedException, IOException {
        Path homePath = Paths.get("build/resources/test/node1");
        System.setProperty("spikex.home", homePath.toAbsolutePath().toString());
        new Main().start(new String[0]);
        Thread.sleep(1000L);
    }

    @Test
    public void testClustered() throws InterruptedException, IOException {
        Path homePath = Paths.get("build/resources/test/node2");
        System.setProperty("spikex.home", homePath.toAbsolutePath().toString());
        new Main().start(new String[0]);
    }

    @Test
    public void testConfModified() throws InterruptedException, ResourceException, IOException {
        Path homePath = Paths.get("build/resources/test/node1");
        System.setProperty("spikex.home", homePath.toAbsolutePath().toString());
        new Main().start(new String[0]);
        //
        // Update Spike.x configuration (simulate update of module)
        //
        Thread.sleep(5500L); // Wait 5.5 seconds - re-init is throttled
        URI base = homePath.resolve("conf").toAbsolutePath().toUri();
        YamlResource confResource = YamlResource.builder(base)
                .name("spikex")
                .version(Version.none())
                .build()
                .load();

        List<Map> modules = new ArrayList();
        Map<String, String> module1 = new HashMap();
        module1.put("id", "io.spikex~spikex-test2~0.8.0");
        modules.add(module1);
        Map<String, String> module2 = new HashMap();
        module2.put("id", "io.spikex~spikex-test1~0.8.0");
        modules.add(module2);

        YamlDocument conf = confResource.getData().get(0);
        conf.setValue("modules", modules);
        confResource.save();
        Thread.sleep(2000L); // Wait a while so that file change is noticed
    }
}
