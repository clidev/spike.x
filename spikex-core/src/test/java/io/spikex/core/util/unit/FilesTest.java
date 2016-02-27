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
package io.spikex.core.util.unit;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import io.spikex.core.util.Files;
import io.spikex.core.util.HostOs;
import org.junit.Assert;
import org.junit.Test;

/**
 * Files tester.
 * <p>
 * @author cli
 */
public class FilesTest {

    @Test
    public void testSymbolicLink() throws IOException {

        Path target1 = Paths.get("build/target1.txt").toAbsolutePath();
        Path target2 = Paths.get("build/target2.txt").toAbsolutePath();

        Path link1 = Paths.get("build/target1.lnk").toAbsolutePath();
        Path link2 = Paths.get("build/target2.lnk").toAbsolutePath();

        java.nio.file.Files.deleteIfExists(target1);
        java.nio.file.Files.deleteIfExists(target2);
        
        Path file1 = java.nio.file.Files.createFile(target1);
        Path file2 = java.nio.file.Files.createFile(target2);

        link1 = Files.createSymbolicLink(file1.toAbsolutePath(), link1);
        link1 = Files.createSymbolicLink(file1.toAbsolutePath(), link1); // Replace existing   
        Assert.assertTrue(java.nio.file.Files.exists(file1));
        Assert.assertTrue(java.nio.file.Files.exists(link1));

        link2 = Files.createSymbolicLink(target2.getFileName(), link2);
        link2 = Files.createSymbolicLink(target2.getFileName(), link2); // Replace existing       
        Assert.assertTrue(java.nio.file.Files.exists(file2));
        Assert.assertTrue(java.nio.file.Files.exists(link2));
    }

    @Test
    public void testCreateDirectories() throws IOException {

        Path dir1 = Paths.get("build/level1/level2/level3").toAbsolutePath();
        Path dir2 = Paths.get("build/dir1/dir2/dir3/dir4/dir5").toAbsolutePath();

        if (HostOs.isUnix()) {

            Files.createUnixDirectories(Files.Permission.OWNER_FULL, dir1);
            Files.createUnixDirectories(Files.Permission.OWNER_FULL, dir1); // Do nothing if exists

            Files.createUnixDirectories(Files.Permission.OWNER_FULL_GROUP_EXEC, dir2);
            Files.createUnixDirectories(Files.Permission.OWNER_FULL_GROUP_EXEC_OTHER_EXEC, dir2); // Do nothing if exists

        } else if (HostOs.isWindows()) {

            Files.createWindowsDirectories(System.getProperty("user.name"), dir1);
            Files.createWindowsDirectories(System.getProperty("user.name"), dir2);

        } else {
            throw new IllegalStateException("Unsupported operating system: "
                    + HostOs.operatingSystem());
        }

        Assert.assertTrue(java.nio.file.Files.exists(dir1));
        Assert.assertTrue(java.nio.file.Files.exists(dir2));
    }

    @Test
    public void testHash() throws IOException {
        String path = "build/resources/test/pg-the-land-that-time-forgot.txt";
        Path textFile = Paths.get(path).toAbsolutePath();
        int hash = Files.hashOfFile(textFile);
        Assert.assertEquals(-1159548822, hash);
    }
}
