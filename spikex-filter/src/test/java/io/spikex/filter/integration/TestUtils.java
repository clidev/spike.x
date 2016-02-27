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

import java.io.File;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author cli
 */
public class TestUtils {

    private static final Logger m_logger = LoggerFactory.getLogger(TestUtils.class);

    // http://stackoverflow.com/questions/7768071/how-to-delete-directory-content-in-java
    public static void deleteFolder(File folder) {
        m_logger.info("Cleaning up folder: {}", folder);
        File[] files = folder.listFiles();
        if (files != null) { //some JVMs return null for empty dirs
            for (File f : files) {
                if (f.isDirectory()) {
                    m_logger.info("Removing dir: {}", f);
                    deleteFolder(f);
                } else {
                    m_logger.info("Removing file: {}", f);
                    f.delete();
                }
            }
        }
        folder.delete();
    }

}
