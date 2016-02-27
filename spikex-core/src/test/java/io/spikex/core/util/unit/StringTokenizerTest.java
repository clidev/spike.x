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
package io.spikex.core.util.unit;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import io.spikex.core.util.StringTokenizer;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * StringTokenizer tester.
 * <p>
 * @author cli
 */
public class StringTokenizerTest {

    private final Logger m_logger = LoggerFactory.getLogger(StringTokenizerTest.class);

    @Test
    public void testTokenization() throws IOException, InterruptedException {

        byte[] bytes = Files.readAllBytes(Paths.get("build/resources/test/pg-the-land-that-time-forgot.txt").toAbsolutePath());
        String text = new String(bytes, StandardCharsets.UTF_8);
        String[] words = StringTokenizer.tokenize(text, " ");
        m_logger.info("Found approx. {} words", words.length);
        String[] lines = StringTokenizer.tokenize(text, "\n");
        m_logger.info("Found approx. {} lines", lines.length);
    }
}
