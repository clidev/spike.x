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

import java.io.IOException;
import io.spikex.core.AbstractFilter;
import io.spikex.filter.internal.DsvLineParser;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.vertx.java.core.json.JsonArray;
import org.vertx.java.core.json.JsonObject;

/**
 * DsvLineParser tester.
 *
 * @author cli
 */
public class DsvLineParserTest {

    private final Logger m_logger = LoggerFactory.getLogger(DsvLineParserTest.class);

    @Test
    public void testMinimal() throws IOException {

        // Config
        JsonObject config = new JsonObject();
        config.putString("delimiter", "<SPACE>");
        config.putBoolean("trunc-dup-delimiters", true);
        JsonObject mapping = new JsonObject();
        JsonArray fields = new JsonArray();
        fields.addArray(new JsonArray("[ \"permission\" ]"));
        fields.addArray(new JsonArray("[ \"inodes\" ]"));
        fields.addArray(new JsonArray("[ \"owner\" ]"));
        fields.addArray(new JsonArray("[ \"group\" ]"));
        fields.addArray(new JsonArray("[ \"size\" ]"));
        fields.addArray(new JsonArray("[ \"modified\" ]"));
        fields.addArray(new JsonArray("[ \"modified\", \"StrReplace(([0-9]+), $1)\" ]"));
        fields.addArray(new JsonArray("[ \"modified\", \"StrReplace(([0-9]+\\\\:[0-9]+), $1)\" ]"));
        fields.addArray(new JsonArray("[ \"file\" ]"));
        mapping.putArray("fields", fields);
        config.putObject("mapping", mapping);

        String[] lines = {
            "drwxr-x---  4 john  users   136 Oct  9 21:06 report.pdf",
            "drwxrwxrwx  114 john  users   1146 Aug  20 13:19 summary.pdf"
        };

        DsvLineParser parser = new DsvLineParser(createFilter(), config);
        JsonObject[] events = parser.parse(lines);
        for (JsonObject event : events) {
            m_logger.info(event.encodePrettily());
        }
    }

    @Test
    public void testLongFormatDate() throws IOException {

        // Config
        JsonObject config = new JsonObject();
        config.putString("delimiter", "<SPACE>");
        config.putBoolean("trunc-dup-delimiters", true);
        JsonObject mapping = new JsonObject();
        JsonArray fields = new JsonArray();
        fields.addArray(new JsonArray("[ \"job\" ]"));
        fields.addArray(new JsonArray("[ \"user\" ]"));
        fields.addArray(new JsonArray("[ \"size\" ]"));
        fields.addArray(new JsonArray("[ \"submitted\" ]"));
        fields.addArray(new JsonArray("[ \"submitted\", \"StrReplace((\\\\w+), $1)\" ]"));
        fields.addArray(new JsonArray("[ \"submitted\", \"StrReplace(([0-9]+), $1)\" ]"));
        fields.addArray(new JsonArray("[ \"submitted\", \"StrReplace(([0-9]+\\\\:[0-9]+\\\\:[0-9]+), $1)\" ]"));
        fields.addArray(new JsonArray("[ \"submitted\", \"StrReplace(([0-9]+), $1)\" ]"));
        mapping.putArray("fields", fields);
        config.putObject("mapping", mapping);

        String[] lines = {
            "Xerox_WorkCentre_6015B-115 userx   453112   Tue Nov 18 21:50:51 2014",
            "Xerox_WorkCentre_6015B-116 usery   683307   Tue Nov 18 21:50:52 2014"
        };

        DsvLineParser parser = new DsvLineParser(createFilter(), config);
        JsonObject[] events = parser.parse(lines);
        for (JsonObject event : events) {
            m_logger.info(event.encodePrettily());
        }
    }

    private static AbstractFilter createFilter() {
        return new AbstractFilter() {
        };
    }
}
