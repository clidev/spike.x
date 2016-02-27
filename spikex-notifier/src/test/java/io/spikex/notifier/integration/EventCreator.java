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
package io.spikex.notifier.integration;

import com.eaio.uuid.UUID;
import static io.spikex.core.helper.Events.EVENT_FIELD_BATCH_EVENTS;
import static io.spikex.core.helper.Events.EVENT_FIELD_BATCH_SIZE;
import static io.spikex.core.helper.Events.EVENT_FIELD_HOST;
import static io.spikex.core.helper.Events.EVENT_FIELD_ID;
import static io.spikex.core.helper.Events.EVENT_FIELD_MESSAGE;
import static io.spikex.core.helper.Events.EVENT_FIELD_PRIORITY;
import static io.spikex.core.helper.Events.EVENT_FIELD_SOURCE;
import static io.spikex.core.helper.Events.EVENT_FIELD_TAGS;
import static io.spikex.core.helper.Events.EVENT_FIELD_TIMESTAMP;
import static io.spikex.core.helper.Events.EVENT_FIELD_TIMEZONE;
import static io.spikex.core.helper.Events.EVENT_FIELD_TITLE;
import static io.spikex.core.helper.Events.EVENT_FIELD_TYPE;
import static io.spikex.core.helper.Events.EVENT_PRIORITY_NORMAL;
import io.spikex.core.util.HostOs;
import java.time.ZoneId;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import org.vertx.java.core.json.JsonArray;
import org.vertx.java.core.json.JsonObject;

/**
 *
 * @author cli
 */
public class EventCreator {

    private static final Random RND = new Random();

    public static JsonObject createBatch(final String testName) {
        return createBatch(testName, null);
    }

    public static JsonObject createBatch(
            final String testName,
            final Map<String, Object> otherFields) {

        Map<String, Object> fields = manfdatoryFields(testName);
        JsonObject batch = create(testName, fields);
        JsonArray batchEvents = new JsonArray();
        int n = RND.nextInt(100) + 1;
        for (int i = 0; i < n; i++) {
            batchEvents.addObject(create(testName, otherFields));
        }
        batch.putArray(EVENT_FIELD_BATCH_EVENTS, batchEvents);
        batch.putNumber(EVENT_FIELD_BATCH_SIZE, n);
        return batch;
    }

    public static JsonObject create(final String testName) {
        return create(testName, null);
    }

    public static JsonObject create(
            final String testName,
            final Map<String, Object> otherFields) {

        Map<String, Object> fields = manfdatoryFields(testName);

        if (otherFields != null) {
            fields.putAll(otherFields);
        }

        // Test fields
        fields.put("path", "/var/log/catalina.log");
        fields.put("BigNumber", Long.MAX_VALUE);
        fields.put("arrayOfStuff", new JsonArray(new String[]{"yellow", "blue", "purple"}));
        fields.put("Light1OnOff", Boolean.TRUE);
        JsonObject json = new JsonObject();
        json.putNumber("int", Integer.MIN_VALUE);
        fields.put("mapOfStuff", json);

        StringBuilder title = new StringBuilder();
        title.append(fields.get(EVENT_FIELD_TYPE));
        title.append(" from ");
        title.append(fields.get(EVENT_FIELD_SOURCE));
        fields.put(EVENT_FIELD_TITLE, title.toString());

        // Random metrics
        fields.put("@cpu.total", 0.0d + RND.nextInt(100));
        fields.put("@io.total", 0.0d + RND.nextInt(100));
        fields.put("@mem.free", 0.0d + RND.nextInt(100));

        return new JsonObject(fields);
    }

    private static Map<String, Object> manfdatoryFields(final String testName) {

        Map<String, Object> fields = new HashMap();

        // Default/mandatory fields
        fields.put(EVENT_FIELD_ID, new UUID().toString());
        fields.put(EVENT_FIELD_SOURCE, testName);
        fields.put(EVENT_FIELD_HOST, HostOs.hostName());
        fields.put(EVENT_FIELD_TIMESTAMP, System.currentTimeMillis());
        fields.put(EVENT_FIELD_TIMEZONE, ZoneId.systemDefault().getId());
        fields.put(EVENT_FIELD_TYPE, "MIX");
        fields.put(EVENT_FIELD_TAGS, new String[]{"spikex", testName});
        fields.put(EVENT_FIELD_PRIORITY, EVENT_PRIORITY_NORMAL); // Default
        fields.put(EVENT_FIELD_MESSAGE,
                "08.11.2010 1:06:46 org.apache.coyote.http11.Http11AprProtocol start\n"
                + "INFO: Starting Coyote HTTP/1.1 on http-8080");

        return fields;
    }
}
