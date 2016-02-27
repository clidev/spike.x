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
package io.spikex.filter.output;

import io.spikex.core.AbstractFilter;
import static io.spikex.core.helper.Events.EVENT_FIELD_BATCH_EVENTS;
import static io.spikex.core.helper.Events.EVENT_FIELD_ID;
import org.slf4j.MDC;
import org.vertx.java.core.json.JsonArray;
import org.vertx.java.core.json.JsonObject;

/**
 * <p>
 * This filter has been tested on Linux, Windows, FreeBSD and OS X.
 * <p>
 * Alias: <b>Log</b><br>
 * Name: <b>io.spikex.filter.output.Logback</b><br>
 * <p>
 * Example:
 * <pre>
 *  {"Log":
 *      {
 *          "configuration": "file:${jelly.conf}/logback.xml",
 *          "match-tags": [ "ERROR", "ALERT" ]
 *      }
 *  }
 * </pre>
 *
 * @author cli
 */
public final class Logback extends AbstractFilter {

    private String m_mdcValue;

    private static final String FIELD_MDC_KEY = "mdc-key";
    private static final String FIELD_MDC_VALUE = "mdc-value";

    //
    // Configuration defaults
    //
    private static final String DEF_MDC_KEY = "event";
    private static final String DEF_MDC_VALUE = "";

    @Override
    protected void startFilter() {
        m_mdcValue = config().getString(FIELD_MDC_VALUE, DEF_MDC_VALUE);
    }

    @Override
    protected void handleEvent(final JsonObject batchEvent) {
        try {
            //
            // Operate on arrays only (batches)
            //
            JsonArray batch = batchEvent.getArray(EVENT_FIELD_BATCH_EVENTS, new JsonArray());
            if (!batchEvent.containsField(EVENT_FIELD_BATCH_EVENTS)) {
                batch.addObject(batchEvent);
            }
            for (int i = 0; i < batch.size(); i++) {

                JsonObject event = batch.get(i);

                // Simply log the event using the specified MDC
                String mdcKey = event.getString(FIELD_MDC_KEY, DEF_MDC_KEY);
                String mdcValue = event.getString(FIELD_MDC_VALUE, m_mdcValue);
                MDC.put(mdcKey, String.valueOf(variables().translate(event, mdcValue)));
                eventLogger().info(event.toString());
            }
        } catch (Exception e) {
            logger().error("Failed to log event: {}",
                    batchEvent.getString(EVENT_FIELD_ID), e);
        }
    }
}
