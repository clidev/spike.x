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
package io.spikex.filter;

import com.gs.collections.impl.list.mutable.FastList;
import io.spikex.core.AbstractFilter;
import io.spikex.core.helper.Events;
import static io.spikex.core.helper.Events.EVENT_FIELD_ID;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import org.vertx.java.core.json.JsonObject;

/**
 *
 * @author cli
 */
public final class Batch extends AbstractFilter {

    private final BlockingQueue m_queue;
    private final List<String> m_events;
    private int m_batchSize;

    private static final String CONF_KEY_MAX_BATCH_SIZE = "max-batch-size";

    //
    // Default configuration values
    //
    private static final int DEF_MAX_BATCH_SIZE = 1000;

    public Batch() {
        m_queue = new LinkedBlockingQueue();
        m_events = FastList.<String>newList();
        m_batchSize = 0;
    }

    @Override
    protected void startFilter() {
        m_batchSize = config().getInteger(CONF_KEY_MAX_BATCH_SIZE,
                DEF_MAX_BATCH_SIZE);
    }

    @Override
    protected void handleEvent(final JsonObject event) {
        logger().trace("Adding event to batch: {}",
                event.getString(EVENT_FIELD_ID, ""));

        BlockingQueue<String> queue = m_queue;
        queue.add(event.toString());
    }

    @Override
    protected void handleTimerEvent() {

        List<String> events = m_events;
        events.clear();
        m_queue.drainTo(events, m_batchSize);

        if (!events.isEmpty()) {

            JsonObject batchEvent = Events.createBatchEvent(this, events);
            logger().trace("Created batch event: {} with {} events",
                    batchEvent.getString(EVENT_FIELD_ID),
                    events.size());

            //
            // Send event(s) to next in chain
            //
            emitEvent(batchEvent);
        }
    }
}
