/**
 *
 * Copyright (c) 2016 NG Modular Oy.
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
package io.spikex.notifier.internal;

import com.hazelcast.core.EntryEvent;
import com.hazelcast.core.EntryListener;
import com.hazelcast.core.ItemEvent;
import com.hazelcast.core.ItemListener;
import com.hazelcast.core.MapEvent;
import com.hazelcast.core.MemberAttributeEvent;
import com.hazelcast.core.MembershipEvent;
import com.hazelcast.core.MembershipListener;
import static io.spikex.core.helper.Events.EVENT_FIELD_ID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.vertx.java.core.json.JsonObject;

/**
 *
 * @author cli
 */
public final class HzEventListener implements MembershipListener, EntryListener<String, JsonObject>, ItemListener<JsonObject> {

    private final Logger m_logger = LoggerFactory.getLogger(HzEventListener.class);

    @Override
    public void entryAdded(final EntryEvent<String, JsonObject> event) {
        m_logger.debug("Member: {} {} - entry added: {}",
                event.getMember().getSocketAddress(),
                event.getMember().getUuid(),
                event.getKey());
    }

    @Override
    public void entryUpdated(final EntryEvent<String, JsonObject> event) {
        m_logger.debug("Member: {} {} - entry updated: {}",
                event.getMember().getSocketAddress(),
                event.getMember().getUuid(),
                event.getKey());
    }

    @Override
    public void entryRemoved(final EntryEvent<String, JsonObject> event) {
        m_logger.debug("Member: {} {} - entries removed: {}",
                event.getMember().getSocketAddress(),
                event.getMember().getUuid(),
                event.getKey());
    }

    @Override
    public void entryEvicted(final EntryEvent<String, JsonObject> event) {
        m_logger.debug("Member: {} {} - entries evicted: {}",
                event.getMember().getSocketAddress(),
                event.getMember().getUuid(),
                event.getKey());
    }

    @Override
    public void mapCleared(final MapEvent event) {
        m_logger.debug("Member: {} {} - cleared entries: {}",
                event.getMember().getSocketAddress(),
                event.getMember().getUuid(),
                event.getNumberOfEntriesAffected());
    }

    @Override
    public void mapEvicted(final MapEvent event) {
        m_logger.debug("Member: {} {} - evicted entries: {}",
                event.getMember().getSocketAddress(),
                event.getMember().getUuid(),
                event.getNumberOfEntriesAffected());
    }

    @Override
    public void itemAdded(final ItemEvent<JsonObject> event) {
        m_logger.debug("Member: {} {} - item added: {}",
                event.getMember().getSocketAddress(),
                event.getMember().getUuid(),
                event.getItem().getValue(EVENT_FIELD_ID));
    }

    @Override
    public void itemRemoved(final ItemEvent<JsonObject> event) {
        m_logger.debug("Member: {} {} - item removed: {}",
                event.getMember().getSocketAddress(),
                event.getMember().getUuid(),
                event.getItem().getValue(EVENT_FIELD_ID));
    }

    @Override
    public void memberAdded(final MembershipEvent event) {
        m_logger.debug("Member added: {} {}",
                event.getMember().getSocketAddress(),
                event.getMember().getUuid());
    }

    @Override
    public void memberRemoved(final MembershipEvent event) {
        m_logger.debug("Member removed: {} {}",
                event.getMember().getSocketAddress(),
                event.getMember().getUuid());
    }

    @Override
    public void memberAttributeChanged(final MemberAttributeEvent event) {
        m_logger.debug("Member attribute changed: {} {}",
                event.getMember().getSocketAddress(),
                event.getMember().getUuid());
    }
}
