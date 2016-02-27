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
package io.spikex.core;

import com.eaio.uuid.UUID;
import static io.spikex.core.helper.Events.EVENT_FIELD_CHAIN;
import static io.spikex.core.helper.Events.EVENT_FIELD_ID;
import io.spikex.core.helper.Variables;
import io.spikex.core.util.HostOs;
import org.vertx.java.core.Handler;
import org.vertx.java.core.eventbus.Message;
import org.vertx.java.core.json.JsonObject;

/**
 * Base class for all input/output filters and codecs.
 *
 * @author cli
 */
public abstract class AbstractFilter extends AbstractVerticle {

    private final String m_id;
    private final String m_name;

    private String m_srcAddr;
    private String m_destAddr;
    private String m_chainName;

    public static final String CONF_KEY_SOURCE_ADDRESS = "source-address";
    public static final String CONF_KEY_DEST_ADDRESS = "dest-address";
    public static final String CONF_KEY_CHAIN_NAME = "chain-name";

    public static final String MSG_FIELD_FILTER_CHAIN = "filter-chain";

    public AbstractFilter() {
        m_id = new UUID().toString();
        m_name = getClass().getSimpleName();
    }

    public final String getId() {
        return m_id;
    }

    public String getName() {
        return m_name;
    }

    public String getChainName() {
        return m_chainName;
    }

    public String getLocation() {
        return HostOs.hostName();
    }

    public final String getSourceAddress() {
        return m_srcAddr;
    }

    public final String getDestinationAddress() {
        return m_destAddr;
    }

    @Override
    public void startVerticle() {

        // Source and target address for filter chaining
        m_srcAddr = config().getString(CONF_KEY_SOURCE_ADDRESS);
        m_destAddr = config().getString(CONF_KEY_DEST_ADDRESS);
        m_chainName = config().getString(CONF_KEY_CHAIN_NAME);

        // Setup an input handler if source address has been defined
        if (m_srcAddr != null && m_srcAddr.length() > 0) {
            registerHandler();
        }

        // Start filter...
        startFilter();

        logger().info("{}:{} filter started", m_chainName, getName());
    }

    @Override
    public void stopVerticle() {

        // Stop filter...
        stopFilter();

        logger().info("{}:{} filter stopped", m_chainName, getName());
    }

    /**
     * Override this method to start the filter.
     */
    protected void startFilter() {
        // Do nothing by default...
    }

    /**
     * Override this method to perform cleanup of filter resources.
     */
    protected void stopFilter() {
        // Do nothing by default...
    }

    /**
     * Handle incoming event from previous in chain.
     *
     * @param event the event received from the previous filter
     */
    protected void handleEvent(final JsonObject event) {
        // Do nothing by default...
    }

    /**
     * Emit event to next in chain
     *
     * @param event the event to send to the next filter
     */
    protected void emitEvent(final JsonObject event) {
        emitEvent(event, getDestinationAddress());
    }

    /**
     * Emit event to given address
     *
     * @param event the event to send to the next filter
     * @param destAddr the destination address of the event
     */
    protected void emitEvent(
            final JsonObject event,
            final String destAddr) {

        if (destAddr != null && destAddr.length() > 0) {

            // Always add UUID if missing
            if (!event.containsField(EVENT_FIELD_ID)) {
                event.putString(EVENT_FIELD_ID, new UUID().toString());
            }

            // Always add "chain" to event
            String chainName = (m_chainName != null ? m_chainName : "");
            event.putString(EVENT_FIELD_CHAIN, chainName);

            // Emit event to any listeners near or far...
            eventBus().publish(destAddr, event);
        }
    }

    /**
     * Override this to customize the registration of the message handler. The
     * default implementation registers a JVM local handler.
     */
    protected void registerHandler() {

        String srcAddr = getSourceAddress();
        eventBus().registerLocalHandler(srcAddr, new Handler<Message<JsonObject>>() {

            @Override
            public void handle(final Message<JsonObject> event) {
                // Never fall outside of the event handler
                try {
                    handleEvent(event.body());
                } catch (Exception e) {
                    logger().error("Failed to handle event: {}", event.body(), e);
                }
            }

        });
    }

    @Override
    protected Variables createVariables() {
        JsonObject config = container.config();
        config.putString(CONF_KEY_CHAIN_NAME, m_chainName);
        return new Variables(config, vertx);
    }
}
