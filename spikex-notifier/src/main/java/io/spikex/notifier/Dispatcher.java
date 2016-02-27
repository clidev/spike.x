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
package io.spikex.notifier;

import io.spikex.core.AbstractVerticle;
import static io.spikex.core.helper.Events.EVENT_FIELD_DESTINATIONS;
import static io.spikex.core.helper.Events.EVENT_FIELD_ID;
import static io.spikex.core.helper.Events.EVENT_FIELD_MESSAGE;
import static io.spikex.core.helper.Events.EVENT_FIELD_PRIORITY;
import static io.spikex.core.helper.Events.EVENT_FIELD_TITLE;
import static io.spikex.core.helper.Events.EVENT_PRIORITY_NORMAL;
import static io.spikex.core.util.Files.Permission.OWNER_FULL_GROUP_EXEC;
import io.spikex.core.util.XXHash32;
import io.spikex.notifier.internal.EmailClient;
import io.spikex.notifier.internal.Flowdock;
import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.unbescape.html.HtmlEscape;
import org.vertx.java.core.eventbus.Message;
import org.vertx.java.core.json.JsonArray;
import org.vertx.java.core.json.JsonObject;

/**
 * Message dispatcher.
 *
 * @author cli
 */
public final class Dispatcher extends AbstractVerticle {

    // Temporary list of events
    private final List<String> m_events;

    // Configuration file
    private NotifierConfig m_config;

    // Email client, flowdock helper
    private EmailClient m_emailClient;
    private Flowdock m_flowdock;

    private static final String SCHEME_FILE = "file";
    private static final String SCHEME_MAIL = "mailto";
    private static final String SCHEME_FLOWDOCK = "flowdock";

    private static final int FILENAME_SALT = 12898923;
    private static final DateTimeFormatter DTF_FILE_DATE
            = DateTimeFormat.forPattern("yyyyMMDD");
    private static final DateTimeFormatter DTF_FILE_TM
            = DateTimeFormat.forPattern("HHmmssSSS");

    private final Logger m_logger = LoggerFactory.getLogger(Dispatcher.class);

    public Dispatcher() {
        m_events = new ArrayList();
    }

    @Override
    protected void startVerticle() {

        m_config = new NotifierConfig(confPath());

        try {
            m_config.load();

            m_emailClient = new EmailClient(
                    m_config.getEmailDef(),
                    variables(),
                    m_config.getMailHtmlEscape());

            m_flowdock = new Flowdock(
                    m_config.getFlowdockDef(),
                    variables(),
                    vertx);

        } catch (Exception e) {
            throw new IllegalStateException("Failed to load notifier configuration", e);
        }
    }

    @Override
    protected void handleLocalMessage(final Message events) {

        JsonArray jsonEvents = (JsonArray) events.body();

        for (int i = 0; i < jsonEvents.size(); i++) {

            JsonObject event = jsonEvents.get(i);

            String eventId = event.getString(EVENT_FIELD_ID, "");
            String title = event.getString(EVENT_FIELD_TITLE, "");
            String message = event.getString(EVENT_FIELD_MESSAGE, "");

            logger().trace("Processing notification: {}", eventId);

            // Event priority
            String priority = event.getString(EVENT_FIELD_PRIORITY,
                    EVENT_PRIORITY_NORMAL);

            // Destinations
            JsonArray destinations = event.getArray(EVENT_FIELD_DESTINATIONS,
                    new JsonArray());

            // Bombs away...
            send(eventId,
                    destinations,
                    title,
                    message,
                    priority);
        }
    }

    private void send(
            final String eventId,
            final JsonArray destinations,
            final String title,
            final String message,
            final String priority) {

        for (int i = 0; i < destinations.size(); i++) {

            String destination = destinations.get(i);

            try {
                String address = variables().translate(destination);
                URI destUri;
                try {
                    destUri = URI.create(address);
                } catch (IllegalArgumentException e) {
                    // Assume that destination is a file
                    destUri = new File(address).toURI();
                }

                m_logger.info("Sending {} priority notification ({}) to: {}",
                        priority.toLowerCase(), eventId, destUri);

                String scheme = destUri.getScheme();
                if (scheme != null) {
                    switch (scheme) {
                        case SCHEME_FILE:
                            saveToFile(destUri, title, message);
                            break;
                        case SCHEME_MAIL:
                            m_emailClient.send(destUri, title, message, priority);
                            break;
                        case SCHEME_FLOWDOCK:
                            m_flowdock.publish(destUri, title, message, priority);
                            break;
                        default:
                            m_logger.error("Unsupported scheme: {}", scheme);
                            break;
                    }
                } else {
                    // Assume file
                    destUri = new File(address).toURI();
                    saveToFile(destUri, title, message);
                }
            } catch (Exception e) {
                m_logger.error("Failed to send event: {} to destination: {}",
                        eventId, destination, e);
            }
        }
    }

    private void saveToFile(
            final URI location,
            final String subject,
            final String body) {

        try {
            // Create dir if it doesn't exist
            Path path = Paths.get(location);
            if (!path.toFile().exists()) {
                io.spikex.core.util.Files.createDirectories(
                        config().getString(CONF_KEY_USER),
                        OWNER_FULL_GROUP_EXEC,
                        path);
            }
            //
            // Unescape body
            //
            String text = body;
            if (m_config.getFileHtmlUnescape()) {
                text = HtmlEscape.unescapeHtml(body);
            }
            //
            DateTime now = DateTime.now();
            StringBuilder filename = new StringBuilder();
            String datePart = DTF_FILE_DATE.print(now);
            filename.append(datePart); // YYYYMMDD
            filename.append("-");
            filename.append(DTF_FILE_TM.print(now)); // HHmmssSSS
            filename.append("-");
            filename.append(XXHash32.hashAsHex(text, FILENAME_SALT));
            filename.append(".ntf");

            // Create subdir (if it doesn't exist)
            Path datePath = path.resolve(datePart);
            if (!datePath.toFile().exists()) {
                io.spikex.core.util.Files.createDirectories(
                        config().getString(CONF_KEY_USER),
                        OWNER_FULL_GROUP_EXEC,
                        datePath);
            }

            Files.write(datePath.resolve(filename.toString()), text.getBytes());
            m_logger.debug("Saved notification to: {}", path);
        } catch (IOException e) {
            m_logger.error("Failed to save \"{}\"notification", subject, e);
        }
    }
}
