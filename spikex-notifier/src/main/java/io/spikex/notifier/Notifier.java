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

import com.github.mustachejava.DefaultMustacheFactory;
import com.github.mustachejava.Mustache;
import com.github.mustachejava.MustacheFactory;
import static io.spikex.core.helper.Events.EVENT_FIELD_DESTINATIONS;
import static io.spikex.core.helper.Events.EVENT_FIELD_ID;
import static io.spikex.core.helper.Events.EVENT_FIELD_MESSAGE;
import static io.spikex.core.helper.Events.EVENT_FIELD_PRIORITY;
import static io.spikex.core.helper.Events.EVENT_FIELD_TITLE;
import io.spikex.core.helper.Variables;
import io.spikex.core.util.CronEntry;
import static io.spikex.core.util.Files.Permission.OWNER_FULL_GROUP_EXEC;
import io.spikex.core.util.Version;
import io.spikex.core.util.resource.TextResource;
import io.spikex.notifier.NotifierConfig.DestinationDef;
import io.spikex.notifier.NotifierConfig.TemplateDef;
import io.spikex.notifier.internal.Rule;
import java.io.IOException;
import java.io.StringReader;
import java.io.StringWriter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.function.Function;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.mapdb.DB;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.unbescape.html.HtmlEscape;
import org.unbescape.json.JsonEscape;
import org.unbescape.uri.UriEscape;
import org.unbescape.xml.XmlEscape;
import org.vertx.java.core.Handler;
import org.vertx.java.core.eventbus.EventBus;
import org.vertx.java.core.eventbus.Message;
import org.vertx.java.core.json.JsonArray;
import org.vertx.java.core.json.JsonObject;

/**
 * TODO implement notification retention (dir(s) with json files under data dir)
 * TODO implement notification storage (store notif in mapdb before sending -
 * HA)
 *
 * @author cli
 */
public final class Notifier implements Handler<Message<JsonObject>> {

    // Configuration file and variables
    private final NotifierConfig m_config;
    private final Variables m_variables;

    // Configuration path and process user
    private final Path m_confPath;
    private final String m_user;

    // Notification database
    private final DB m_db;

    // Mustache templates
    private final Map<String, Mustache> m_templates;

    // Template functions
    private final Map<String, Object> m_functions;

    private static final String EVENT_FIELD_NOTIF_SUCCESS = "@success";
    private static final String EVENT_FIELD_NOTIF_WARNING = "@warning";
    private static final String EVENT_FIELD_NOTIF_DANGER = "@danger";

    private static final String TEMPLATE_DIR = "template";

    private final Logger m_logger = LoggerFactory.getLogger(Notifier.class);

    public Notifier(
            final DB db,
            final NotifierConfig config,
            final Variables variables,
            final Path confPath,
            final String user) {

        m_db = db;
        m_config = config;
        m_variables = variables;
        m_confPath = confPath;
        m_user = user;
        m_templates = new HashMap();

        // Functions
        m_functions = new HashMap();
        m_functions.put("escape-json", new Function<String, String>() {

            @Override
            public String apply(String text) {
                return JsonEscape.escapeJson(text);
            }
        });
        m_functions.put("escape-html5", new Function<String, String>() {

            @Override
            public String apply(String text) {
                return HtmlEscape.escapeHtml5(text);
            }
        });
        m_functions.put("escape-html4", new Function<String, String>() {

            @Override
            public String apply(String text) {
                return HtmlEscape.escapeHtml4Xml(text);
            }
        });
        m_functions.put("escape-xml11", new Function<String, String>() {

            @Override
            public String apply(String text) {
                return XmlEscape.escapeXml11(text);
            }
        });
        m_functions.put("escape-xml10", new Function<String, String>() {

            @Override
            public String apply(String text) {
                return XmlEscape.escapeXml10Minimal(text);
            }
        });
        m_functions.put("escape-uri-path", new Function<String, String>() {

            @Override
            public String apply(String text) {
                return UriEscape.escapeUriPath(text);
            }
        });
    }

    public void start(final EventBus eventBus) {
        //
        // Build Mustasche templates
        //
        buildTemplates();
        //
        // Start listening for events
        //
        m_logger.info("Listening on local address: {}", m_config.getLocalAddress());
        eventBus.registerLocalHandler(m_config.getLocalAddress(), this);
    }

    public void stop(final EventBus eventBus) {
        eventBus.unregisterHandler(m_config.getLocalAddress(), this);
    }

    public void buildTemplates() {
        //
        // Build mustasche templates
        //
        m_templates.clear();
        Map<String, TemplateDef> templates = m_config.getTemplates();
        MustacheFactory mstFactory = new DefaultMustacheFactory();

        Set<String> keys = templates.keySet();
        for (String key : keys) {

            TemplateDef def = templates.get(key);
            List<String> locales = new ArrayList();
            locales.add(Locale.ROOT.toString()); // No locale
            locales.addAll(def.getLocales());

            for (String locale : locales) {
                Locale loc = new Locale(locale);
                String url = m_variables.translate(def.getUrl());
                String tmpl = key + "-" + loc.toString();
                if (Locale.ROOT.equals(loc)) {
                    tmpl = key;
                }
                m_logger.debug("Building Mustasche template: {} ({})", tmpl, url);
                m_templates.put(tmpl,
                        buildTemplate(
                                m_confPath,
                                m_user,
                                mstFactory,
                                url,
                                loc));
            }
        }
    }

    @Override
    public void handle(final Message<JsonObject> message) {
        //
        // Find matching rule
        //
        m_logger.trace("Received: {}", message.body());
        DateTime now = DateTime.now();
        String timezone = now.getZone().getID();
        Map<String, CronEntry> schedules = m_config.getSchedules();
        List<Rule> rules = m_config.getRules();
        for (Rule rule : rules) {
            //
            // Matching schedule and tags?
            //
            CronEntry entry = schedules.get(rule.getSchedule());
            if (!timezone.equals(entry.getTimezone())) {
                timezone = entry.getTimezone();
                now = DateTime.now(DateTimeZone.forID(timezone));
            }
            // We must make a copy (we want to replace @message with the translated template contents)
            JsonObject event = new JsonObject().mergeIn(message.body());
            if (entry.isDefined(now)
                    && rule.match(event)) {

                List<String> destinations = rule.getDestinations();
                m_logger.debug("Rule \"{}\" matched - template: {} notifying: {}",
                        rule.getName(), rule.getTemplate(), destinations);

                //
                // Add optional fields (if missing) - so that templates work correctly
                //
                if (!event.containsField(EVENT_FIELD_NOTIF_SUCCESS)) {
                    event.putBoolean(EVENT_FIELD_NOTIF_SUCCESS, false);
                }
                if (!event.containsField(EVENT_FIELD_NOTIF_WARNING)) {
                    event.putBoolean(EVENT_FIELD_NOTIF_WARNING, false);
                }
                if (!event.containsField(EVENT_FIELD_NOTIF_DANGER)) {
                    event.putBoolean(EVENT_FIELD_NOTIF_DANGER, false);
                }

                //
                // Translate subject and fill/execute template
                //
                String subject = m_variables.translate(event, rule.getSubject());
                Mustache template = m_templates.get(rule.getTemplate());
                StringWriter writer = new StringWriter();
                Map scopes = new HashMap(event.toMap());
                scopes.putAll(m_functions); // Functions
                template.execute(writer, scopes);
                List<String> resolvedDestinations = resolveDestinations(destinations);

                //
                // Fill event with notification info
                //
                event.putString(EVENT_FIELD_TITLE, subject);
                event.putString(EVENT_FIELD_MESSAGE, writer.toString());
                event.putArray(EVENT_FIELD_DESTINATIONS, new JsonArray(resolvedDestinations));

                if (!m_db.isClosed()) {

                    m_logger.info("Storing notification: {} priority: {} destinations: {} subject: {}",
                            event.getString(EVENT_FIELD_ID, ""),
                            event.getString(EVENT_FIELD_PRIORITY, ""),
                            resolvedDestinations,
                            subject);

                    BlockingQueue<String> queue = m_db.getQueue(NotifierConfig.queueName());
                    queue.add(event.toString());
                    m_db.commit();

                } else {
                    // Log event
                    m_logger.error("Notification database is closed. "
                            + "Could not store event: {}", event.toString());
                }
            }
        }
    }

    private Mustache buildTemplate(
            final Path confPath,
            final String user,
            final MustacheFactory factory,
            final String url,
            final Locale locale) {

        Mustache mst = null;
        try {
            //
            // Check if template exists
            //
            Path urlPath = Paths.get(url);
            Path basePath = confPath.resolve(TEMPLATE_DIR);
            String name = resolveResourceName(urlPath);
            String suffix = resolveResourceSuffix(urlPath);
            TextResource template = TextResource.builder(basePath.toUri())
                    .name(name)
                    .suffix(suffix)
                    .location(urlPath.getFileName().toUri())
                    .locale(locale)
                    .version(Version.create(name, Version.nullVersion()))
                    .build();

            // Create template dir if it doesn't exist
            if (!Files.exists(basePath)) {
                io.spikex.core.util.Files.createDirectories(
                        user,
                        OWNER_FULL_GROUP_EXEC,
                        basePath);
            }

            if (!template.exists()) {
                //
                // Load and save template
                //
                m_logger.info("Saving \"{}\" template in: {}",
                        template.getQualifiedName(),
                        basePath);
                TextResource origTemplate = TextResource.builder(
                        urlPath.getParent().toUri(),
                        template)
                        .build()
                        .load();

                template = TextResource.builder(
                        basePath.toUri(),
                        origTemplate)
                        .build()
                        .save();
            } else {
                m_logger.info("Loading template: {}",
                        template.getQualifiedName());
                template = template.load();
            }

            //
            // Compile template
            //
            mst = factory.compile(new StringReader(template.getData()), name);

        } catch (IOException e) {
            throw new IllegalStateException("Failed to build template: "
                    + url, e);
        }
        return mst;
    }

    private List<String> resolveDestinations(final List<String> destinations) {
        List<String> resolved = new ArrayList();
        Map<String, DestinationDef> defs = m_config.getDestinations();
        for (String destination : destinations) {
            DestinationDef def = defs.get(destination);
            List<String> dests = def.geAddresses();
            resolved.addAll(dests);
        }
        return resolved;
    }

    private String resolveResourceName(final Path filename) {
        String name = filename.getFileName().toString();
        int pos = name.lastIndexOf('.');
        if (pos > 0) {
            name = name.substring(0, pos);
        }
        return name;
    }

    private String resolveResourceSuffix(final Path filename) {
        String suffix = "";
        String name = filename.getFileName().toString();
        int pos = name.lastIndexOf('.');
        if (pos > 0) {
            suffix = name.substring(pos);
        }
        return suffix;
    }
}
