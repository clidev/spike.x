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
package io.spikex.notifier.internal;

import com.google.common.base.Preconditions;
import io.spikex.core.helper.Events;
import io.spikex.core.util.resource.YamlDocument;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import org.vertx.java.core.json.JsonArray;
import org.vertx.java.core.json.JsonObject;

/**
 *
 * @author cli
 */
public final class Rule {

    private final String m_name;
    private final String m_template;
    private final String m_subject;
    private final String m_schedule;
    private final String m_locale;
    private final List<String> m_tags;
    private final List<String> m_destinations;

    private static final String CONFIG_FIELD_MATCH_TAGS = "match-tags";
    private static final String CONFIG_FIELD_TEMPLATE = "template";
    private static final String CONFIG_FIELD_SUBJECT = "subject";
    private static final String CONFIG_FIELD_SCHEDULE = "schedule";
    private static final String CONFIG_FIELD_LOCALE = "locale";
    private static final String CONFIG_FIELD_DESTINATIONS = "destinations";

    private static final String MATCH_ANY_TAG = "*";

    private Rule(
            final String name,
            final String template,
            final String subject,
            final String schedule,
            final String locale,
            final List<String> tags,
            final List<String> destinations) {

        // Sanity checks
        Preconditions.checkArgument(name != null && name.length() > 0,
                "name is null or empty");
        Preconditions.checkArgument(template != null && template.length() > 0,
                "template is null or empty");
        Preconditions.checkArgument(subject != null && subject.length() > 0,
                "subject is null or empty");
        Preconditions.checkArgument(schedule != null && schedule.length() > 0,
                "schedule is null or empty");
        Preconditions.checkArgument(locale != null,
                "locale is null");
        Preconditions.checkArgument(tags != null && !tags.isEmpty(),
                "tags is null or empty");
        Preconditions.checkArgument(destinations != null && !destinations.isEmpty(),
                "destinations is null or empty");

        m_name = name;
        Locale loc = new Locale(locale);
        if (Locale.ROOT.equals(loc)) {
            m_template = template;
        } else {
            m_template = template + "-" + loc.toString();
        }
        m_subject = subject;
        m_schedule = schedule;
        m_locale = locale;
        m_tags = tags;
        m_destinations = destinations;
    }

    public String getName() {
        return m_name;
    }

    public String getTemplate() {
        return m_template;
    }

    public String getSubject() {
        return m_subject;
    }

    public String getSchedule() {
        return m_schedule;
    }

    public String getLocale() {
        return m_locale;
    }

    public List<String> getTags() {
        return m_tags;
    }

    public List<String> getDestinations() {
        return m_destinations;
    }

    public boolean match(final JsonObject event) {
        boolean match = false;

        JsonArray eventTags = event.getArray(Events.EVENT_FIELD_TAGS);
        if (eventTags != null) {

            // Special case (any tag goes)
            List<String> tags = m_tags; // Defined tags
            if (tags.contains(MATCH_ANY_TAG)) {
                match = true;
            } else {
                // Iterate through defined tags and find a match
                for (int i = 0; i < eventTags.size(); i++) {
                    String tag = eventTags.get(i);
                    if (tags.contains(tag)) {
                        match = true;
                        break;
                    }
                }
            }
        }

        return match;
    }

    public static Rule create(
            final String name,
            final YamlDocument config) {

        List<String> tags = new ArrayList();
        List<String> destinations = new ArrayList();

        List<String> matchTags = config.getValue(CONFIG_FIELD_MATCH_TAGS);
        for (String tag : matchTags) {
            tags.add(tag);
        }

        List<String> dests = config.getValue(CONFIG_FIELD_DESTINATIONS);
        for (String dest : dests) {
            destinations.add(dest);
        }

        return new Rule(
                name,
                config.getValue(CONFIG_FIELD_TEMPLATE, ""),
                config.getValue(CONFIG_FIELD_SUBJECT, ""),
                config.getValue(CONFIG_FIELD_SCHEDULE, "* * * *"),
                config.getValue(CONFIG_FIELD_LOCALE, Locale.ROOT.toString()),
                tags,
                destinations);
    }
}
