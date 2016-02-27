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

import com.google.common.base.Preconditions;
import com.google.common.io.Files;
import com.google.common.io.Resources;
import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import oi.thekraken.grok.api.Match;
import oi.thekraken.grok.api.exception.GrokException;
import io.spikex.core.AbstractFilter;
import static io.spikex.core.helper.Events.EVENT_FIELD_TAGS;
import java.util.ArrayList;
import java.util.HashMap;
import org.vertx.java.core.json.JsonArray;
import org.vertx.java.core.json.JsonObject;

/**
 * Example:
 * <pre>
 *  "chain": [
 *          { "Grok": {
 *                   "patterns": [
 *                       "file:%{#spikex.conf}/grok/base.grok",
 *                       "file:%{#spikex.conf}/grok/log.grok"
 *                   ],
 *                   "input-field": "@message",
 *                   "output-field": "@message",
 *                   "group": {
 *                       "fields": ["class", "method", "message", "thread", "level",
 *                                  "year", "month", "day", "hour", "minute", "second"],
 *                       "output-field": "@fields"
 *                   },
 *                   "match-lines": [
 *                      {
 *                       "pattern": "%{JAVAJBOSS4LOG:line}",
 *                       "tags": ["log", "java"],
 *                       "ignore": ["JAVALVLCLS"]
 *                      }
 *                   ],
 *                   "multi-line": {
 *                       "pattern": "%{JAVAEXCEPTION:line}",
 *                       "tags": ["error", "exception"],
 *                       "segment-field": "class"
 *                   }
 *               }
 *          }
 *  ]
 * </pre>
 *
 * @author cli
 */
public class Grok extends AbstractFilter {

    private final oi.thekraken.grok.api.Grok m_grokMulti;
    private final String[] m_multiTags;
    private final List<String> m_groupFields;
    private final List<MatchLine> m_matchLines;
    private final MultiLine m_multiLine;
    private String m_inputField;
    private String m_outputField;
    private String m_groupField;

    //
    // Configuration defaults
    //
    private static final String[] DEF_PATTERNS = {
        "classpath:/grok/patterns",
        "classpath:/grok/firewalls",
        "classpath:/grok/linux-syslog",
        "classpath:/grok/haproxy",
        "classpath:/grok/nagios",
        "classpath:/grok/java",
        "classpath:/grok/ruby"
    };

    private static final String SCHEME_CLASSPATH = "classpath:";
    private static final int MAX_TAG_COUNT = 32;
    private static final int MAX_LINE_COUNT = 1000;

    private static final String CONF_KEY_TAGS = "tags";
    private static final String CONF_KEY_GROUP = "group";
    private static final String CONF_KEY_FIELDS = "fields";
    private static final String CONF_KEY_IGNORE = "ignore";
    private static final String CONF_KEY_PATTERN = "pattern";
    private static final String CONF_KEY_PATTERNS = "patterns";
    private static final String CONF_KEY_MATCH_LINES = "match-lines";
    private static final String CONF_KEY_MULTI_LINE = "multi-line";
    private static final String CONF_KEY_INPUT_FIELD = "input-field";
    private static final String CONF_KEY_OUTPUT_FIELD = "output-field";
    private static final String CONF_KEY_SEGMENT_FIELD = "segment-field";

    private static final String DEF_INPUT_FIELD = "@message";
    private static final String DEF_OUTPUT_FIELD = "@message";
    private static final String DEF_GROUP_FIELD = "@fields";

    public Grok() {
        m_grokMulti = new oi.thekraken.grok.api.Grok();
        m_multiTags = new String[MAX_TAG_COUNT];
        m_matchLines = new ArrayList();
        m_multiLine = new MultiLine(MAX_LINE_COUNT, DEF_GROUP_FIELD);
        m_groupFields = new ArrayList();
        m_groupField = "";
    }

    @Override
    protected void startFilter() {

        // Create grok directory if it doesn't exist
        File grokDir = new File(dataPath().toFile(), "grok");
        boolean exists = grokDir.exists();
        if (!exists) {
            if (!grokDir.mkdirs()) {
                throw new IllegalStateException(
                        "Failed to create grok directory in: " + dataPath());
            }
        }

        // input and output fields
        m_inputField = config().getString(CONF_KEY_INPUT_FIELD, DEF_INPUT_FIELD);
        m_outputField = config().getString(CONF_KEY_OUTPUT_FIELD, DEF_OUTPUT_FIELD);
        m_multiLine.setOutputField(m_outputField);

        // Group fields
        JsonObject group = config().getObject(CONF_KEY_GROUP, new JsonObject());
        m_groupField = group.getString(CONF_KEY_GROUP, DEF_GROUP_FIELD);
        JsonArray groupFields = group.getArray(CONF_KEY_FIELDS, new JsonArray());
        for (int i = 0; i < groupFields.size(); i++) {
            m_groupFields.add((String) groupFields.get(i));
        }
        m_multiLine.setGroupField(m_groupField);

        // Pre build match lines
        // match-lines
        if (config().containsField(CONF_KEY_MATCH_LINES)) {
            JsonArray matchLines = config().getArray(CONF_KEY_MATCH_LINES, new JsonArray());
            for (int i = 0; i < matchLines.size(); i++) {
                MatchLine matcher = new MatchLine();
                m_matchLines.add(matcher);
            }
        }

        // grok-urls
        JsonArray defGrokUrls = new JsonArray(DEF_PATTERNS);
        JsonArray grokUrls = config().getArray(CONF_KEY_PATTERNS, defGrokUrls);

        for (int i = 0; i < grokUrls.size(); i++) {

            String url = grokUrls.get(i);
            URI uri = URI.create(resolveUrl(url));

            try {
                Path uriPath = Paths.get(uri); // Grok file URI
                String filename = uriPath.getFileName().toString();
                File grokFile = new File(grokDir, filename);
                if (!java.nio.file.Files.exists(grokFile.toPath())) {
                    // Copy grok file to local directory
                    logger().info("Copying \"{}\" to \"{}\"", filename, grokFile.getAbsolutePath());
                    Resources.asByteSource(uri.toURL()).copyTo(Files.asByteSink(grokFile));
                }
                for (MatchLine matcher : m_matchLines) {
                    matcher.addPatternFromFile(grokFile.getAbsolutePath());
                }
                m_grokMulti.addPatternFromFile(grokFile.getAbsolutePath());
            } catch (GrokException | IOException e) {
                throw new IllegalStateException("Failed to add grok pattern: "
                        + uri.toString(), e);
            }
        }

        String pattern = null;
        try {
            // match-lines
            if (config().containsField(CONF_KEY_MATCH_LINES)) {

                JsonArray matchLines = config().getArray(CONF_KEY_MATCH_LINES, new JsonArray());
                for (int i = 0; i < matchLines.size(); i++) {

                    JsonObject matchLine = (JsonObject) matchLines.get(i);
                    MatchLine matcher = m_matchLines.get(i);

                    pattern = matchLine.getString(CONF_KEY_PATTERN);
                    matcher.compile(pattern);

                    // tags
                    JsonArray tags = matchLine.getArray(CONF_KEY_TAGS);
                    Preconditions.checkArgument(tags.size() <= MAX_TAG_COUNT,
                            "You can define only " + MAX_TAG_COUNT + " tags for "
                            + CONF_KEY_MATCH_LINES);

                    for (int j = 0; j < tags.size() && j < MAX_TAG_COUNT; j++) {
                        matcher.addTag((String) tags.get(j), j);
                    }

                    // ignore
                    JsonArray ignore = matchLine.getArray(CONF_KEY_IGNORE, new JsonArray());
                    for (int j = 0; j < ignore.size(); j++) {
                        matcher.addIgnore((String) ignore.get(j));
                    }
                }
            }
        } catch (GrokException e) {
            throw new IllegalStateException("Failed to compile pattern: "
                    + pattern, e);
        }

        pattern = null;
        try {
            // multi-line
            if (config().containsField(CONF_KEY_MULTI_LINE)) {
                JsonObject multiLine = config().getObject(CONF_KEY_MULTI_LINE);
                pattern = multiLine.getString(CONF_KEY_PATTERN);
                m_grokMulti.compile(pattern);
                // segment-field
                String field = multiLine.getString(CONF_KEY_SEGMENT_FIELD, "");
                m_multiLine.setSegmentField(field);
                // tags
                JsonArray tags = multiLine.getArray(CONF_KEY_TAGS);
                Preconditions.checkArgument(tags.size() <= MAX_TAG_COUNT,
                        "You can define only " + MAX_TAG_COUNT + " tags for "
                        + CONF_KEY_MULTI_LINE);
                for (int i = 0; i < tags.size() && i < MAX_TAG_COUNT; i++) {
                    m_multiTags[i] = tags.get(i);
                }
            }
        } catch (GrokException e) {
            throw new IllegalStateException("Failed to compile pattern: "
                    + pattern, e);
        }
    }

    @Override
    protected void handleEvent(final JsonObject event) {

        final String line = event.getString(m_inputField, "");

        // Does line match grok single-line expression?
        if (matchLine(event, line)) {

            emitMultiLine(); // Emit multi-line (if any)

            //
            // Forward event
            //
            emitEvent(event);

        } else {

            // Does line match grok multi-line expression?
            if (!matchMulti(event, line)) {
                emitMultiLine(); // Emit multi-line (if any)
            }
        }
    }

    private boolean matchLine(
            final JsonObject event,
            final String line) {

        boolean match = false;

        for (MatchLine matcher : m_matchLines) {
            Match m = matcher.match(line);
            m.captures();

            List<String> ignore = matcher.getIgnore();
            Map<String, Object> captures = m.toMap();
            match = !captures.isEmpty();
            if (match) {

                JsonObject group = null;
                if (m_groupField.length() > 0) {
                    group = new JsonObject();
                }

                List<String> groupFields = m_groupFields;
                String outputField = m_outputField;
                Iterator<String> keys = captures.keySet().iterator();
                while (keys.hasNext()) {
                    String key = keys.next();
                    if (!ignore.contains(key)) {
                        Object value = captures.get(key);
                        if (value != null) {
                            if ("line".equals(key)) {
                                event.putValue(outputField, value);
                            } else {
                                if (group != null && groupFields.contains(key)) {
                                    group.putValue(key, value);
                                } else {
                                    event.putValue(key, value);
                                }
                            }
                        }
                    }
                }
                // Contain specified fields within a specific group
                if (group != null) {
                    event.putObject(m_groupField, group);
                }
                // Update tags
                JsonArray tags = event.getArray(EVENT_FIELD_TAGS);

                for (String tag : matcher.getLineTags()) {
                    if (tag != null) {
                        tags.addString(tag);
                    } else {
                        break;
                    }
                }
                event.putArray(EVENT_FIELD_TAGS, tags);
                break; // Match found - first match wins
            }
        }

        return match;
    }

    private boolean matchMulti(
            final JsonObject event,
            final String line) {

        boolean match = false;
        Match m = m_grokMulti.match(line);
        m.captures();

        Map<String, Object> captures = m.toMap();
        if (!captures.isEmpty()) {
            MultiLine multiLine = m_multiLine;
            multiLine.setEvent(event);
            match = multiLine.addFields(captures);
        }

        return match;
    }

    private void emitMultiLine() {
        // Emit multi-line (if any)
        MultiLine multiLine = m_multiLine;
        if (multiLine.hasLines()) {

            JsonObject multiEvent = multiLine.getEvent();
            JsonArray tags = multiEvent.getArray(EVENT_FIELD_TAGS);
            for (String tag : m_multiTags) {
                if (tag != null) {
                    tags.addString(tag);
                } else {
                    break;
                }
            }
            emitEvent(multiEvent);
            multiLine.clear();
        }
    }

    private String resolveUrl(final String url) {

        String grokUrl = url;

        if (url.startsWith(SCHEME_CLASSPATH)) {
            String path = url.substring(SCHEME_CLASSPATH.length());
            grokUrl = getClass().getResource(path).toExternalForm();
        }
        return variables().translate(grokUrl);
    }

    private static final class MatchLine {

        private final oi.thekraken.grok.api.Grok m_grokLine;
        private final String[] m_lineTags;
        private final List<String> m_ignore;

        private MatchLine() {
            m_grokLine = new oi.thekraken.grok.api.Grok();
            m_lineTags = new String[MAX_TAG_COUNT];
            m_ignore = new ArrayList();
        }

        private String[] getLineTags() {
            return m_lineTags;
        }

        private List<String> getIgnore() {
            return m_ignore;
        }

        private void addPatternFromFile(final String path) throws GrokException {
            m_grokLine.addPatternFromFile(path);
        }

        private void compile(final String pattern) throws GrokException {
            m_grokLine.compile(pattern);
        }

        private void addTag(
                final String tag,
                final int index) {

            m_lineTags[index] = tag;
        }

        private void addIgnore(final String ignore) {
            m_ignore.add(ignore);
        }

        private Match match(final String line) {
            return m_grokLine.match(line);
        }
    }

    private static final class MultiLine {

        private final int m_maxLineCount;
        private JsonObject m_event;
        private List<MultiLineSegment> m_segments;
        private String m_segmentField;
        private String m_outputField;
        private String m_groupField;
        private int m_count;
        private int m_segIndex;

        private MultiLine(
                final int maxLineCount,
                final String groupField) {

            m_maxLineCount = maxLineCount;
            m_groupField = groupField;
            clear();
        }

        private boolean hasLines() {
            return (m_count > 0);
        }

        private JsonObject getEvent() {

            JsonObject event = m_event; // The original/first event
            JsonArray segArray = new JsonArray();
            StringBuilder lines = new StringBuilder();

            for (MultiLineSegment segment : m_segments) {
                lines.append(segment.getLines());
                segArray.addObject(segment.getFields());
            }

            event.putString(m_outputField, lines.toString());
            event.putArray(m_groupField, segArray);

            return event;
        }

        private MultiLineSegment getCurrentSegment() {
            return m_segments.get(m_segIndex);
        }

        private boolean addFields(final Map<String, Object> fields) {

            boolean added = false;

            if (m_count < m_maxLineCount && fields.containsKey("line")) {

                // Create new segment?
                Object segFieldValue = fields.get(m_segmentField);
                if (m_count > 0 && segFieldValue != null) {
                    MultiLineSegment segment = new MultiLineSegment();
                    m_segments.add(segment);
                    m_segIndex++;
                }

                // Add fields to current segment
                MultiLineSegment segment = getCurrentSegment();
                segment.addFields(fields);
                m_count++;
                added = true;
            }

            return added;
        }

        private void setSegmentField(final String segmentField) {
            m_segmentField = segmentField;
        }

        private void setOutputField(final String outputField) {
            m_outputField = outputField;
        }

        private void setGroupField(final String groupField) {
            m_groupField = groupField;
        }

        private void setEvent(final JsonObject event) {
            if (m_count == 0) {
                m_event.mergeIn(event);
            }
        }

        private void clear() {
            m_count = 0;
            m_event = new JsonObject();
            m_segments = new ArrayList();
            m_segments.add(new MultiLineSegment());
            m_segIndex = 0;
        }
    }

    private static final class MultiLineSegment {

        private final StringBuilder m_lines;
        private final Map<String, Object> m_fields;

        private MultiLineSegment() {
            m_lines = new StringBuilder();
            m_fields = new HashMap();
        }

        private String getLines() {
            return m_lines.toString();
        }

        private JsonObject getFields() {
            return new JsonObject(m_fields);
        }

        private void addFields(final Map<String, Object> fields) {

            // Append line to this segment
            String line = (String) fields.get("line");
            m_lines.append(line);
            m_lines.append("\n");

            // Store fields for later use (append to list if same field exists)
            Map<String, Object> segFields = m_fields;

            Iterator<String> keys = fields.keySet().iterator();
            while (keys.hasNext()) {
                String key = keys.next();
                Object value = fields.get(key);
                if (!"line".equals(key) && value != null) {

                    // Create list if field already exists
                    if (segFields.containsKey(key)) {
                        Object entries = segFields.get(key);
                        if (!(entries instanceof List)) {
                            List tmp = new ArrayList();
                            tmp.add(entries);
                            entries = tmp;
                        }
                        ((List) entries).add(value);
                        segFields.put(key, entries);

                    } else {
                        segFields.put(key, value);
                    }
                }
            }
        }
    }
}
