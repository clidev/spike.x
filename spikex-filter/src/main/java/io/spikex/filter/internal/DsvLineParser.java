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
package io.spikex.filter.internal;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import java.io.IOException;
import java.io.StringReader;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import io.spikex.core.AbstractFilter;
import io.spikex.core.helper.Events;
import static io.spikex.core.helper.Events.EVENT_PRIORITY_NORMAL;
import io.spikex.core.util.HostOs;
import java.util.ArrayList;
import java.util.HashMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.supercsv.cellprocessor.Optional;
import org.supercsv.cellprocessor.ParseChar;
import org.supercsv.cellprocessor.ParseDouble;
import org.supercsv.cellprocessor.ParseInt;
import org.supercsv.cellprocessor.ParseLong;
import org.supercsv.cellprocessor.StrReplace;
import org.supercsv.cellprocessor.Trim;
import org.supercsv.cellprocessor.constraint.DMinMax;
import org.supercsv.cellprocessor.constraint.NotNull;
import org.supercsv.cellprocessor.constraint.StrNotNullOrEmpty;
import org.supercsv.cellprocessor.constraint.UniqueHashCode;
import org.supercsv.cellprocessor.ift.CellProcessor;
import org.supercsv.cellprocessor.ift.DoubleCellProcessor;
import org.supercsv.cellprocessor.ift.LongCellProcessor;
import org.supercsv.cellprocessor.ift.StringCellProcessor;
import org.supercsv.comment.CommentMatcher;
import org.supercsv.io.CsvListReader;
import org.supercsv.io.ICsvListReader;
import org.supercsv.prefs.CsvPreference;
import org.vertx.java.core.json.JsonArray;
import org.vertx.java.core.json.JsonElement;
import org.vertx.java.core.json.JsonObject;

/**
 * Supported parsers, formatters and constraints:
 * <ul>
 * <li>DMinMax(min, max)</li>
 * <li>Equals(value)</li>
 * <li>ForbidSubStr(string, string, string, ...)</li>
 * <li>LMinMax(min, max)</li>
 * <li>NotNull</li>
 * <li>RequireSubStr(string, string, string, ...)</li>
 * <li>StrMinMax(min, max)</li>
 * <li>StrNotNullOrEmpty</li>
 * <li>StrRegEx(regExp)</li>
 * <li>UniqueHashCode</li>
 * <li>ConvertNullTo(value)</li>
 * <li>HashMapper(lookupMap)</li>
 * <li>Optional</li>
 * <li>StrReplace(regExp, replacement)</li>
 * <li>Trim</li>
 * <li>Truncate(maxSize)</li>
 * <li>BigDecimal</li>
 * <li>BigDecimal(locale)</li>
 * <li>Bool</li>
 * <li>Bool(true-list, false-list)</li>
 * <li>Char</li>
 * <li>Date(format)</li>
 * <li>Double</li>
 * <li>Int</li>
 * <li>Long</li>
 * </ul>
 * <p>
 * Configuration:
 * <pre>
 *  "type": "dsv",
 *  "delimiter": "&lt;SPACE&gt;",
 *  "quote-char": '"',
 *  "line-terminator": "&lt;LF&gt;",
 *  "trunc-dup-delimiters": true,
 *  "mapping": {
 *      "lookup-values": {
 *                  "owners": {
 *                      "root": "ADMIN",
 *                      "john": "USER",
 *                      "jody": "USER"
 *                  },
 *                  "true-list": [ "y", "yes", "-" ],
 *                  "false-list": [ "n", "no" ]
 *      },
 *      "fields": [
 *                  [ "permission", "NotNull" ],
 *                  [ "inodes", "NotNull", "Long" ],
 *                  [ "owner", "NotNull", "HashMapper(owners)" ],
 *                  [ "group", "NotNull" ],
 *                  [ "size", "NotNull", "Long" ],
 *                  [ "modified", "NotNull" ],
 *                  [ "modified", "NotNull" ],
 *                  [ "modified", "NotNull" ],
 *                  [ "file", "NotNull" ]
 *      ]
 *  }
 * </pre>
 * <p>
 * @author cli
 */
public final class DsvLineParser implements ILineParser {

    private final AbstractFilter m_filter;
    private final int m_delimiter;
    private final boolean m_truncDupDelimiters;
    private final CsvPreference m_csvPreference;
    private final CellProcessor[] m_cellProcessors;
    private final String[] m_fieldNames;
    private final Map<String, Map> m_lookupMaps;
    private final Map<String, List> m_lookupLists;

    private static final String CONFIG_FIELD_COMMENT_STRING = "comment-string";
    private static final String CONFIG_FIELD_DELIMITER = "delimiter";
    private static final String CONFIG_FIELD_QUOTE_CHAR = "quote-char";
    private static final String CONFIG_FIELD_LINE_TERMINATOR = "line-terminator";
    private static final String CONFIG_FIELD_FIELDS = "fields";
    private static final String CONFIG_FIELD_MAPPING = "mapping";
    private static final String CONFIG_FIELD_LOOKUP_VALUES = "lookup-values";
    private static final String CONFIG_FIELD_TRUNC_DUP_DELIMITERS = "trunc-dup-delimiters";

    private static final String DFN_CHAR_SPACE = "<SPACE>";
    private static final String DFN_CHAR_BELL = "<BELL>";
    private static final String DFN_CHAR_VTAB = "<VTAB>";
    private static final String DFN_CHAR_NULL = "<NULL>";
    private static final String DFN_CHAR_ESC = "<ESC>";
    private static final String DFN_CHAR_TAB = "<TAB>";
    private static final String DFN_CHAR_FF = "<FF>";
    private static final String DFN_CHAR_LF = "<LF>";
    private static final String DFN_CHAR_CR = "<CR>";

    private static final String DFN_FUNC_DMINMAX = "DMinMax";
    private static final String DFN_FUNC_FORBIDSUBSTR = "ForbidSubStr";
    private static final String DFN_FUNC_LMINMAX = "LMinMax";
    private static final String DFN_FUNC_NOTNULL = "NotNull";
    private static final String DFN_FUNC_REQUIRESUBSTR = "RequireSubStr";
    private static final String DFN_FUNC_STRMINMAX = "StrMinMax";
    private static final String DFN_FUNC_STRNOTNULLOREMPTY = "StrNotNullOrEmpty";
    private static final String DFN_FUNC_STRREGEX = "StrRegEx";
    private static final String DFN_FUNC_UNIQUEHASHCODE = "UniqueHashCode";
    private static final String DFN_FUNC_CONVERTNULLTO = "ConvertNullTo";
    private static final String DFN_FUNC_HASHMAPPER = "HashMapper";
    private static final String DFN_FUNC_OPTIONAL = "Optional";
    private static final String DFN_FUNC_STRREPLACE = "StrReplace";
    private static final String DFN_FUNC_TRIM = "Trim";
    private static final String DFN_FUNC_TRUNCATE = "Truncate";
    private static final String DFN_FUNC_BIGDECIMAL = "BigDecimal";
    private static final String DFN_FUNC_BOOL = "Bool";
    private static final String DFN_FUNC_CHAR = "Char";
    private static final String DFN_FUNC_DATE = "Date";
    private static final String DFN_FUNC_DOUBLE = "Double";
    private static final String DFN_FUNC_INT = "Int";
    private static final String DFN_FUNC_LONG = "Long";

    private static final String DEF_COMMENT_STRING = "";
    private static final String DEF_DELIMITER = ",";
    private static final String DEF_TERMINATOR = "\n";
    private static final String DEF_QUOTE_CHAR = "\"";
    private static final Boolean DEF_TRUNC_DUP_DELIMITERS = false;

    private final Logger m_logger = LoggerFactory.getLogger(DsvLineParser.class);

    public DsvLineParser(
            final AbstractFilter filter,
            final JsonObject config) {

        m_filter = filter;

        // Remove duplicate delimiters from input lines
        m_truncDupDelimiters = config.getBoolean(
                CONFIG_FIELD_TRUNC_DUP_DELIMITERS, DEF_TRUNC_DUP_DELIMITERS);

        // Quote char
        String quoteChar = config.getString(CONFIG_FIELD_QUOTE_CHAR, DEF_QUOTE_CHAR);
        Preconditions.checkArgument(!Strings.isNullOrEmpty(quoteChar),
                CONFIG_FIELD_QUOTE_CHAR + " is null or empty");

        // Delimiter
        String delim = config.getString(CONFIG_FIELD_DELIMITER, DEF_DELIMITER);
        Preconditions.checkArgument(!Strings.isNullOrEmpty(delim),
                CONFIG_FIELD_DELIMITER + " is null or empty");
        m_delimiter = resolveDelimiter(delim);

        // Terminator
        String term = config.getString(CONFIG_FIELD_LINE_TERMINATOR, DEF_TERMINATOR);
        Preconditions.checkArgument(!Strings.isNullOrEmpty(term),
                CONFIG_FIELD_LINE_TERMINATOR + " is null or empty");
        String terminator = resolveTerminator(term);

        //
        // Build CsvPreference
        //
        CsvPreference pref = new CsvPreference.Builder(
                quoteChar.charAt(0),
                m_delimiter,
                terminator)
                .build();

        // Comment
        final String commentString
                = config.getString(CONFIG_FIELD_COMMENT_STRING, DEF_COMMENT_STRING);

        if (commentString.length() > 0) {
            pref = new CsvPreference.Builder(pref)
                    .skipComments(new CommentMatcher() {

                        @Override
                        public boolean isComment(String line) {
                            return line.startsWith(commentString);
                        }
                    }).build();
        }

        m_csvPreference = pref;

        // Mappings
        JsonObject mapping = config.getObject(CONFIG_FIELD_MAPPING, new JsonObject());
        if (mapping.size() > 0) {

            //
            // Build lookup maps and lists
            //
            JsonObject lookupValues = mapping.getObject(CONFIG_FIELD_LOOKUP_VALUES);
            if (lookupValues != null) {
                m_lookupMaps = buildLookupMaps(lookupValues);
                m_lookupLists = buildLookupLists(lookupValues);
            } else {
                m_lookupMaps = new HashMap();
                m_lookupLists = new HashMap();
            }

            //
            // Build field names and cell processors
            //
            JsonArray fields = mapping.getArray(CONFIG_FIELD_FIELDS);
            Preconditions.checkArgument(fields != null && fields.size() > 0,
                    CONFIG_FIELD_FIELDS + " is null or empty");
            m_fieldNames = buildFieldNames(fields);
            m_cellProcessors = buildCellProcessors(fields);
        } else {
            m_cellProcessors = new CellProcessor[0];
            m_fieldNames = new String[0];
            m_lookupMaps = new HashMap();
            m_lookupLists = new HashMap();
        }
    }

    @Override
    public JsonObject[] parse(final String[] lines) throws IOException {

        List<JsonObject> events = new ArrayList();
        boolean truncDupDelimiters = m_truncDupDelimiters;
        int delimiter = m_delimiter;
        CsvPreference pref = m_csvPreference;

        Pattern delimPattern = null;
        String delimStr = String.copyValueOf(Character.toChars(delimiter));
        if (truncDupDelimiters) {
            m_logger.debug("Truncating duplicate delimiters");
            delimPattern = Pattern.compile(delimStr + "+");
        }

        StringBuilder sb = new StringBuilder();
        for (String line : lines) {
            if (delimPattern != null) {
                Matcher m = delimPattern.matcher(line);
                line = m.replaceAll(delimStr);
            }
            m_logger.trace("line: {}", line);
            sb.append(line);
            sb.append(pref.getEndOfLineSymbols());
        }

        ICsvListReader listReader = new CsvListReader(
                new StringReader(sb.toString()),
                pref);

        List<Object> values;
        AbstractFilter filter = m_filter;
        String[] fieldNames = m_fieldNames;

        CellProcessor[] processors = m_cellProcessors;
        while ((values = listReader.read(processors)) != null) {
            //
            // Build one JsonObject event per row
            //                
            // @TODO support setting of event type (metric or notification)
            JsonObject event = Events.createNotificationEvent(
                    filter,
                    HostOs.hostName(),
                    EVENT_PRIORITY_NORMAL,
                    "",
                    "");            
            
            for (int i = 0; i < values.size(); i++) {
                String fieldName = fieldNames[i];
                Object value = values.get(i);
                //
                // Append to existing value
                //
                if (event.containsField(fieldName)) {
                    sb = new StringBuilder(event.getString(fieldName));
                    sb.append(value);                   
                    event.putValue(fieldName, sb.toString());
                } else {
                    event.putValue(fieldName, value);
                }
            }
            events.add(event);
        }

        return events.toArray(new JsonObject[events.size()]);
    }

    private String resolveTerminator(final String def) {
        String terminator = def;
        terminator = terminator.replaceAll(DFN_CHAR_LF, "\\n");
        terminator = terminator.replaceAll(DFN_CHAR_CR, "\\r");
        return terminator;
    }

    private int resolveDelimiter(final String delim) {
        int delimiter;
        switch (delim) {
            case DFN_CHAR_SPACE:
                delimiter = 0x20;
                break;
            case DFN_CHAR_TAB:
                delimiter = 0x09;
                break;
            case DFN_CHAR_BELL:
                delimiter = 0x07;
                break;
            case DFN_CHAR_VTAB:
                delimiter = 0x0B;
                break;
            case DFN_CHAR_ESC:
                delimiter = 0x1B;
                break;
            case DFN_CHAR_FF:
                delimiter = 0x0C;
                break;
            case DFN_CHAR_NULL:
                delimiter = 0x0;
                break;
            default:
                delimiter = delim.charAt(0);
                break;
        }
        return delimiter;
    }

    private long[] resolveIgnoreLines(final String def) {
        long[] ignores = new long[2];

        int pos = def.indexOf('-');
        if (pos > 0) {
            ignores[0] = Long.parseLong(def.substring(0, pos));
            ignores[1] = Long.parseLong(def.substring(pos + 1));
        } else {
            ignores[0] = Long.parseLong(def);
            ignores[1] = ignores[0];
        }

        return ignores;
    }

    private Map<String, Map> buildLookupMaps(final JsonObject lookupValues) {
        Map<String, Map> lookupMaps = new HashMap();

        Set<String> fields = lookupValues.getFieldNames();
        for (String field : fields) {
            JsonElement values = lookupValues.getElement(field);
            if (values instanceof JsonObject) {
                lookupMaps.put(field, ((JsonObject) values).toMap());
            }
        }

        return lookupMaps;
    }

    private Map<String, List> buildLookupLists(final JsonObject lookupValues) {
        Map<String, List> lookupLists = new HashMap();

        Set<String> fields = lookupValues.getFieldNames();
        for (String field : fields) {
            JsonElement values = lookupValues.getElement(field);
            if (values instanceof JsonArray) {
                lookupLists.put(field, ((JsonArray) values).toList());
            }
        }

        return lookupLists;
    }

    private String[] buildFieldNames(final JsonArray fields) {
        String[] names = new String[fields.size()];
        for (int i = 0; i < fields.size(); i++) {
            JsonArray def = fields.get(i);
            // Sanity check
            if (def == null || def.size() == 0) {
                throw new IllegalArgumentException(CONFIG_FIELD_FIELDS
                        + " is missing field names");
            }
            names[i] = def.get(0);
        }
        return names;
    }

    private CellProcessor[] buildCellProcessors(final JsonArray fields) {
        CellProcessor[] processors = new CellProcessor[fields.size()];
        for (int i = 0; i < fields.size(); i++) {
            JsonArray def = fields.get(i);
            String fieldName = def.get(0);

            // Any cell processing definitions?
            int len = def.size();
            if (len > 1) {
                CellProcessor nextProcessor = null;
                for (int j = (len - 1); j > 0; j--) {
                    String procDef = def.get(j);
                    processors[i] = buildCellProcessor(nextProcessor, fieldName, procDef);
                    nextProcessor = processors[i];
                }
            } else {

                // Nope, use "Optional" as default
                processors[i] = new Optional();
            }
        }
        return processors;
    }

    private CellProcessor buildCellProcessor(
            final CellProcessor nextProcessor,
            final String fieldName,
            final String def) {

        CellProcessor processor = null;
        //
        // Sanity check
        //
        Preconditions.checkArgument(!Strings.isNullOrEmpty(def),
                "Cell processor definition is null or empty for field: "
                + fieldName);

        m_logger.debug("Parsing cell processor for \"{}\": {}", fieldName, def);

        if (def.startsWith(DFN_FUNC_DMINMAX)) {
            processor = parseDMinMax(nextProcessor, def);
        } else if (def.startsWith(DFN_FUNC_FORBIDSUBSTR)) {
            processor = parseForbidSubStr(nextProcessor, def);
        } else if (def.startsWith(DFN_FUNC_LMINMAX)) {
            processor = parseLMinMax(nextProcessor, def);
        } else if (def.startsWith(DFN_FUNC_NOTNULL)) {
            if (nextProcessor != null) {
                processor = new NotNull(nextProcessor);
            } else {
                processor = new NotNull();
            }
        } else if (def.startsWith(DFN_FUNC_REQUIRESUBSTR)) {
            processor = parseRequireSubStr(nextProcessor, def);
        } else if (def.startsWith(DFN_FUNC_STRMINMAX)) {
            processor = parseStrMinMax(nextProcessor, def);
        } else if (def.startsWith(DFN_FUNC_STRNOTNULLOREMPTY)) {
            if (nextProcessor != null) {
                processor = new StrNotNullOrEmpty(nextProcessor);
            } else {
                processor = new StrNotNullOrEmpty();
            }
        } else if (def.startsWith(DFN_FUNC_STRREGEX)) {
            processor = parseStrRegEx(nextProcessor, def);
        } else if (def.startsWith(DFN_FUNC_UNIQUEHASHCODE)) {
            if (nextProcessor != null) {
                processor = new UniqueHashCode(nextProcessor);
            } else {
                processor = new UniqueHashCode();
            }
        } else if (def.startsWith(DFN_FUNC_CONVERTNULLTO)) {
            processor = parseConvertNullTo(nextProcessor, def);
        } else if (def.startsWith(DFN_FUNC_HASHMAPPER)) {
            processor = parseHashMapper(nextProcessor, def);
        } else if (def.startsWith(DFN_FUNC_OPTIONAL)) {
            if (nextProcessor != null) {
                processor = new Optional(nextProcessor);
            } else {
                processor = new Optional();
            }
        } else if (def.startsWith(DFN_FUNC_STRREPLACE)) {
            processor = parseStrReplace(nextProcessor, def);
        } else if (def.startsWith(DFN_FUNC_TRIM)) {
            if (nextProcessor != null) {
                processor = new Trim((StringCellProcessor) nextProcessor);
            } else {
                processor = new Trim();
            }
        } else if (def.startsWith(DFN_FUNC_TRUNCATE)) {
            processor = parseTruncate(nextProcessor, def);
        } else if (def.startsWith(DFN_FUNC_BIGDECIMAL)) {
            processor = parseBigDecimal(nextProcessor, def);
        } else if (def.startsWith(DFN_FUNC_BOOL)) {
            processor = parseBool(nextProcessor, def);
        } else if (def.startsWith(DFN_FUNC_CHAR)) {
            if (nextProcessor != null) {
                processor = new ParseChar((DoubleCellProcessor) nextProcessor);
            } else {
                processor = new ParseChar();
            }
        } else if (def.startsWith(DFN_FUNC_DATE)) {
            processor = parseDate(nextProcessor, def);
        } else if (def.startsWith(DFN_FUNC_DOUBLE)) {
            if (nextProcessor != null) {
                processor = new ParseDouble((DoubleCellProcessor) nextProcessor);
            } else {
                processor = new ParseDouble();
            }
        } else if (def.startsWith(DFN_FUNC_INT)) {
            if (nextProcessor != null) {
                processor = new ParseInt((LongCellProcessor) nextProcessor);
            } else {
                processor = new ParseInt();
            }
        } else if (def.startsWith(DFN_FUNC_LONG)) {
            if (nextProcessor != null) {
                processor = new ParseLong((LongCellProcessor) nextProcessor);
            } else {
                processor = new ParseLong();
            }
        } else {
            throw new IllegalArgumentException("Unsupported constraint"
                    + " or function: " + def);
        }

        return processor;
    }

    private String[] parseParams(final String def) {

        String[] params = new String[]{"", ""};
        int startPos = def.indexOf('(');
        int endPos = def.lastIndexOf(')');

        if (startPos > 0 && endPos > startPos) {
            int index = 0;
            int pos = startPos;
            while ((pos = def.indexOf(',', pos + 1)) > 0) {
                params[index++] = def.substring(startPos + 1, pos);
                startPos = pos;
            }
            params[index] = def.substring(startPos + 1, endPos);
        }

        return params;
    }

    private CellProcessor parseDMinMax(
            final CellProcessor nextProcessor,
            final String def) {

        String[] params = parseParams(def);

        Preconditions.checkArgument(
                params[0].length() > 0 && params[1].length() > 0,
                "min and max values cannot be empty: " + def);

        DMinMax processor;
        if (nextProcessor != null) {

            Preconditions.checkArgument(
                    nextProcessor instanceof DoubleCellProcessor,
                    "the next processor must be of type DoubleCellProcessor");

            processor = new DMinMax(
                    Double.parseDouble(params[0]),
                    Double.parseDouble(params[1]),
                    (DoubleCellProcessor) nextProcessor);
        } else {
            processor = new DMinMax(
                    Double.parseDouble(params[0]),
                    Double.parseDouble(params[1]));
        }
        return processor;
    }

    private CellProcessor parseForbidSubStr(
            final CellProcessor nextProcessor,
            final String def) {

        return null;
    }

    private CellProcessor parseLMinMax(
            final CellProcessor nextProcessor,
            final String def) {

        return null;
    }

    private CellProcessor parseRequireSubStr(
            final CellProcessor nextProcessor,
            final String def) {

        return null;
    }

    private CellProcessor parseStrMinMax(
            final CellProcessor nextProcessor,
            final String def) {

        return null;
    }

    private CellProcessor parseStrRegEx(
            final CellProcessor nextProcessor,
            final String def) {

        return null;
    }

    private CellProcessor parseConvertNullTo(
            final CellProcessor nextProcessor,
            final String def) {

        return null;
    }

    private CellProcessor parseHashMapper(
            final CellProcessor nextProcessor,
            final String def) {

        return null;
    }

    private CellProcessor parseStrReplace(
            final CellProcessor nextProcessor,
            final String def) {

        String[] params = parseParams(def);

        Preconditions.checkArgument(
                params[0].length() > 0 && params[1].length() > 0,
                "regex and replacemenet cannot be empty: " + def);

        StrReplace processor;
        if (nextProcessor != null) {

            Preconditions.checkArgument(
                    nextProcessor instanceof StringCellProcessor,
                    "the next processor must be of type StringCellProcessor");

            processor = new StrReplace(
                    params[0],
                    params[1],
                    (StringCellProcessor) nextProcessor);
        } else {
            processor = new StrReplace(
                    params[0],
                    params[1]);
        }
        return processor;
    }

    private CellProcessor parseTruncate(
            final CellProcessor nextProcessor,
            final String def) {

        return null;
    }

    private CellProcessor parseBigDecimal(
            final CellProcessor nextProcessor,
            final String def) {

        return null;
    }

    private CellProcessor parseBool(
            final CellProcessor nextProcessor,
            final String def) {

        return null;
    }

    private CellProcessor parseDate(
            final CellProcessor nextProcessor,
            final String def) {

        return null;
    }
}
