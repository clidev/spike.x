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

import io.spikex.core.AbstractFilter;
import io.spikex.core.util.process.ChildProcess;
import io.spikex.core.util.process.LineReader;
import io.spikex.core.util.process.LineWriter;
import io.spikex.core.util.process.ProcessExecutor;
import io.spikex.filter.internal.DsvLineParser;
import io.spikex.filter.internal.ILineParser;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.vertx.java.core.json.JsonArray;
import org.vertx.java.core.json.JsonObject;

/**
 *
 * <p>
 * Output format:
 * <pre>
 * [status code]
 * [line 1]
 * ...
 * [line n]
 * </pre>
 * <p>
 * Where status code is "0" for successful execution of command. Any other
 * status code is considered an execution failure.
 * <p>
 * Supported output format types:
 * <ul>
 * <li>dsv - delimiter separated values</li>
 * </ul>
 *
 * Example:
 * <pre>
 *  "chain": [
 *      { "Command":
 *                  "update-interval": 15000,
 *                  "command": "/bin/ls",
 *                  "args": [ "-l", "-a" ],
 *                  "timeout": 2500,
 *                  "ignore-exit-code": false,
 *                  "work-dir", "%{#spikex.tmp}",
 *                  "max-output-size": 10485760,
 *                  "skip-lines-start": 1,
 *                  "skip-lines-end": 3,
 *                  "encoding": "UTF-8",
 *                  "output-format": {
 *                                      "type": "dsv",
 *                                      "delimiter": "&lt;SPACE&gt;",
 *                                      "quote-char": '"',
 *                                      "line-terminator": "&lt;LF&gt;",
 *                                      "trunc-dup-delimiters": true,
 *                                      "mapping": {
 *                                          "lookup-values": {
 *                                                       "owners": {
 *                                                              "root": "ADMIN",
 *                                                              "john": "USER",
 *                                                              "jody": "USER"
 *                                                      },
 *                                                      "true-list": [ "y", "yes", "-" ],
 *                                                      "false-list": [ "n", "no" ]
 *                                          },
 *                                          "fields": [
 *                                              [ "permission", "NotNull" ],
 *                                              [ "inodes", "NotNull", "Long" ],
 *                                              [ "owner", "NotNull", "HashMapper(owners)" ],
 *                                              [ "group", "NotNull" ],
 *                                              [ "size", "NotNull", "Long" ],
 *                                              [ "modified", "NotNull" ],
 *                                              [ "modified", "NotNull" ],
 *                                              [ "modified", "NotNull" ],
 *                                              [ "file", "NotNull" ]
 *                                          ]
 *                                      }
 *                  }
 *      }
 * ]
 * </pre>
 *
 * @author cli
 */
public final class Command extends AbstractFilter {

    private String m_cmd;
    private String m_encoding;
    private String[] m_args;
    private Map<String, String> m_env;
    private ILineParser m_lineParser;
    private int m_skipLinesStart;
    private int m_skipLinesEnd;
    private long m_timeout;
    private long m_maxLineCount;

    private static final String CONF_KEY_COMMAND = "command";
    private static final String CONF_KEY_ARGS = "args";
    private static final String CONF_KEY_TIMEOUT = "timeout";
    private static final String CONF_KEY_ENCODING = "encoding";
    private static final String CONF_KEY_MAX_LINE_COUNT = "max-line-count";
    private static final String CONF_KEY_SKIP_LINES_START = "skip-lines-start";
    private static final String CONF_KEY_SKIP_LINES_END = "skip-lines-end";
    private static final String CONF_KEY_OUTPUT_FORMAT = "output-format";
    private static final String CONF_KEY_TYPE = "type";

    private static final String FORMAT_TYPE_DSV = "dsv";

    private static final long DEF_TIMEOUT = 2500L; // ms
    private static final long DEF_MAX_LINE_COUNT = 4000;
    private static final String DEF_ENCODING = StandardCharsets.UTF_8.name();
    private static final String DEF_OUTPUT_TYPE = "dsv";

    @Override
    protected void startFilter() {

        m_timeout = config().getLong(CONF_KEY_TIMEOUT, DEF_TIMEOUT);
        m_cmd = config().getString(CONF_KEY_COMMAND);

        List<String> args = new ArrayList();
        JsonArray argsDef = config().getArray(CONF_KEY_ARGS, new JsonArray());
        for (int i = 0; i < argsDef.size(); i++) {
            String arg = argsDef.get(i);
            args.add(arg);
        }
        m_args = args.toArray(new String[args.size()]);
        m_skipLinesStart = config().getInteger(CONF_KEY_SKIP_LINES_START, 0);
        m_skipLinesEnd = config().getInteger(CONF_KEY_SKIP_LINES_END, 0);
        m_encoding = config().getString(CONF_KEY_ENCODING, DEF_ENCODING);
        m_maxLineCount = config().getLong(CONF_KEY_MAX_LINE_COUNT,
                DEF_MAX_LINE_COUNT);

        m_env = new HashMap();
        m_env.put(CONF_KEY_NODE_NAME, config().getString(CONF_KEY_NODE_NAME));
        m_env.put(CONF_KEY_CLUSTER_NAME, config().getString(CONF_KEY_CLUSTER_NAME));
        m_env.put(CONF_KEY_LOCAL_ADDRESS, config().getString(CONF_KEY_LOCAL_ADDRESS));
        m_env.put(CONF_KEY_HOME_PATH, config().getString(CONF_KEY_HOME_PATH));
        m_env.put(CONF_KEY_CONF_PATH, config().getString(CONF_KEY_CONF_PATH));
        m_env.put(CONF_KEY_DATA_PATH, config().getString(CONF_KEY_DATA_PATH));
        m_env.put(CONF_KEY_TMP_PATH, config().getString(CONF_KEY_TMP_PATH));
        m_env.put(CONF_KEY_USER, config().getString(CONF_KEY_USER));

        JsonObject outputFormat = config().getObject(CONF_KEY_OUTPUT_FORMAT,
                new JsonObject());
        String type = outputFormat.getString(CONF_KEY_TYPE, DEF_OUTPUT_TYPE);

        switch (type) {
            case FORMAT_TYPE_DSV: {
                //
                // DSV line format
                //
                m_lineParser = new DsvLineParser(this, outputFormat);
                break;
            }
            default:
                throw new IllegalArgumentException("Unsupported output format: " + type);
        }
    }

    @Override
    public void handleTimerEvent() {

        LineReader reader = new LineReader(
                m_skipLinesStart,
                m_skipLinesEnd,
                m_maxLineCount);
        ChildProcess cmd = new ProcessExecutor()
                .command(m_cmd, m_args)
                .env(m_env)
                .encoding(m_encoding)
                .timeout(m_timeout, TimeUnit.MILLISECONDS)
                .handler(reader)
                .start();

        try {
            logger().debug("Periodic command: {}", cmd.getArgs());
            cmd.waitForExit();
            String[] lines = reader.getLines();
            JsonObject[] events = m_lineParser.parse(lines);
            for (JsonObject event : events) {
                emitEvent(event);
            }
        } catch (InterruptedException | IOException e) {
            logger().error("Failed to execute \"{}\" command with arguments: {}",
                    m_cmd, m_args, e);
        }
    }

    @Override
    protected void handleEvent(final JsonObject event) {

        final String eventStr = event.toString();
        logger().trace("Received event: {}", eventStr);

        ChildProcess cmd = new ProcessExecutor()
                .command(m_cmd, m_args)
                .env(m_env)
                .encoding(m_encoding)
                .timeout(m_timeout, TimeUnit.MILLISECONDS)
                .handler(new LineWriter() {

                    @Override
                    public boolean onWrite(final PrintWriter out) {
                        out.println(eventStr);
                        return false;
                    }

                })
                .start();

        try {
            logger().debug("Command: {}", cmd.getArgs());
            // @TODO implement support for DsvLineWriter                        
            cmd.waitForExit();
        } catch (InterruptedException e) {
            logger().error("Failed to execute \"{}\" command with arguments: {}",
                    m_cmd, m_args, e);
        }
    }
}
