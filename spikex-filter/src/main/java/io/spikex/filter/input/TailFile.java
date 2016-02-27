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
package io.spikex.filter.input;

import com.google.common.base.Preconditions;
import io.spikex.core.AbstractFilter;
import io.spikex.filter.internal.AsyncFileWatcher;
import io.spikex.filter.internal.VertxFileHandler;
import static io.spikex.filter.internal.VertxFileHandler.EVENT_ADDRESS;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import org.vertx.java.core.json.JsonArray;

/**
 * Filter that listens to file changes and emits new rows as events. This is
 * typically used to "tail" log files. It is possible that this filter misses
 * some log lines during log rollover.
 * <p>
 * This filter can operate in two different modes: nio and interval. Interval
 * mode is based on a timer that periodically watches for file changes. The nio
 * mode uses the
 * <a href="http://docs.oracle.com/javase/7/docs/api/java/nio/file/WatchService.html">WatchService</a>
 * of Java 7. This mode works well on Windows and Linux. Please note that this
 * filter does not work well on OS X.
 * <p>
 * This filter has been tested on Linux, Windows, Solaris, FreeBSD and OS X.
 * <p>
 * Alias: <b>TailFile</b><br>
 * Name: <b>io.spikex.filter.input.TailFile</b><br>
 * <p>
 * Parameters:
 *
 * <table summary="">
 * <tr><th>Name</th><th>Values</th><th>Description</th></tr>
 *
 * <tr>
 * <td>worker</td>
 * <td>true or false</td>
 * <td>Run this filter as a worker verticle. The recommended and default value
 * is true.</td></tr>
 * <tr>
 *
 * <tr>
 * <td>paths</td>
 * <td>list of strings</td>
 * <td>The list of files to tail.</td>
 * </tr>
 *
 * <tr>
 * <td>encoding</td>
 * <td>string</td>
 * <td>The character set encoding used by the files we are tailing. Default is
 * "UTF-8".</td>
 * </tr>
 *
 * <tr>
 * <td>delimiter</td>
 * <td>string</td>
 * <td>The line delimiter used by the files we are tailing. Default is platform
 * specific.</td>
 * </tr>
 *
 * <tr>
 * <td>mode</td>
 * <td>string</td>
 * <td>The mode of tailing. The nio mode works well on Linux, Solaris and
 * Windows. Please consider using the "interval" mode on other platforms. You
 * can set the interval by defining it after the mode string delimited by a
 * colon:
 * <pre>interval:50</pre>. The interval value is in milliseconds and the default
 * value is 150 ms. Default mode is "nio".</td>
 * </tr>
 *
 * <tr>
 * <td>data-dir</td>
 * <td>directory path</td>
 * <td>The directory where book keeping files are kept. Default is
 * "var/tail"</td>
 * </tr>
 *
 * <tr>
 * <td>read-buf-size</td>
 * <td>integer</td>
 * <td>The minimal read buffer size. Default is 800 bytes</td>
 * </tr>
 *
 * <tr>
 * <td>tags</td>
 * <td>string</td>
 * <td>Add the listed tags to the event.</td>
 * </tr>
 *
 * </table>
 * <p>
 * Output format:
 * <pre>
 *  {
 *      &lt;standard event fields&gt;
 *      ...
 *      "@message": "&lt;file row&gt;"
 *  }
 * </pre>
 * <p>
 * Example:
 * <pre>
 *  "input": {
 *          "TailFile": {
 *                      "paths": [ "/var/log/node1/catalina.log",
 *                                  "/var/log/node2/catalina.log" ]
 *                      }
 *          },
 *  "filter": {
 *          "RegExp": {
 *                      "grok-urls": [ "http://grokified.shangri-la.fi/patterns-base.grok",
 *                                      "file:///home/grok/patterns-log.grok" ],
 *                      "match-line": {
 *                                  "pattern": "%{JAVALOG4J:tomcat}",
 *                                  "tags": [ "log", "java" ]
 *                              },
 *                      "multi-line": {
 *                                      "pattern": "(^.+Exception: .+)|(^\s+at .+)|(^\s+... \d+ more)|(^\s*Caused by:.+)",
 *                                      "prev-line-count": 2,
 *                                      "max-line-count": 1000,
 *                                      "tags": [ "error", "exception", "java" ]
 *                                  }
 *                      }
 *          },
 *  ...
 * </pre>
 * <p>
 * References:<br>
 * http://www.brics.dk/automaton/doc/index.html?dk/brics/automaton/RegExp.html<br>
 * https://github.com/cloudera/flume/blob/master/flume-core/src/main/java/com/cloudera/flume/handlers/text/TailSource.java<br>
 * http://www.greentelligent.com/java/tailinputstream<br>
 * http://stackoverflow.com/questions/557844/java-io-implementation-of-unix-linux-tail-f<br>
 * http://stackoverflow.com/questions/20233950/tail-10-implementation-for-large-log-file-using-java<br>
 * http://mohammed-technical.blogspot.fi/2011/02/how-to-read-file-without-locking-in.html
 * http://bugs.java.com/view_bug.do?bug_id=6357433
 * https://issues.apache.org/jira/browse/HADOOP-8564
 * https://issues.apache.org/jira/browse/LUCENE-4848
 *
 * @author cli
 */
public final class TailFile extends AbstractFilter {

    private long m_timerId;

    // Line handler
    private VertxFileHandler m_fileHandler;

    //
    // Configuration defaults
    //
    private static final String DEF_FILE_ENCODING = "UTF-8";
    private static final int DEF_MIN_READ_SIZE = 800; // 800 bytes
    private static final int DEF_MAX_READ_SIZE = 1024 * 1024; // 1 MB
    private static final long DEF_INTERVAL = 200L; // 200 ms
    private static final boolean DEF_START_AT_EOF = true;
    private static final boolean DEF_READ_CHUNKS = false; // Chunks of max read size

    private static final String CONF_KEY_ENCODING = "encoding";
    private static final String CONF_KEY_DELIMITER = "delimiter";
    private static final String CONF_KEY_MIN_READ_SIZE = "min-read-size";
    private static final String CONF_KEY_MAX_READ_SIZE = "max-read-size";
    private static final String CONF_KEY_PATHS = "paths";
    private static final String CONF_KEY_INTERVAL = "interval";
    private static final String CONF_KEY_START_AT_EOF = "start-at-eof";
    private static final String CONF_KEY_READ_CHUNKS = "read-chunks";

    @Override
    protected void startFilter() {

        String encoding = config().getString(CONF_KEY_ENCODING, DEF_FILE_ENCODING);
        String delimiter = config().getString(CONF_KEY_DELIMITER, System.lineSeparator());
        int minReadSize = config().getInteger(CONF_KEY_MIN_READ_SIZE, DEF_MIN_READ_SIZE);
        int maxReadSize = config().getInteger(CONF_KEY_MAX_READ_SIZE, DEF_MAX_READ_SIZE);
        boolean startAtEof = config().getBoolean(CONF_KEY_START_AT_EOF, DEF_START_AT_EOF);
        boolean readChunks = config().getBoolean(CONF_KEY_READ_CHUNKS, DEF_READ_CHUNKS);
        JsonArray cfgPaths = config().getArray(CONF_KEY_PATHS);
        // Watch dir every 150 ms by default
        long interval = config().getLong(CONF_KEY_INTERVAL, DEF_INTERVAL);

        // Sanity checks
        Preconditions.checkArgument(encoding != null && encoding.length() > 0, "encoding cannot be empty");
        Preconditions.checkArgument(delimiter != null && delimiter.length() > 0, "delimiter cannot be empty");
        Preconditions.checkArgument(interval > 0, "interval must be greater than zero");
        Preconditions.checkArgument(minReadSize > 0, "min-read-size must be greater than zero");
        Preconditions.checkArgument(maxReadSize > 0, "max-read-size must be greater than zero");
        Preconditions.checkArgument(cfgPaths != null, "paths must be specified");
        Preconditions.checkArgument(cfgPaths.size() > 0, "please specify at least one path");

        // Allow non-existent files to be added
        List<Path> paths = new ArrayList();
        for (int i = 0; i < cfgPaths.size(); i++) {
            String cfgPath = cfgPaths.get(i);
            Path path = Paths.get(cfgPath).toAbsolutePath();
            paths.add(path);
        }

        // Start listening to file changes
        if (paths.size() > 0) {
            m_fileHandler = new VertxFileHandler(
                    this,
                    eventBus(),
                    encoding,
                    delimiter,
                    minReadSize,
                    maxReadSize,
                    startAtEof);

            eventBus().registerLocalHandler(EVENT_ADDRESS,
                    m_fileHandler);

            // Timer based tailing
            logger().debug("Tailing files: {}", paths);
            m_timerId = vertx.setPeriodic(interval,
                    new AsyncFileWatcher(
                            eventBus(),
                            vertx.fileSystem(),
                            paths,
                            readChunks,
                            maxReadSize));
        }
    }

    @Override
    protected void stopFilter() {

        // Stop timer
        vertx.cancelTimer(m_timerId);

        // Stop listening to changes
        eventBus().unregisterHandler(EVENT_ADDRESS, m_fileHandler);
    }
}
