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
package io.spikex.core.util.process;

import com.gs.collections.impl.list.mutable.FastList;
import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

/**
 *
 * @author cli
 */
public class LineReader extends DefaultProcessHandler {

    private final int m_skipLinesFromStart;
    private final int m_skipLinesFromEnd;
    private final long m_maxLineCount;
    private final List<String> m_lines;

    public LineReader() {
        this(0, 0, 0L);
    }

    public LineReader(final long maxLineCount) {
        this(0, 0, maxLineCount);
    }

    public LineReader(
            final int skipLinesFromStart,
            final int skipLinesFromEnd,
            final long maxLineCount) {

        m_skipLinesFromStart = skipLinesFromStart;
        m_skipLinesFromEnd = skipLinesFromEnd;
        m_maxLineCount = maxLineCount;
        m_lines = FastList.<String>newList().asSynchronized();
    }

    public long getLineCount() {
        int size = m_lines.size();
        int newSize = size - m_skipLinesFromStart - m_skipLinesFromEnd;
        return newSize;
    }

    public String getFirstLine() {
        String[] lines = getLines();
        return lines.length > 0 ? lines[0] : null;
    }

    public String[] getLines() {

        // Remove lines from head and tail
        String[] lines = new String[0];
        int size = m_lines.size();
        int headIndex = m_skipLinesFromStart;
        int tailIndex = size - m_skipLinesFromEnd;
        int newSize = size - m_skipLinesFromStart - m_skipLinesFromEnd;

        if (newSize > 0) {
            lines = m_lines.subList(headIndex, tailIndex).toArray(new String[newSize]);
        }

        return lines;
    }

    @Override
    public void onStdout(
            final ByteBuffer buffer,
            final boolean closed) {

        if (buffer != null) {

            BufferedReader reader = null;
            ChildProcess process = getProcess();

            try {
                byte[] bytes = new byte[buffer.remaining()];
                buffer.get(bytes);

                Charset encoding = StandardCharsets.UTF_8;

                if (process != null) {
                    encoding = process.getEncoding();
                }

                InputStreamReader inputReader
                        = new InputStreamReader(new ByteArrayInputStream(bytes), encoding);
                reader = new BufferedReader(inputReader);

                List<String> lines = new ArrayList();
                long curLineCount = m_lines.size();
                long maxLineCount = m_maxLineCount;

                String line;
                while ((line = reader.readLine()) != null) {

                    // Add line to list
                    lines.add(line);
                    curLineCount++;

                    //
                    // Stop processing if max line count reached
                    //
                    if (maxLineCount > 0
                            && curLineCount >= maxLineCount) {
                        closeStdin();
                        break;
                    }
                }
                m_lines.addAll(lines);

            } catch (IOException e) {
                logger().error("Failed to read lines from {}",
                        process == null ? "process" : process.getArgs(), e);
            } finally {
                if (reader != null) {
                    try {
                        reader.close();
                    } catch (IOException e) {
                        logger().error("Failed to close reader stream", e);
                    }
                }
            }
        }
        if (closed) {
            closeStdin();
        }
    }
}
