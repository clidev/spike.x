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

import java.io.ByteArrayOutputStream;
import java.io.PrintWriter;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;

/**
 *
 * @author cli
 */
public abstract class LineWriter extends DefaultProcessHandler {

    @Override
    public void onStart(final ChildProcess process) {
        super.onStart(process);
        process.wantWrite();
    }

    @Override
    public void onStdout(
            final ByteBuffer buffer,
            final boolean closed) {

        if (buffer != null) {

            byte[] bytes = new byte[buffer.remaining()];
            buffer.get(bytes);

            Charset encoding = StandardCharsets.UTF_8;
            ChildProcess process = getProcess();

            if (process != null) {
                encoding = process.getEncoding();
            }

            logger().info("{}", new String(bytes, encoding));
        }
        closeStdin();
    }

    @Override
    public boolean onStdinReady(final ByteBuffer buffer) {

        boolean moreToWrite = false;

        // Fill writer with lines
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        try (PrintWriter writer = new PrintWriter(baos)) {
            moreToWrite = onWrite(writer);
            writer.flush();
        } catch (Exception e) {
            logger().error("Failure in onWrite", e);
        }

        // Push lines to buffer
        buffer.put(baos.toByteArray());
        buffer.flip();

        // Close stdin and make process exit if we're done
        if (!moreToWrite) {
            closeStdin();
        }

        return moreToWrite;
    }

    protected abstract boolean onWrite(final PrintWriter out);
}
