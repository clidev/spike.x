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

import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author cli
 */
public class DefaultProcessHandler extends AbstractProcessHandler {

    private ChildProcess m_process;

    private final Logger m_logger = LoggerFactory.getLogger(getClass());

    public boolean isRunning() {
        boolean running = false;
        ChildProcess process = getProcess();
        if (process != null) {
            running = process.isRunning();
        }
        return running;
    }

    @Override
    public void onStart(final ChildProcess process) {
        m_process = process;
        m_logger.debug("Executing command: {} timeout: {}",
                process.getArgs(),
                process.getTimeout());
    }

    @Override
    public void onExit(final int statusCode) {
        m_logger.debug("Command exited with status: {}", statusCode);
    }

    @Override
    public void onStdout(
            final ByteBuffer buffer,
            final boolean closed) {

        ChildProcess process = getProcess();
        if (process != null && process.isRunning()) {
            if (buffer != null) {
                handleStdout(getText(buffer));
            }
            closeStdin();
        }
    }

    @Override
    public void onStderr(
            final ByteBuffer buffer,
            final boolean closed) {

        ChildProcess process = getProcess();
        if (process != null && process.isRunning()) {
            if (buffer != null) {
                handleStderr(getText(buffer));
            }
            closeStdin();
        }
    }

    @Override
    public boolean onStdinReady(final ByteBuffer buffer) {
        // Do nothing by default
        return false;
    }

    protected void handleStdout(final String txt) {
        // Just log it..
        if (txt != null
                && txt.length() > 0) {
            m_logger.info("{}", txt);
        }
    }

    protected void handleStderr(final String txt) {
        // Just log it..
        if (txt != null
                && txt.length() > 0) {
            m_logger.error("{}", txt);
        }
    }

    protected ChildProcess getProcess() {
        return m_process;
    }

    protected Logger logger() {
        return m_logger;
    }

    protected void closeStdin() {

        ChildProcess process = getProcess();

        if (process != null) {
            if (process.closeStdin()) {
                // Ready, process can exit
                m_logger.trace("Closed stdin");
            }
        }
    }

    private String getText(final ByteBuffer buffer) {

        byte[] bytes = new byte[buffer.remaining()];
        buffer.get(bytes);

        Charset encoding = StandardCharsets.UTF_8;
        ChildProcess process = getProcess();

        if (process != null) {
            encoding = process.getEncoding();
        }

        return new String(bytes, encoding);
    }
}
