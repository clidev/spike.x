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

import com.zaxxer.nuprocess.NuProcess;
import java.nio.charset.Charset;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 *
 * @author cli
 */
public final class ChildProcess {

    private final NuProcess m_process;
    private final ProcessSettings m_settings;
    private final AtomicBoolean m_stdinClosed;

    ChildProcess(
            final NuProcess process,
            final ProcessSettings settings) {

        m_process = process;
        m_settings = settings;
        m_stdinClosed = new AtomicBoolean(false);

        // Handle write in callback
        if (settings.getWantWrite()) {
            m_process.wantWrite();
        }
    }

    public boolean isRunning() {
        return m_process.isRunning();
    }

    public <T extends AbstractProcessHandler> T getHandler() {
        return (T) m_settings.getHandler();
    }

    public List<String> getArgs() {
        return m_settings.getArgs();
    }

    public long getTimeout() {
        return m_settings.getTimeout();
    }

    public Charset getEncoding() {
        return m_settings.getEncoding();
    }

    public boolean getWantWrite() {
        return m_settings.getWantWrite();
    }

    public int waitForExit() throws InterruptedException {
        return m_process.waitFor(
                m_settings.getTimeout(),
                TimeUnit.MILLISECONDS);
    }

    public boolean closeStdin() {
        boolean closed = m_stdinClosed.compareAndSet(false, true);
        if (closed) {
            m_process.closeStdin();
        }
        return closed;
    }

    public void destroy(final boolean force) {
        m_process.destroy(force);
    }

    public void wantWrite() {
        m_process.wantWrite();
    }

    static class ProcessSettings {

        private final AbstractProcessHandler m_handler;
        private final List<String> m_args;
        private final long m_timeout; // ms
        private final Charset m_encoding;
        private final boolean m_wantWrite;

        ProcessSettings(
                final AbstractProcessHandler handler,
                final List<String> args,
                final long timeout,
                final Charset encoding,
                final boolean wantWrite) {

            m_handler = handler;
            m_args = args;
            m_timeout = timeout;
            m_encoding = encoding;
            m_wantWrite = wantWrite;
        }

        public AbstractProcessHandler getHandler() {
            return m_handler;
        }

        public List<String> getArgs() {
            return m_args;
        }

        public long getTimeout() {
            return m_timeout;
        }

        public Charset getEncoding() {
            return m_encoding;
        }

        public boolean getWantWrite() {
            return m_wantWrite;
        }
    }
}
