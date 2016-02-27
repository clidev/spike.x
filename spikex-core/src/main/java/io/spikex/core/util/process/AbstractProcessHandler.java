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
import com.zaxxer.nuprocess.NuProcessHandler;
import io.spikex.core.util.process.ChildProcess.ProcessSettings;
import java.nio.ByteBuffer;

/**
 *
 * @author cli
 */
public abstract class AbstractProcessHandler {

    public void onPreStart(final ChildProcess process) {
        // Do nothing by default
    }

    public void onStart(final ChildProcess process) {
        // Do nothing by default
    }

    public void onExit(final int statusCode) {
        // Do nothing by default
    }

    public void onStdout(
            final ByteBuffer buffer,
            final boolean closed) {

        // Ensure we consume the entire buffer in case it's not used.
        buffer.position(buffer.limit());
    }

    public void onStderr(
            final ByteBuffer buffer,
            final boolean closed) {

        // Ensure we consume the entire buffer in case it's not used.
        buffer.position(buffer.limit());
    }

    public boolean onStdinReady(final ByteBuffer buffer) {
        // Do nothing by default
        return false;
    }

    InternalProcessHandler nuHandler(final ProcessSettings settings) {
        return new InternalProcessHandler(this, settings);
    }

    private class InternalProcessHandler implements NuProcessHandler {

        private final AbstractProcessHandler m_handler;
        private final ProcessSettings m_settings;

        private InternalProcessHandler(
                final AbstractProcessHandler handler,
                final ProcessSettings settings) {

            m_handler = handler;
            m_settings = settings;
        }

        @Override
        public void onPreStart(final NuProcess process) {
            m_handler.onPreStart(new ChildProcess(process, m_settings));
        }

        @Override
        public void onStart(final NuProcess process) {
            m_handler.onStart(new ChildProcess(process, m_settings));
        }

        @Override
        public void onExit(final int statusCode) {
            m_handler.onExit(statusCode);
        }

        @Override
        public void onStdout(
                final ByteBuffer buffer,
                final boolean closed) {

            m_handler.onStdout(buffer, closed);
        }

        @Override
        public void onStderr(
                final ByteBuffer buffer,
                final boolean closed) {

            m_handler.onStderr(buffer, closed);
        }

        @Override
        public boolean onStdinReady(final ByteBuffer buffer) {
            return m_handler.onStdinReady(buffer);
        }
    }
}
