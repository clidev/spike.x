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

import com.zaxxer.nuprocess.NuProcessBuilder;
import io.spikex.core.util.process.ChildProcess.ProcessSettings;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 *
 * @author cli
 */
public final class ProcessExecutor {

    private String m_encoding;
    private long m_timeout;
    private TimeUnit m_unit;
    private boolean m_wantWrite;
    private AbstractProcessHandler m_handler;

    private final List<String> m_args;
    private final Map<String, String> m_env;

    public ProcessExecutor() {
        m_args = new ArrayList();
        m_env = new HashMap();
        m_timeout = 0L; // Wait forever
        m_unit = TimeUnit.MILLISECONDS;
        m_wantWrite = false;
    }

    public ProcessExecutor command(
            final String cmd,
            final String... args) {

        m_args.clear();
        m_args.add(cmd);
        m_args.addAll(Arrays.asList(args));

        return this;
    }

    public ProcessExecutor env(final Map<String, String> vars) {
        m_env.clear();
        m_env.putAll(vars);
        return this;
    }

    public ProcessExecutor handler(final AbstractProcessHandler handler) {
        m_handler = handler;
        return this;
    }

    public ProcessExecutor timeout(
            final long timeout,
            final TimeUnit unit) {

        m_timeout = timeout;
        m_unit = unit;
        return this;
    }

    public ProcessExecutor encoding(final String encoding) {
        m_encoding = encoding;
        return this;
    }

    public ProcessExecutor wantWrite(final boolean wantWrite) {
        m_wantWrite = wantWrite;
        return this;
    }

    public ChildProcess start() {

        NuProcessBuilder builder = new NuProcessBuilder(m_args);
        builder.environment().putAll(m_env);

        AbstractProcessHandler handler = m_handler;
        if (handler == null) {
            handler = new DefaultProcessHandler();
        }

        ProcessSettings settings = new ProcessSettings(
                handler,
                m_args,
                m_unit.convert(m_timeout, TimeUnit.MILLISECONDS),
                m_encoding == null ? StandardCharsets.UTF_8 : Charset.forName(m_encoding),
                m_wantWrite);

        builder.setProcessListener(handler.nuHandler(settings));

        return new ChildProcess(
                builder.start(),
                settings);
    }
}
