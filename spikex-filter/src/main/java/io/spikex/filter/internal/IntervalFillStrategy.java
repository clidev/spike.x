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

/**
 *
 * @author cli
 */
public final class IntervalFillStrategy {

    private final long m_tokens;
    private final long m_interval; // ms
    private long m_lastEmit;

    public IntervalFillStrategy(
            final long tokens,
            final long interval,
            final long lastEmit) {

        m_tokens = tokens;
        m_interval = interval;
        m_lastEmit = lastEmit;
    }

    public long getLastEmit() {
        return m_lastEmit;
    }

    public long refill() {
        long now = System.currentTimeMillis();
        if (now < m_lastEmit) {
            return 0;
        }
        m_lastEmit = now + m_interval;
        return m_tokens;
    }
}
