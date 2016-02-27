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
package io.spikex.core.util.resource;

import java.net.URI;
import java.nio.charset.Charset;
import java.util.Locale;
import io.spikex.core.util.IVersion;

/**
 *
 * @author cli
 */
public final class ResourceChangeEvent {

    public enum State {

        LOADED, CHANGED, SAVED
    }

    private final String m_name; // The resource name (eg. settings)
    private final String m_suffix; // The suffix of the filename
    private final IVersion m_version; // The resource version
    private final Locale m_locale; // The resource locale
    private final Charset m_encoding; // Character set used by the resource (canonical name)
    private final URI m_location; // The resolved full path, including filename
    private final State m_state;
    //
    private volatile int m_hashCode;

    public ResourceChangeEvent(
            final IResource resource,
            final State state) {

        m_name = resource.getName();
        m_suffix = resource.getSuffix();
        m_version = resource.getVersion();
        m_locale = resource.getLocale();
        m_encoding = resource.getEncoding();
        m_location = resource.getLocation();
        m_state = state;
    }

    /**
     * Please see {@link java.lang.Object#equals} for documentation.
     */
    @Override
    public boolean equals(final Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null) {
            return false;
        }
        if (obj.getClass() != getClass()) {
            return false;
        }
        return (hashCode() == obj.hashCode());
    }

    /**
     * Please see {@link java.lang.Object#hashCode} for documentation.
     */
    @Override
    public int hashCode() {
        // Racy single-check is acceptable for hashCode
        // [Effective Java, Joshua Block, Item 71]
        int hashCode = m_hashCode;
        if (hashCode == 0) {
            hashCode = 73;
            hashCode = 3 * hashCode + m_name.hashCode();
            hashCode = 3 * hashCode + m_suffix.hashCode();
            hashCode = 3 * hashCode + m_locale.hashCode();
            hashCode = 3 * hashCode + m_version.hashCode();
            hashCode = 3 * hashCode + m_encoding.hashCode();
            hashCode = 3 * hashCode + m_location.hashCode();
            m_hashCode = hashCode;
        }
        return hashCode;
    }

    public final String getName() {
        return m_name;
    }

    public final String getSuffix() {
        return m_suffix;
    }

    public final IVersion getVersion() {
        return m_version;
    }

    public final Locale getLocale() {
        return m_locale;
    }

    public final Charset getEncoding() {
        return m_encoding;
    }

    public final URI getLocation() {
        return m_location;
    }

    public final State getState() {
        return m_state;
    }
}
