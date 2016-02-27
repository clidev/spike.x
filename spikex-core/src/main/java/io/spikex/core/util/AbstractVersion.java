/**
 *
 * Copyright (c) 2015 NG Modular Oy.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */
package io.spikex.core.util;

/**
 * Abstract base class for version implementations.
 *
 * @author cli
 */
public abstract class AbstractVersion implements IVersion {

    /**
     * Represents a non-version
     */
    protected static final int VERSION_NULL = -1;
    /**
     * Represents the latest version
     */
    protected static final int VERSION_LATEST = 0;
    //
    private final String m_id;
    private final int m_version;
    private final long m_timestamp;
    //
    private volatile int m_hashCode;

    public AbstractVersion(
            final String id,
            final int version) {
        //
        // Sanity checks
        //
        if (id == null) {
            throw new IllegalArgumentException("id is null");
        }
        if (version < -1) {
            throw new IllegalArgumentException("version is negative");
        }
        m_id = id;
        m_version = version;
        m_timestamp = System.currentTimeMillis();
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
            hashCode = 47;
            hashCode = 3 * hashCode + m_id.hashCode();
            hashCode = 3 * hashCode + getSequence();
            m_hashCode = hashCode;
        }
        return hashCode;
    }

    @Override
    public boolean isLatest() {
        return (getSequence() == VERSION_LATEST);
    }

    @Override
    public boolean isNull() {
        return (getSequence() == VERSION_NULL);
    }

    @Override
    public String getId() {
        return m_id;
    }

    @Override
    public int getSequence() {
        return m_version;
    }

    @Override
    public long getCreationTimestamp() {
        return m_timestamp;
    }

    @Override
    public String toString() {
        String sname = getClass().getSimpleName();
        StringBuilder sb = new StringBuilder(sname);
        sb.append("[");
        sb.append(hashCode());
        sb.append("] id: ");
        sb.append(getId());
        sb.append(" sequence: ");
        sb.append(getSequence());
        sb.append(" tm: ");
        sb.append(getCreationTimestamp());
        return sb.toString();
    }

    public static int latestVersion() {
        return VERSION_LATEST;
    }

    public static int nullVersion() {
        return VERSION_NULL;
    }
}
