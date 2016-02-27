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
 * Represents a versioned entity. Any versioned entity has an identifier,
 * actual version number and a creation timestamp.
 *
 * @author cli
 */
public interface IVersion {

    /**
     * Returns true if this is version represents the latest version.
     * <p>
     * @return true if latest version
     */
    public boolean isLatest();

    /**
     * Returns true if this is version represents a NULL version.
     * <p>
     * @return true if NULL version
     */
    public boolean isNull();

    /**
     * Returns the identifier of the versioned entity. This can be a primary
     * key used in a database, a "unique" filename (including path), an URI
     * pointing to a network location or any similar identifier.
     * <p>
     * @return the version identifier
     */
    public String getId();

    /**
     * Returns the actual version number of this versioned entity. The first
     * version number is 1. Zero can be used to indicate the current version.
     * <p>
     * @return the version number
     */
    public int getSequence();

    /**
     * Returns the time-of-creation of this version. This should be a UTC
     * timestamp. A natural candidate is provided by the
     * {@link java.lang.System#currentTimeMillis()} method. The milliseconds
     * since midnight, January 1, 1970 UTC.
     * <p>
     * @return the creation timestamp of this version
     */
    public long getCreationTimestamp();
}
