/**
 *
 * Copyright (c) 2015 NG Modular Oy.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
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
 * Default version implementation.
 *
 * @version $Revision: 66 $
 * @author cli
 */
public final class Version extends AbstractVersion {

    private Version(
            final String id,
            final int version) {

        super(id, version);
    }

    public Version increment() {
        return Version.create(getId(), getSequence() + 1);
    }

    public Version decrement() {
        return Version.create(getId(), getSequence() - 1);
    }

    public static Version create(
            final String id,
            final int version) {
        return new Version(id, version);
    }

    public static Version none() {
        return new Version("", Version.VERSION_NULL);
    }

    public static Version latest(final String id) {
        return new Version(id, Version.VERSION_LATEST);
    }
}
