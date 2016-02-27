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

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.jar.Attributes;
import java.util.jar.Manifest;
import io.spikex.core.util.IVersion;
import io.spikex.core.util.Version;

/**
 *
 * @author cli
 */
public final class Manifests {

    public static Attributes.Name KEY_RESOURCE_VERSION
            = new Attributes.Name("Resource-Version");
    public static final Attributes.Name KEY_RESOURCE_NAME
            = new Attributes.Name("Resource-Name");
    public static final Attributes.Name KEY_RESOURCE_TIMESTAMP
            = new Attributes.Name("Resource-Timestamp");
    public static final Attributes.Name KEY_RESOURCE_SUFFIX
            = new Attributes.Name("Resource-Suffix");
    public static final Attributes.Name KEY_META_VERSION
            = new Attributes.Name("Meta-Version");
    public static final int META_VERSION = 1;

    public static IVersion readLatestVersion(
            final IResource resource,
            final InputStream in) throws IOException {

        Manifest mf = new Manifest();
        mf.read(in);

        int seq = Version.nullVersion();
        Attributes attrs = mf.getMainAttributes();
        if (attrs != null
                && attrs.containsKey(KEY_RESOURCE_VERSION)) {
            seq = Integer.parseInt(attrs.getValue(KEY_RESOURCE_VERSION));
        }

        return Version.create(resource.getName(), seq);
    }

    public static void writeLatestVersion(
            final IResource resource,
            final OutputStream out) throws IOException {

        IVersion version = resource.getVersion();
        Manifest mf = new Manifest();
        Attributes attrs = mf.getMainAttributes();
        attrs.put(Attributes.Name.MANIFEST_VERSION, "1.0");
        attrs.put(KEY_META_VERSION, String.valueOf(META_VERSION));
        attrs.put(KEY_RESOURCE_NAME, resource.getName());
        attrs.put(KEY_RESOURCE_SUFFIX, resource.getSuffix());
        attrs.put(KEY_RESOURCE_TIMESTAMP, String.valueOf(System.currentTimeMillis()));
        attrs.put(KEY_RESOURCE_VERSION, String.valueOf(version.getSequence()));
        mf.write(out);
    }
}
