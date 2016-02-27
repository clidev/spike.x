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
 * Represents a resource that can be versioned. For instance, this can be used
 * to support undo operations for configuration files.
 *
 * @author cli
 * @param <V> the resource type
 */
public interface IResource<V> {

    /**
     * Returns the name of the resource. This is usually the filename, but
     * without the suffix, locale and version part.
     *
     * @return the resource name
     */
    public String getName();

    /**
     * Returns the localized name of the resource. This is usually the filename
     * combined with the locale identifier.
     *
     * @return the resource name
     */
    public String getLocalizedName();

    /**
     * Returns the fully qualified name of the resource. This is usually the filename,
     * including the suffix, locale and version.
     *
     * @return the resource name
     */
    public String getQualifiedName();

    /**
     * Returns the localized name of the resource. This is usually the filename,
     * including the suffix, locale and version.
     *
     * @param locale the locale to use
     * @return the resource name
     */
    public String getQualifiedName(Locale locale);

    /**
     * Returns the resource snapshot version.
     *
     * @return the snapshot version
     */
    public IVersion getVersion();

    /**
     * Returns the resource snapshot locale.
     *
     * @return the snapshot locale
     */
    public Locale getLocale();

    /**
     * Returns the resource snapshot character encoding.
     *
     * @return the snapshot character encoding
     */
    public Charset getEncoding();

    /**
     * Returns the resource filename suffix.
     *
     * @return the filename suffix or an empty string
     */
    public String getSuffix();

    /**
     * Returns the resource version strategy.
     *
     * @return the version strategy
     */
    public IVersionStrategy getVersionStrategy();

    /**
     * Returns the data snapshot that this resource represents.
     *
     * @return the data snapshot
     */
    public V getData();

    /**
     * Returns the location of the resource. This is usually the full path,
     * including the filename. The returned path is null if the resource
     * location and version has not yet been resolved.
     *
     * @return the resource location or null
     */
    public URI getLocation();

    /**
     * Returns true if this resource contains no data.
     *
     * @return true if resource contains no data
     */
    public boolean isEmpty();

    /**
     * Returns true if this resource supports saving of a data snapshot.
     *
     * @return true if saving is supported
     */
    public boolean isSaveable();

    /**
     * Returns true if this resource exists.
     *
     * @return true if this resource exists
     */
    public boolean exists();

    /**
     * Loads the snapshot defined by this resource.
     *
     * @param <T>
     * @return the resource snapshot
     * @throws ResourceException if loading failed
     */
    public <T extends IResource> T load() throws ResourceException;

    /**
     * Saves the snapshot defined by this resource.
     *
     * @param <T>
     * @return the resource snapshot
     * @throws ResourceException if loading failed
     */
    public <T extends IResource> T save() throws ResourceException;
}
