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
package io.spikex.core.util.resource;

import java.net.URI;
import io.spikex.core.util.IVersion;

/**
 * Loads and saves versioned resources of a specific type.
 *
 * @author cli
 * @param <T> the resource type
 */
public interface IResourceProvider<T extends IResource> {

    /**
     * Returns the resource type that this provider supports.
     *
     * @return the resource type
     */
    public Class getType();

    /**
     * Returns the base path of the resources. This can be the directory where
     * the resources reside.
     *
     * @return the base path of the resources
     */
    public URI getBasePath();

    /**
     * Returns true if the given resource exists.
     *
     * @param resource the resource definition
     * @return true if the resource exists
     */
    public boolean exists(T resource);

    /**
     * Resolves the latest version of the resource. This method may perform
     * reading of information from the backend used to store resources.
     *
     * @param name the name of the resource
     * @return the latest snapshot version
     * @throws ResourceException if resolving failed
     */
    public IVersion getLatestVersion(String name) throws ResourceException;

    /**
     * Loads the snapshot defined by the given resource.
     *
     * @param resource the resource definition
     * @return the resource snapshot
     * @throws ResourceException if loading failed
     */
    public T load(T resource) throws ResourceException;

    /**
     * Saves the specified resource snapshot to a file, database or other
     * backend. The save operation always creates a new version.
     *
     * @param resource the resource snapshot
     * @return the saved resource snapshot
     * @throws UnsupportedOperationException if saving is not supported
     * @throws ResourceException if saving failed
     */
    public T save(T resource) throws ResourceException;

    /**
     * Sets the resource reader and writer. Usually the default implementation
     * is used if no reader and writer has been set explicitly.
     *
     * @param readerWriter the resource reader and writer
     */
    public void setReaderWriter(IResourceReaderWriter readerWriter);
}
