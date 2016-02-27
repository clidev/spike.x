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
package io.spikex.core.util.resource;

import java.net.URI;
import io.spikex.core.util.IVersion;
import io.spikex.core.util.Version;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author cli
 * @param <T>
 */
public abstract class AbstractResourceProvider<T extends IResource>
        implements IResourceProvider<T> {

    private final Class m_type;
    private final URI m_basePath;
    //
    private IResourceReaderWriter m_readerWriter;
    //
    private final Logger m_logger = LoggerFactory.getLogger(getClass());

    public AbstractResourceProvider(
            final Class type,
            final URI basePath,
            final IResourceReaderWriter readerWriter) {

        m_type = type;
        m_basePath = basePath;
        m_readerWriter = readerWriter;
    }

    @Override
    public Class getType() {
        return m_type;
    }

    @Override
    public URI getBasePath() {
        return m_basePath;
    }

    @Override
    public boolean exists(final T resource) {
        //
        // Check if resource exists
        //
        boolean found = true;
        try {
            IResourceReaderWriter readerWriter = getReaderWriter();
            readerWriter.findResource(getBasePath(), resource);
        } catch (ResourceException e) {
            found = false;
        }
        return found;
    }

    @Override
    public IVersion getLatestVersion(final String name) throws ResourceException {
        //
        // Read meta info and return latest version
        //
        IResourceReaderWriter readerWriter = getReaderWriter();
        IResource resource = PropertiesResource.builder(getBasePath())
                .name(name)
                .version(Version.none())
                .build();

        return readerWriter.readLatestVersion(getBasePath(), resource);
    }

    @Override
    public T load(final T resource) throws ResourceException {
        //
        // Resolve latest version of resource and load it
        //
        T snapshot = resource;
        IVersion version = resource.getVersion();
        if (version.isLatest()) {
            version = getLatestVersion(resource.getName());
            snapshot = applyVersion(resource, version);
        }
        //
        // Load snapshot and create the resource instance that represents the data
        //  
        IResourceReaderWriter readerWriter = getReaderWriter();
        URI location = readerWriter.findResource(getBasePath(), snapshot);
        snapshot = applyLocation(snapshot, location);
        logger().trace("Reading: {}", location);
        snapshot = (T) readerWriter.readSnapshot(getBasePath(), snapshot);
        logger().debug("Successfully read: {}", location);

        return snapshot;
    }

    @Override
    public T save(final T resource) throws ResourceException {
        //
        // Apply next version (if not NULL version)
        //
        T snapshot = resource;
        IVersion version = resource.getVersion();
        if (version.getSequence() >= 0) {
            IVersionStrategy strategy = resource.getVersionStrategy();
            version = strategy.nextVersion(version);
            snapshot = applyVersion(resource, version);
        }
        //
        // Save resource snapshot and return new version
        //
        IResourceReaderWriter readerWriter = getReaderWriter();
        URI location = readerWriter.writeSnapshot(getBasePath(), snapshot);
        snapshot = applyLocation(snapshot, location);
        //
        // Update meta file and apply next version (if not NULL version)
        //
        if (version.getSequence() >= 0) {
            readerWriter.writeLatestVersion(getBasePath(), snapshot);
        }
        logger().debug("Successfully wrote: {}", location);

        return snapshot;
    }

    @Override
    public final void setReaderWriter(final IResourceReaderWriter readerWriter) {
        m_readerWriter = readerWriter;
    }

    public final IResourceReaderWriter getReaderWriter() {
        return m_readerWriter;
    }

    protected abstract T applyVersion(T resource, IVersion version);

    protected abstract T applyLocation(T resource, URI location);

    /**
     * Returns the logger that is created in this abstract class using:      <code>
     * LoggerFactory.getLogger(getClass());
     * </code>
     *
     * @return the logger that was created by this abstract class
     */
    protected final Logger logger() {
        return m_logger;
    }
}
