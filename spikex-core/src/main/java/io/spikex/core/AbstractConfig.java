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
package io.spikex.core;

import io.spikex.core.util.Version;
import io.spikex.core.util.resource.ResourceException;
import io.spikex.core.util.resource.YamlResource;
import java.io.IOException;
import java.nio.file.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author cli
 */
public abstract class AbstractConfig {

    private final String m_name;
    private final Path m_path;

    // Configuration file hash
    private int m_confHash;

    // Configuration Yaml resource (root)
    private YamlResource m_resource;

    private final Logger m_logger = LoggerFactory.getLogger(getClass());

    public AbstractConfig(
            final String name,
            final Path path) {

        m_name = name;
        m_path = path;
        m_confHash = 0;
    }

    public boolean isEmpty() {
        boolean empty = true;
        if (m_resource != null) {
            empty = m_resource.isEmpty();
        }
        return empty;
    }

    public final String getName() {
        return m_name;
    }

    public final Path getPath() {
        return m_path;
    }

    public final YamlResource getYaml() {
        return m_resource;
    }

    public final boolean hasChanged() {
        boolean changed = false;
        int hash = calculateHash();
        if (hash != m_confHash) {
            m_confHash = hash;
            changed = true;
        }
        return changed;
    }

    public final AbstractConfig load() throws ResourceException {
        m_resource = resource().load();
        build(m_resource);
        return this;
    }

    protected abstract void build(YamlResource resource);

    /**
     * Returns the standard logger that is used by this config instance.
     *
     * @return the standard logger
     */
    protected final Logger logger() {
        return m_logger;
    }

    protected final YamlResource resource() throws ResourceException {
        YamlResource resource = m_resource;
        if (resource == null) {
            resource = YamlResource.builder(m_path.toUri())
                    .name(m_name)
                    .version(Version.none())
                    .build();
            m_resource = resource;
        }
        return resource;
    }

    private int calculateHash() {

        int hash = 0;

        try {
            // Calculate hash
            YamlResource resource = resource();
            Path confFile = m_path.resolve(resource.getQualifiedName());
            hash = io.spikex.core.util.Files.hashOfFile(confFile);
        } catch (IOException e) {
            m_logger.error("Failed to calculate hash for: {}", m_path, e);
        }

        return hash;
    }
}
