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
import java.util.Properties;
import io.spikex.core.util.resource.ResourceChangeEvent.State;

/**
 *
 * @author cli
 */
public final class PropertiesResource extends AbstractResource<Properties> {

    public static final String FILE_SUFFIX = ".properties";

    private final Properties m_data;

    public String getProperty(
            final String key,
            final String defaultValue) {
        return m_data.getProperty(key, defaultValue);
    }

    public void setProperty(
            final String key,
            final String value) {
        m_data.setProperty(key, value);
        publishEvent(this, State.CHANGED);
    }

    @Override
    public Properties getData() {
        return new OrderedProperties(m_data);
    }

    @Override
    public boolean isEmpty() {
        return m_data.isEmpty();
    }

    @Override
    public boolean isSaveable() {
        return true;
    }

    @Override
    public PropertiesResource load() throws ResourceException {
        IResourceProvider<PropertiesResource> provider = getResourceProvider();
        PropertiesResource resource = provider.load(this);
        publishEvent(resource, State.LOADED);
        return resource;
    }

    @Override
    public PropertiesResource save() throws ResourceException {
        IResourceProvider<PropertiesResource> provider = getResourceProvider();
        PropertiesResource resource = provider.save(this);
        publishEvent(resource, State.SAVED);
        return resource;
    }

    public static Builder builder(final URI base) {
        return new Builder(base);
    }

    public static Builder builder(
            final URI base,
            final AbstractResource<Properties> resource) {
        return new Builder(base, resource);
    }

    public static final class Builder extends AbstractResource.Builder<Builder, PropertiesResource> {

        private Properties m_data;

        private Builder(final URI base) {
            super(new PropertiesResourceProvider(base));
            suffix(FILE_SUFFIX);
            m_data = new OrderedProperties();
        }

        private Builder(
                final URI base,
                final AbstractResource<Properties> resource) {
            super(new PropertiesResourceProvider(base), resource);
            m_data = resource.getData();
        }

        public final Builder data(final Properties data) {
            m_data = data;
            return this;
        }

        @Override
        public PropertiesResource build() {
            return new PropertiesResource(this);
        }
    }

    private PropertiesResource(final Builder builder) {
        super(builder);
        m_data = builder.m_data;
    }
}
