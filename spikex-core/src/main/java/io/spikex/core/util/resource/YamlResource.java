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
import java.util.List;
import io.spikex.core.util.resource.ResourceChangeEvent.State;
import java.util.ArrayList;

/**
 *
 * @author cli
 */
public final class YamlResource extends AbstractResource<List<YamlDocument>> {

    public static final String FILE_SUFFIX = ".yaml";

    private final List<YamlDocument> m_data;

    @Override
    public List<YamlDocument> getData() {
        return m_data;
    }

    @Override
    public boolean isSaveable() {
        return true;
    }

    @Override
    public YamlResource load() throws ResourceException {
        IResourceProvider<YamlResource> provider = getResourceProvider();
        YamlResource resource = provider.load(this);
        publishEvent(resource, State.LOADED);
        return resource;
    }

    @Override
    public YamlResource save() throws ResourceException {
        IResourceProvider<YamlResource> provider = getResourceProvider();
        YamlResource resource = provider.save(this);
        publishEvent(resource, State.SAVED);
        return resource;
    }

    @Override
    public boolean isEmpty() {
        return m_data.isEmpty();
    }

    void fireChangedEvent() {
        publishEvent(this, State.CHANGED);
    }

    public static Builder builder(final URI base) {
        return new Builder(base);
    }

    public static Builder builder(
            final URI base,
            final AbstractResource<List<YamlDocument>> resource) {
        return new Builder(base, resource);
    }

    public static final class Builder extends AbstractResource.Builder<Builder, YamlResource> {

        private List<YamlDocument> m_data;

        private Builder(final URI base) {
            super(new YamlResourceProvider(base));
            suffix(FILE_SUFFIX);
            m_data = new ArrayList();
        }

        private Builder(
                final URI base,
                final AbstractResource<List<YamlDocument>> resource) {
            super(new YamlResourceProvider(base), resource);
            m_data = resource.getData();
        }

        public final Builder data(final List<YamlDocument> data) {
            m_data = data;
            return this;
        }

        @Override
        public YamlResource build() {
            return new YamlResource(this);
        }
    }

    private YamlResource(final Builder builder) {
        super(builder);
        m_data = builder.m_data;
    }
}
