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
import io.spikex.core.util.resource.ResourceChangeEvent.State;

/**
 *
 * @author cli
 */
public final class TextResource extends AbstractResource<String> {

    public static final String FILE_SUFFIX = ".txt";

    private final String m_data;

    @Override
    public String getData() {
        return m_data;
    }

    @Override
    public boolean isEmpty() {
        return (m_data.length() > 0);
    }

    @Override
    public boolean isSaveable() {
        return true;
    }

    @Override
    public TextResource load() throws ResourceException {
        IResourceProvider<TextResource> provider = getResourceProvider();
        TextResource resource = provider.load(this);
        publishEvent(resource, State.LOADED);
        return resource;
    }

    @Override
    public TextResource save() throws ResourceException {
        IResourceProvider<TextResource> provider = getResourceProvider();
        TextResource resource = provider.save(this);
        publishEvent(resource, State.SAVED);
        return resource;
    }

    public static Builder builder(final URI base) {
        return new Builder(base);
    }

    public static Builder builder(
            final URI base,
            final AbstractResource<String> resource) {
        return new Builder(base, resource);
    }

    public static final class Builder extends AbstractResource.Builder<Builder, TextResource> {

        private String m_data;

        private Builder(final URI base) {
            super(new TextResourceProvider(base));
            suffix(FILE_SUFFIX);
            m_data = "";
        }

        private Builder(
                final URI base,
                final AbstractResource<String> resource) {
            super(new TextResourceProvider(base), resource);
            m_data = resource.getData();
        }

        public final Builder data(final String data) {
            m_data = data;
            return this;
        }

        @Override
        public TextResource build() {
            return new TextResource(this);
        }
    }

    private TextResource(final Builder builder) {
        super(builder);
        m_data = builder.m_data;
    }
}
