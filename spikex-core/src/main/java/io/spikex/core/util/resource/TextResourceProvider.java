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
 *
 * @author cli
 */
public class TextResourceProvider extends AbstractResourceProvider<TextResource> {

    public TextResourceProvider(final URI basePath) {
        super(
                TextResource.class,
                basePath,
                new TextResourceReaderWriter());
    }

    @Override
    protected TextResource applyVersion(
            final TextResource resource,
            final IVersion version) {
      
        return TextResource.builder(getBasePath(), resource)
                .version(version)
                .build();
    }

    @Override
    protected TextResource applyLocation(
            final TextResource resource,
            final URI location) {

        return TextResource.builder(getBasePath(), resource)
                .location(location)
                .build();
    }
}
