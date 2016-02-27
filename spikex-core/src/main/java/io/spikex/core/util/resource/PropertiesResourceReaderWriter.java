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

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import io.spikex.core.util.IVersion;

/**
 *
 * TODO Atomic writes...
 *
 * @author cli
 */
public class PropertiesResourceReaderWriter
        extends AbstractResourceReaderWriter<PropertiesResource> {

    @Override
    public PropertiesResource readSnapshot(
            final URI base,
            final PropertiesResource resource) throws ResourceException {

        PropertiesResource snapshot;
        URI location = resource.getLocation();
        Map<String, String> params = new HashMap();
        params.put(PARAM_CONN_DIRECTION, CONN_DOWNLOAD);
        try (InputStream in = createInputStream(location, params)) {

            // Load and apply properties to given resource
            Properties properties = new OrderedProperties();
            properties.load(in);
            snapshot = PropertiesResource.builder(base, resource)
                    .data(properties)
                    .build();

        } catch (IOException e) {
            throw new ResourceException(
                    ResourceException.ERR_READ_SNAPSHOT_FAILED,
                    "Failed to create or read input for location: " + location,
                    location,
                    e);
        }
        return snapshot;
    }

    @Override
    public URI writeSnapshot(
            final URI base,
            final PropertiesResource resource) throws ResourceException {

        IVersion version = resource.getVersion();
        URI location = getResourceFileLocation(base, resource);
        getLogger().trace("Writing: {} version: {}",
                location, version.getSequence());

        Map<String, String> params = new HashMap();
        params.put(PARAM_CONN_DIRECTION, CONN_UPLOAD);
        try (OutputStream out = createOutputStream(location, params)) {

            // Save properties
            Properties properties = resource.getData();
            properties.store(out, resource.getQualifiedName());

        } catch (IOException e) {
            throw new ResourceException(
                    ResourceException.ERR_WRITE_LATEST_VERSION_FAILED,
                    "Failed to create or write output for location: " + location,
                    location,
                    e);
        }
        return location;
    }
}
