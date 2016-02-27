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
import java.util.List;
import java.util.Map;
import io.spikex.core.util.IVersion;
import java.util.ArrayList;
import org.yaml.snakeyaml.Yaml;

/**
 *
 * TODO Atomic writes...
 *
 * @author cli
 */
public class YamlResourceReaderWriter extends AbstractResourceReaderWriter<YamlResource> {

    @Override
    public YamlResource readSnapshot(
            final URI base,
            final YamlResource resource) throws ResourceException {

        YamlResource snapshot;
        URI location = resource.getLocation();
        Map<String, String> params = new HashMap();
        params.put(PARAM_CONN_DIRECTION, CONN_DOWNLOAD);
        try (InputStream in = createInputStream(location, params)) {

            // Load yaml documents and return as resource
            List<YamlDocument> docs = new ArrayList();
            Yaml yaml = new Yaml();
            for (Object data : yaml.loadAll(in)) {
                YamlDocument doc = new YamlDocument(resource, data);
                docs.add(doc);
            }
            snapshot = YamlResource.builder(base, resource)
                    .data(docs)
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
            final YamlResource resource) throws ResourceException {

        IVersion version = resource.getVersion();
        URI location = getResourceFileLocation(base, resource);
        getLogger().trace("Writing: {} version: {}",
                location, version.getSequence());

        Map<String, String> params = new HashMap();
        params.put(PARAM_CONN_DIRECTION, CONN_UPLOAD);
        try (OutputStream out = createOutputStream(location, params)) {

            // Create one big yaml document and save it
            StringBuilder data = new StringBuilder();
            List<YamlDocument> docs = resource.getData();
            for (YamlDocument doc : docs) {
                data.append(doc.asString());
                data.append(System.lineSeparator());
            }

            writeString(
                    data.toString(),
                    resource.getEncoding(),
                    out);

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
