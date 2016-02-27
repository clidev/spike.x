/**
 *
 * Copyright (c) 2012 OpenSolstice Team.
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
 * Defines the operations provided by a resource protocol handler. Protocol
 * handlers perform loading and saving of resource snapshots.
 *
 * @param <T>
 * @author cli
 */
public interface IResourceReaderWriter<T extends IResource> {

    public URI findResource(
            URI base,
            T resource) throws ResourceException;

    public IVersion readLatestVersion(
            URI base,
            T resource) throws ResourceException;

    public void writeLatestVersion(
            URI base,
            T resource) throws ResourceException;

    public T readSnapshot(
            URI base,
            T resource) throws ResourceException;

    public URI writeSnapshot(
            URI base,
            T resource) throws ResourceException;
}
