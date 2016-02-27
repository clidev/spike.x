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
package io.spikex.core.util.connection;

import java.net.URI;
import org.vertx.java.core.Handler;

/**
 * Represents a client connection to a remote service.
 *
 * @param <V>
 *
 * @author cli
 */
public interface IConnection<V> {

    public boolean isConnected();

    public URI getAddress();

    public long getConnectTimeout();

    public long getLastActivity();

    /*
     * Can be null
     */
    public V getClient();

    public void doRequest(Handler handler);

    public void disconnect();

    public void setConnected(boolean connected);

    public void updateActivity();
}
