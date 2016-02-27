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

import java.util.List;

/**
 *
 * @param <E>
 * @author cli
 */
public final class RoundRobinStrategy<E extends IConnection> extends AbstractLoadBalancingStrategy<E> {

    private int m_index;

    @Override
    public void setConnections(final List<E> connections) {
        super.setConnections(connections);
        m_index = 0; // First connection
    }

    @Override
    public E next() {
        //
        // Simply pick the next from the list
        //
        List<E> connections = getConnections();
        int size = connections.size();
        return connections.get(m_index++ % size);
    }
}
