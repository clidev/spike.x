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
package io.spikex.core.util.resource;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.yaml.snakeyaml.Yaml;

/**
 * Map based yaml document.
 * <p>
 * @author cli
 */
public final class YamlDocument {

    private final YamlResource m_resource;
    private final Object m_data;

    public YamlDocument(
            final YamlResource resource,
            final Object data) {

        m_resource = resource;
        m_data = data;
    }

    public YamlDocument getDocument(final String key) {

        Map value = getMap(key);
        if (value == null) {
            value = new HashMap();
        }

        return new YamlDocument(m_resource, value);
    }

    public <T extends Object> T getValue(final String key) {
        return (T) ((Map) m_data).get(key);
    }

    public <T extends Object> T getValue(
            final String key,
            final T defValue) {

        T value = (T) ((Map) m_data).get(key);
        if (value == null) {
            value = defValue;
        }

        return value;
    }

    public Map getMap(final String key) {
        return getValue(key, null);
    }

    public Map getMap(
            final String key,
            final Map defaultMap) {

        return getValue(key, defaultMap);
    }

    public Map getMap() {
        return (Map) m_data;
    }

    public List getList(final String key) {
        return getValue(key, null);
    }

    public List getList(
            final String key,
            final List defaultList) {

        return getValue(key, defaultList);
    }

    public <T extends Object> List<T> getList() {
        return (List<T>) m_data;
    }

    public boolean hasKey(final String key) {
        boolean found = false;
        Map m = getMap();
        if (m != null
                && m.containsKey(key)) {
            found = true;
        }
        return found;
    }

    public boolean hasValue(final String key) {
        boolean found = false;
        Map m = getMap();
        if (m != null
                && m.containsKey(key)
                && m.get(key) != null) {
            found = true;
        }
        return found;
    }

    public <T extends Object> void setValue(
            final String key,
            final T value) {

        ((Map) m_data).put(key, value);
        m_resource.fireChangedEvent();
    }

    public String asString() {
        Yaml yaml = new Yaml();
        return yaml.dump(m_data);
    }
}
