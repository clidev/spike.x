/**
 *
 * Copyright (c) 2013 Christoffer Lindevall.
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

import java.io.PrintStream;
import java.io.PrintWriter;
import java.util.Collection;
import java.util.Collections;
import java.util.Enumeration;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Set;

/**
 * Properties implementation that retains the order of the keys in insertion
 * order. The properties are stored in a <code>LinkedHashMap</code>. The
 * implementation is copied from the solution presented at:
 * <a
 * href="http://stackoverflow.com/questions/3619796/how-to-read-a-properties-file-in-java-in-the-original-order">
 * How to read a properties file in java in the original order
 * </a>
 *
 * @version $Revision$
 * @author cli
 * @since Solstice Common 1.0
 */
public class OrderedProperties extends Properties {

    private static final long serialVersionUID = -7032434592318855760L;
    private final Map<Object, Object> m_properties;

    public OrderedProperties() {
        m_properties = new LinkedHashMap();
    }

    public OrderedProperties(final Properties properties) {
        m_properties = new LinkedHashMap();
        Set<Entry<Object, Object>> entries = properties.entrySet();
        for (Entry<Object, Object> entry : entries) {
            m_properties.put(entry.getKey(), entry.getValue());
        }
    }

    @Override
    public void clear() {
        m_properties.clear();
    }

    @Override
    public boolean contains(final Object value) {
        return m_properties.containsValue(value);
    }

    @Override
    public boolean containsKey(final Object key) {
        return m_properties.containsKey(key);
    }

    @Override
    public boolean containsValue(final Object value) {
        return m_properties.containsValue(value);
    }

    @Override
    public Enumeration elements() {
        throw new RuntimeException("Method elements is not supported by "
                + "OrderedProperties class");
    }

    @Override
    public Set entrySet() {
        return m_properties.entrySet();
    }

    @Override
    public boolean equals(final Object o) {
        return m_properties.equals(o);
    }

    @Override
    public int hashCode() {
        int hash = 5;
        hash = 41 * hash + (this.m_properties != null ? this.m_properties.hashCode() : 0);
        return hash;
    }

    @Override
    public Object get(final Object key) {
        return m_properties.get(key);
    }

    @Override
    public String getProperty(final String key) {
        Object val = get(key); //here the class Properties uses super.get()
        if (val == null) {
            return null;
        }
        return (val instanceof String) ? (String) val : null; //behavior of standard properties
    }

    @Override
    public boolean isEmpty() {
        return m_properties.isEmpty();
    }

    @Override
    public Enumeration keys() {
        Set keys = m_properties.keySet();
        return Collections.enumeration(keys);
    }

    @Override
    public Set keySet() {
        return m_properties.keySet();
    }

    @Override
    public void list(PrintStream out) {
        this.list(new PrintWriter(out, true));
    }

    @Override
    public void list(PrintWriter out) {
        out.println("-- listing properties --");
        for (Map.Entry e : (Set<Map.Entry>) this.entrySet()) {
            String key = (String) e.getKey();
            String val = (String) e.getValue();
            if (val.length() > 40) {
                val = val.substring(0, 37) + "...";
            }
            out.println(key + "=" + val);
        }
    }

    @Override
    public Object put(Object key, Object value) {
        return m_properties.put(key, value);
    }

    @Override
    public int size() {
        return m_properties.size();
    }

    @Override
    public Collection values() {
        return m_properties.values();
    }

    @Override
    public Set<String> stringPropertyNames() {
        Set<String> names = new LinkedHashSet();
        Set<Object> keys = m_properties.keySet();
        for (Object key : keys) {
            names.add((String) key);
        }
        return names;
    }
}
