/**
 *
 * Copyright (c) 2016 NG Modular Oy.
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
package io.spikex.filter.internal;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import org.vertx.java.core.buffer.Buffer;

/**
 *
 * @author cli
 */
public final class CollectdMeasurement {

    public static final String DSTYPE_GAUGE = "GAUGE";
    public static final String DSTYPE_COUNTER = "COUNTER";
    public static final String DSTYPE_ABSOLUTE = "ABSOLUTE";
    public static final String DSTYPE_DERIVE = "DERIVE";

    private String m_hostname;
    private String m_plugin;
    private String m_pluginInstance;
    private String m_type;
    private String m_typeInstance;
    private long m_timestamp;
    private long m_interval;

    private String[] m_dstypes;
    private Object[] m_values;
    private int m_index;

    public CollectdMeasurement() {
        m_hostname = "";
        m_plugin = "";
        m_pluginInstance = "";
        m_type = "";
        m_typeInstance = "";
        m_timestamp = 0L;
        m_interval = 0L;
    }

    public void clear() {
        m_dstypes = new String[]{"", "", "", "", "", "", "", ""};
        m_values = new Object[]{0.0d, 0.0d, 0.0d, 0.0d, 0.0d, 0.0d, 0.0d, 0.0d};
        m_index = 0;
    }

    public boolean hasValues() {
        return (m_index > 0);
    }

    public int getValueCount() {
        return m_index;
    }

    public String getDstype(final int index) {
        return m_dstypes[index];
    }

    public Object getValue(final int index) {
        return m_values[index];
    }

    public String getHostname() {
        return m_hostname;
    }

    public String getPlugin() {
        return m_plugin;
    }

    public String getPluginInstance() {
        return m_pluginInstance;
    }

    public String getType() {
        return m_type;
    }

    public String getTypeInstance() {
        return m_typeInstance;
    }

    public long getTimestamp() {
        return m_timestamp;
    }

    public long getInterval() {
        return m_interval;
    }

    public void setHostname(
            final Buffer buffer,
            final int pos,
            final int len) {

        m_hostname = buffer.getString(pos, len, StandardCharsets.US_ASCII.name());
    }

    public void setPlugin(
            final Buffer buffer,
            final int pos,
            final int len) {

        m_plugin = buffer.getString(pos, len, StandardCharsets.US_ASCII.name());
    }

    public void setPluginInstance(
            final Buffer buffer,
            final int pos,
            final int len) {

        m_pluginInstance = buffer.getString(pos, len, StandardCharsets.US_ASCII.name());
    }

    public void setType(
            final Buffer buffer,
            final int pos,
            final int len) {

        m_type = buffer.getString(pos, len, StandardCharsets.US_ASCII.name());
    }

    public void setTypeInstance(
            final Buffer buffer,
            final int pos,
            final int len) {

        m_typeInstance = buffer.getString(pos, len, StandardCharsets.US_ASCII.name());
    }

    public void setTimestamp(final long timestamp) {
        m_timestamp = timestamp;
    }

    public void setInterval(final long interval) {
        m_interval = interval;
    }

    public void pushValue(
            final String dstype,
            final Object value) {

        m_dstypes[m_index] = dstype;
        m_values[m_index] = resolveNumValue(dstype, value);
        m_index++;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder(getClass().getSimpleName());
        sb.append("{host: ");
        sb.append(getHostname());
        sb.append(" timestamp: ");
        sb.append(getTimestamp());
        sb.append(" plugin: ");
        sb.append(getPlugin());
        sb.append(" plugin-instance: ");
        sb.append(getPluginInstance());
        sb.append(" type: ");
        sb.append(getType());
        sb.append(" type-instance: ");
        sb.append(getTypeInstance());
        sb.append(" types: ");
        sb.append(Arrays.asList(m_dstypes));
        sb.append(" values: ");
        sb.append(Arrays.asList(m_values));
        sb.append(" interval: ");
        sb.append(getInterval());
        sb.append("}");
        return sb.toString();
    }

    public static Object resolveNumValue(
            final String dstype,
            final Object value) {

        Object numValue = value;

        if (value != null) {
            //
            // Sanity conversion
            //
            if (value instanceof Double
                    && (DSTYPE_COUNTER.equals(dstype)
                    || DSTYPE_ABSOLUTE.equals(dstype))) {

                numValue = ((Number) value).longValue();
            }
        } else {
            switch (dstype) {
                case DSTYPE_COUNTER:
                case DSTYPE_ABSOLUTE:
                    numValue = 0L;
                    break;
                default:
                    numValue = 0.0d;
                    break;
            }
        }

        return numValue;
    }
}
