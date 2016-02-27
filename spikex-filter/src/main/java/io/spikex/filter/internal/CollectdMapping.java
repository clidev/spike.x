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

import com.google.common.base.CharMatcher;
import static io.spikex.core.helper.Events.EVENT_FIELD_DSNAME;
import static io.spikex.core.helper.Events.EVENT_FIELD_DSTYPE;
import static io.spikex.core.helper.Events.EVENT_FIELD_INSTANCE;
import static io.spikex.core.helper.Events.EVENT_FIELD_SUBGROUP;
import io.spikex.core.util.StringReplace;
import static io.spikex.filter.internal.CollectdTypes.COLLECTD_KEY_PLUGIN;
import java.util.List;
import java.util.Map;
import org.vertx.java.core.json.JsonObject;
import org.yaml.snakeyaml.Yaml;

/**
 *
 * @author cli
 */
public final class CollectdMapping {

    private final String m_dsname;
    private final String m_dstype;
    private final String m_subgroup;
    private final String m_instance;
    private final String m_dsnameSeparator;
    private final String m_itemSeparator;
    private final String m_dsnamePattern;
    private final String m_itemPattern;
    private final char m_dsnameSepChar;
    private final char m_itemSepChar;

    public static final String SPIKEX_KEY_DSNAME = "dsname";
    public static final String SPIKEX_KEY_DSTYPE = "dstype";
    public static final String SPIKEX_KEY_SUBGROUP = "subgroup";
    public static final String SPIKEX_KEY_INSTANCE = "instance";

    public static final String DEF_MAPPING = "*";

    private static final String DSNAME_VALUE = "value";

    private static final String VAR_PLUGIN = "${plugin}";
    private static final String VAR_PLUGIN_INSTANCE = "${plugin_instance}";
    private static final String VAR_TYPE = "${type}";
    private static final String VAR_TYPE_INSTANCE = "${type_instance}";
    private static final String VAR_DSNAME = "${dsname}";
    private static final String VAR_DSTYPE = "${dstype}";
    private static final String VAR_SEP = "${sep}";

    private static final String CONF_DEF_MAPPINGS = "["
            + "{ plugin: 'cpu', dsname: 'system.cpu', subgroup: '${type_instance}', instance: '${plugin_instance}' },"
            + "{ plugin: 'memory', dsname: 'system.memory', subgroup: '${type_instance}' },"
            + "{ plugin: 'disk', dsname: 'system.disk', subgroup: '${type}${sep}${dsname}', instance: '${plugin_instance}' },"
            + "{ plugin: 'load', dsname: 'system.load', subgroup: '${dsname}', instance: '${plugin_instance}' },"
            + "{ plugin: 'swap', dsname: 'system.swap', subgroup: '${type}${sep}${type_instance}' },"
            + "{ plugin: 'df', dsname: 'filesystem', subgroup: '${type_instance}', instance: '${plugin_instance}' },"
            + "{ plugin: 'interface', dsname: 'system.interface', subgroup: '${type}${sep}${dsname}', instance: '${plugin_instance}' },"
            + "{ plugin: 'processes', dsname: 'processes', dstype: 'GAUGE', subgroup: '${type}${sep}${type_instance}${sep}${dsname}', instance: '${plugin_instance}' },"
            + "{ plugin: '*', dsname: '${plugin}${sep}${type}', subgroup: '${type_instance}', instance: '${plugin_instance}' }"
            + "]";

    public CollectdMapping(
            final String dsname,
            final String dstype,
            final String subgroup,
            final String instance,
            final String dsnameSeparator,
            final String itemSeparator) {

        m_dsname = dsname;
        m_dstype = dstype;
        m_subgroup = subgroup;
        m_instance = instance;
        m_dsnameSeparator = dsnameSeparator;
        m_dsnameSepChar = dsnameSeparator.charAt(0);
        m_itemSeparator = itemSeparator;
        m_itemSepChar = itemSeparator.charAt(0);

        // Pattern matcher and replacement
        StringBuilder sb = new StringBuilder();
        sb.append("[");
        sb.append(m_dsnameSeparator);
        sb.append("]{2,16}");
        m_dsnamePattern = sb.toString();

        sb = new StringBuilder();
        sb.append("[");
        sb.append(m_itemSeparator);
        sb.append("]{2,16}");
        m_itemPattern = sb.toString();
    }

    public static void buildDefaults(
            final Map<String, CollectdMapping> mappings,
            final String dsnameSeparator,
            final String itemSeparator) {

        List<Map> defMappings = new Yaml().loadAs(CONF_DEF_MAPPINGS, List.class);
        for (Map defMapping : defMappings) {

            String plugin = (String) defMapping.get(COLLECTD_KEY_PLUGIN);
            String dsname = (String) defMapping.get(SPIKEX_KEY_DSNAME);
            String dstype = (String) defMapping.get(SPIKEX_KEY_DSTYPE);
            String subgroup = (String) defMapping.get(SPIKEX_KEY_SUBGROUP);
            String instance = (String) defMapping.get(SPIKEX_KEY_INSTANCE);

            CollectdMapping mapping = new CollectdMapping(
                    dsname == null ? "" : dsname,
                    dstype == null ? "" : dstype,
                    subgroup == null ? "" : subgroup,
                    instance == null ? "" : instance,
                    dsnameSeparator,
                    itemSeparator);

            mappings.put(plugin, mapping);
        }
    }

    public void resolveAndSetFields(
            final JsonObject event,
            final String plugin,
            final String pluginInstance,
            final String type,
            final String typeInstance,
            final String cldDsname,
            final String cldDstype) {

        // plugin
        String dsname = m_dsname;
        dsname = StringReplace.replace(dsname, VAR_PLUGIN, plugin);

        String subgroup = m_subgroup;
        subgroup = StringReplace.replace(subgroup, VAR_PLUGIN, plugin);

        String instance = m_instance;
        instance = StringReplace.replace(instance, VAR_PLUGIN, plugin);

        // plugin_instance
        dsname = StringReplace.replace(dsname, VAR_PLUGIN_INSTANCE, pluginInstance);
        subgroup = StringReplace.replace(subgroup, VAR_PLUGIN_INSTANCE, pluginInstance);
        instance = StringReplace.replace(instance, VAR_PLUGIN_INSTANCE, pluginInstance);

        // type
        dsname = StringReplace.replace(dsname, VAR_TYPE, type);
        subgroup = StringReplace.replace(subgroup, VAR_TYPE, type);
        instance = StringReplace.replace(instance, VAR_TYPE, type);

        // type_instance
        dsname = StringReplace.replace(dsname, VAR_TYPE_INSTANCE, typeInstance);
        subgroup = StringReplace.replace(subgroup, VAR_TYPE_INSTANCE, typeInstance);
        instance = StringReplace.replace(instance, VAR_TYPE_INSTANCE, typeInstance);

        // dsname - ignore "value" dsname
        if (!DSNAME_VALUE.equals(cldDsname)) {
            dsname = StringReplace.replace(dsname, VAR_DSNAME, cldDsname);
            subgroup = StringReplace.replace(subgroup, VAR_DSNAME, cldDsname);
            instance = StringReplace.replace(instance, VAR_DSNAME, cldDsname);
        } else {
            // replace "value" with empty string
            dsname = StringReplace.replace(dsname, VAR_DSNAME, "");
            subgroup = StringReplace.replace(subgroup, VAR_DSNAME, "");
            instance = StringReplace.replace(instance, VAR_DSNAME, "");
        }

        // dstype
        String dstype = m_dstype;
        dsname = StringReplace.replace(dsname, VAR_DSTYPE, cldDstype);
        subgroup = StringReplace.replace(subgroup, VAR_DSTYPE, cldDstype);
        instance = StringReplace.replace(instance, VAR_DSTYPE, cldDstype);

        // Separators
        dsname = StringReplace.replace(dsname, VAR_SEP, m_dsnameSeparator);
        subgroup = StringReplace.replace(subgroup, VAR_SEP, m_dsnameSeparator);
        instance = StringReplace.replace(instance, VAR_SEP, m_dsnameSeparator);

        // Trim separators
        dsname = CharMatcher.is(m_dsnameSepChar).trimFrom(dsname)
                .replaceAll(m_dsnamePattern, "\\" + m_dsnameSeparator);
        subgroup = CharMatcher.is(m_itemSepChar).trimFrom(subgroup)
                .replaceAll(m_itemPattern, "\\" + m_itemSeparator);
        instance = CharMatcher.is(m_itemSepChar).trimFrom(instance)
                .replaceAll(m_itemPattern, "\\" + m_itemSeparator);

        event.putString(EVENT_FIELD_DSNAME, dsname.length() > 0 ? dsname : "-");
        event.putString(EVENT_FIELD_DSTYPE, dstype.length() > 0 ? dstype : cldDstype);
        event.putString(EVENT_FIELD_SUBGROUP, subgroup.length() > 0 ? subgroup : "-");
        event.putString(EVENT_FIELD_INSTANCE, instance.length() > 0 ? instance : "-");
    }
}
