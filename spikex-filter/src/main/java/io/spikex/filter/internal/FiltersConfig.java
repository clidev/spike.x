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
package io.spikex.filter.internal;

import com.eaio.uuid.UUID;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import io.spikex.core.AbstractConfig;
import static io.spikex.core.AbstractFilter.CONF_KEY_CHAIN_NAME;
import io.spikex.core.util.resource.YamlDocument;
import io.spikex.core.util.resource.YamlResource;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.vertx.java.core.json.JsonObject;

/**
 *
 * @author cli
 */
public final class FiltersConfig extends AbstractConfig {

    // Available filters
    private final List<FilterDef> m_filters;

    // Filter chains
    private final List<ChainDef> m_chains;

    private static final String CONF_KEY_ALIAS = "alias";
    private static final String CONF_KEY_CHAIN = "chain";
    private static final String CONF_KEY_CHAINS = "chains";
    private static final String CONF_KEY_CONFIG = "config";
    private static final String CONF_KEY_FILTER = "filter";
    private static final String CONF_KEY_FILTERS = "filters";
    private static final String CONF_KEY_INSTANCES = "instances";
    private static final String CONF_KEY_MODULE = "module";
    private static final String CONF_KEY_MODULES = "modules";
    private static final String CONF_KEY_MULTI_THREADED = "multi-threaded";
    private static final String CONF_KEY_VERTICLE = "verticle";
    private static final String CONF_KEY_WORKER = "worker";

    private static final String ALIAS_INPUT_ADDRESS = "%InputAddress";
    private static final String ALIAS_OUTPUT_ADDRESS = "%OutputAddress";

    public FiltersConfig(
            final String filename,
            final Path path) {

        super(filename, path);
        m_filters = new ArrayList();
        m_chains = new ArrayList();
    }

    public List<FilterDef> getFilters() {
        return m_filters;
    }

    public List<ChainDef> getChains() {
        return m_chains;
    }

    public void logInputOutputDef() {

        for (ChainDef chain : getChains()) {

            logger().debug("Input/output of chain: {}", chain.getName());
            for (FilterDef filter : chain.getFilters()) {

                String alias = filter.getAlias();
                String inputAddress = filter.getInputAddress();
                String outputAddress = filter.getOutputAddress();

                if (Strings.isNullOrEmpty(outputAddress)) {
                    logger().debug("{} -> [{}]",
                            inputAddress,
                            alias);
                } else if (Strings.isNullOrEmpty(inputAddress)) {
                    logger().debug("[{}] -> {}",
                            alias,
                            outputAddress);
                } else if (Strings.isNullOrEmpty(inputAddress)
                        && Strings.isNullOrEmpty(outputAddress)) {
                    logger().debug("[{}]", alias);
                } else {
                    logger().debug("{} -> [{}] -> {}",
                            inputAddress,
                            alias,
                            outputAddress);
                }
            }
        }
    }

    @Override
    protected void build(final YamlResource resource) {
        buildFilters(resource);
        buildChains(resource);
    }

    private void buildFilters(final YamlResource resource) {

        m_filters.clear();
        List<YamlDocument> documents = resource.getData();
        if (documents != null
                && !documents.isEmpty()) {

            YamlDocument conf = documents.get(0);
            List<Map> modules = conf.getList(CONF_KEY_MODULES, new ArrayList());

            for (Map module : modules) {
                String groupArtifactId = (String) module.get(CONF_KEY_MODULE);
                Preconditions.checkNotNull(groupArtifactId, "Missing module");
                logger().debug("Module: {}", groupArtifactId);
                List<Map> filters = (List) module.get(CONF_KEY_FILTERS);

                for (Map<String, String> filter : filters) {
                    String alias = filter.get(CONF_KEY_ALIAS);
                    String verticle = filter.get(CONF_KEY_VERTICLE);

                    //
                    // Sanity checks
                    //
                    Preconditions.checkNotNull(alias, "Missing alias");
                    Preconditions.checkNotNull(verticle, "Missing verticle");

                    logger().debug("Filter: {} verticle: {}", alias, verticle);
                    m_filters.add(new FilterDef(
                            groupArtifactId,
                            alias,
                            verticle
                    ));
                }
            }
        }
    }

    private void buildChains(final YamlResource resource) {

        m_chains.clear();
        List<YamlDocument> documents = resource.getData();
        if (documents != null
                && !documents.isEmpty()) {

            YamlDocument conf = documents.get(0);
            List<Map> chains = conf.getList(CONF_KEY_CHAINS, new ArrayList());
            for (Map chainMap : chains) {

                String name = (String) chainMap.get(CONF_KEY_CHAIN);
                Preconditions.checkNotNull(name, "Missing chain");
                logger().info("Found chain: {}", name);
                ChainDef chain = new ChainDef(name);
                String chainInputAddress = "";
                String chainOutputAddress = "";

                {
                    List<Map> filters = (List) chainMap.get(CONF_KEY_FILTERS);
                    for (Map filterMap : filters) {
                        String alias = (String) filterMap.get(CONF_KEY_FILTER);
                        logger().info("Filter map: {}", filterMap);

                        if (filterMap.containsKey(ALIAS_INPUT_ADDRESS)) {
                            alias = ALIAS_INPUT_ADDRESS;
                        }

                        if (filterMap.containsKey(ALIAS_OUTPUT_ADDRESS)) {
                            alias = ALIAS_OUTPUT_ADDRESS;
                        }

                        Preconditions.checkNotNull(alias, "Missing alias");

                        //
                        // Handle filter configurations
                        //
                        switch (alias) {

                            case ALIAS_INPUT_ADDRESS:
                                chainInputAddress = (String) filterMap.get(ALIAS_INPUT_ADDRESS);
                                break;

                            case ALIAS_OUTPUT_ADDRESS:
                                chainOutputAddress = (String) filterMap.get(ALIAS_OUTPUT_ADDRESS);
                                break;

                            default:
                                FilterDef filter = findFilterCopy(name, alias);
                                Preconditions.checkNotNull(filter,
                                        "Could not find matching filter definition: " + alias);

                                Map config = (Map) filterMap.get(CONF_KEY_CONFIG);
                                config.put(CONF_KEY_CHAIN_NAME, name);
                                filter.setConfig(config);

                                //
                                // worker, multi-threaded and instances
                                //
                                if (filterMap.containsKey(CONF_KEY_WORKER)) {
                                    filter.setWorker((Boolean) filterMap.get(CONF_KEY_WORKER));
                                }
                                if (filterMap.containsKey(CONF_KEY_MULTI_THREADED)) {
                                    filter.setMultiThreaded((Boolean) filterMap.get(CONF_KEY_MULTI_THREADED));
                                }
                                if (filterMap.containsKey(CONF_KEY_INSTANCES)) {
                                    filter.setInstances((Integer) filterMap.get(CONF_KEY_INSTANCES));
                                }

                                chain.addFilter(filter);
                                break;
                        }
                    }
                }
                {
                    //
                    // Set the output address of each filter
                    //
                    String outputAddress = "";
                    List<FilterDef> filters = chain.getFilters();
                    for (FilterDef filter : Lists.reverse(filters)) {
                        if (!Strings.isNullOrEmpty(outputAddress)) {
                            filter.setOutputAddress(outputAddress);
                        }
                        outputAddress = filter.getInputAddress();
                    }
                }
                //
                // Redirect input/output
                //
                if (!Strings.isNullOrEmpty(chainInputAddress)) {
                    chain.setInputAddress(chainInputAddress);
                }
                if (!Strings.isNullOrEmpty(chainOutputAddress)) {
                    chain.setOutputAddress(chainOutputAddress);
                }

                m_chains.add(chain);
            }
        }
    }

    private FilterDef findFilterCopy(
            final String name,
            final String alias) {

        FilterDef filter = null;
        for (FilterDef tmpFilter : m_filters) {
            if (alias.equals(tmpFilter.getAlias())) {
                filter = tmpFilter.copy(name);
            }
        }
        return filter;
    }

    public static final class ChainDef {

        private final String m_name;
        private final List<FilterDef> m_filters;

        private ChainDef(final String name) {
            m_name = name;
            m_filters = new ArrayList();
        }

        public String getName() {
            return m_name;
        }

        public List<FilterDef> getFilters() {
            return m_filters;
        }

        public void addFilter(final FilterDef filter) {
            m_filters.add(filter);
        }

        public void clear() {
            m_filters.clear();
        }

        public void setInputAddress(final String address) {
            //
            // Change the input address of the first filter
            //
            if (m_filters.size() > 0) {
                FilterDef filter = m_filters.get(0);
                filter.setInputAddress(address);
            }
        }

        public void setOutputAddress(final String address) {
            //
            // Change the output address of the last filter
            //
            if (m_filters.size() > 0) {
                int lastIndex = m_filters.size() - 1;
                FilterDef filter = m_filters.get(lastIndex);
                filter.setOutputAddress(address);
            }
        }
    }

    public static final class FilterDef {

        private final String m_id;
        private final String m_module;
        private final String m_chain;
        private final String m_alias;
        private final String m_verticle;
        private Map m_config;
        private String m_inputAddress;
        private String m_outputAddress;
        private String m_deploymentId;
        private int m_instances;
        private boolean m_worker;
        private boolean m_multiThreaded;

        private FilterDef(final String alias) {
            this(alias, alias, alias, alias);
        }

        private FilterDef(
                final String module,
                final String alias,
                final String verticle) {
            this(module, "", alias, verticle);
        }

        private FilterDef(
                final String module,
                final String chain,
                final String alias,
                final String verticle) {

            m_id = new UUID().toString();
            m_module = module;
            m_chain = chain;
            m_alias = alias;
            m_verticle = verticle;

            StringBuilder addr = new StringBuilder(chain.toLowerCase());
            addr.append(".");
            addr.append(alias.toLowerCase());

            m_inputAddress = addr.toString();
            m_outputAddress = ""; // No output messages by default
            m_deploymentId = "";
            m_instances = 1;
            m_worker = false;
            m_multiThreaded = false;
        }

        public FilterDef copy(final String chain) {
            FilterDef copy = new FilterDef(m_module, chain, m_alias, m_verticle);
            copy.setConfig(m_config);
            return copy;
        }

        public boolean isWorker() {
            return m_worker;
        }

        public boolean isMultiThreaded() {
            return m_multiThreaded;
        }

        public boolean isDeployed() {
            return !Strings.isNullOrEmpty(m_deploymentId);
        }

        public String getId() {
            return m_id;
        }

        public String getDeploymentId() {
            return m_deploymentId;
        }

        public String getChain() {
            return m_chain;
        }

        public String getAlias() {
            return m_alias;
        }

        public String getModule() {
            return m_module;
        }

        public String getVerticle() {
            return m_verticle;
        }

        public Map getConfig() {
            return m_config;
        }

        public JsonObject getJsonConfig() {
            return new JsonObject(m_config);
        }

        public String getInputAddress() {
            return m_inputAddress;
        }

        public String getOutputAddress() {
            return m_outputAddress;
        }

        public int getInstances() {
            return m_instances;
        }

        public void setConfig(final Map config) {
            m_config = config;
        }

        public void setInputAddress(final String address) {
            m_inputAddress = address;
        }

        public void setOutputAddress(final String address) {
            m_outputAddress = address;
        }

        public void setDeploymentId(final String deploymentId) {
            m_deploymentId = deploymentId;
        }

        public void setWorker(final boolean worker) {
            m_worker = worker;
        }

        public void setMultiThreaded(final boolean multiThreaded) {
            m_multiThreaded = multiThreaded;
        }

        public void setInstances(final int instances) {
            m_instances = instances;
        }
    }
}
