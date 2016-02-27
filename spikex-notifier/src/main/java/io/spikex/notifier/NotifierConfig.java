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
package io.spikex.notifier;

import com.google.common.base.Preconditions;
import io.spikex.core.AbstractConfig;
import io.spikex.core.util.CronEntry;
import io.spikex.core.util.resource.YamlDocument;
import io.spikex.core.util.resource.YamlResource;
import io.spikex.notifier.internal.Rule;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 *
 * @author cli
 */
public final class NotifierConfig extends AbstractConfig {

    private final List<Rule> m_rules;
    private final Map<String, TemplateDef> m_templates;
    private final Map<String, DestinationDef> m_destinations;
    private final Map<String, CronEntry> m_schedules;

    private String m_localAddress;
    private long m_dispatcherInterval;
    private int m_dispatcherBatchSize;
    private boolean m_mailHtmlEscape;
    private boolean m_fileHtmlUnescape;
    private DatabaseDef m_databaseDef;
    private EmailDef m_emailDef;
    private FlowdockDef m_flowdockDef;

    //
    // Configuration fields
    //
    private static final String CONF_KEY_LOCAL_ADDRESS = "local-address";
    private static final String CONF_KEY_DISPATCHER_INTERVAL = "dispatcher-interval";
    private static final String CONF_KEY_DISPATCHER_BATCH_SIZE = "dispatcher-batch-size";
    private static final String CONF_KEY_DISPATCHER_MAIL_HTML_ESCAPE = "dispatcher-mail-html-escape";
    private static final String CONF_KEY_DISPATCHER_FILE_HTML_UNESCAPE = "dispatcher-file-html-unescape";
    private static final String CONF_KEY_TEMPLATES = "templates";
    private static final String CONF_KEY_NAME = "name";
    private static final String CONF_KEY_URL = "url";
    private static final String CONF_KEY_LOCALES = "locales";
    private static final String CONF_KEY_SCHEDULES = "schedules";
    private static final String CONF_KEY_DESTINATIONS = "destinations";
    private static final String CONF_KEY_ADDRESSES = "addresses";
    private static final String CONF_KEY_CRONLINE = "cronline";
    private static final String CONF_KEY_RULES = "rules";
    private static final String CONF_KEY_PASSWORD = "password";
    private static final String CONF_KEY_COMPACT = "compact-on-startup";
    private static final String CONF_KEY_DATABASE = "database";
    private static final String CONF_KEY_EMAIL = "email";
    private static final String CONF_KEY_FLOWDOCK = "flowdock";

    //
    // Email configuration fields
    //
    private static final String CONFIG_FIELD_SMTP_HOST = "smtp-host";
    private static final String CONFIG_FIELD_SMTP_PORT = "smtp-port";
    private static final String CONFIG_FIELD_SMTP_USER = "smtp-user";
    private static final String CONFIG_FIELD_SMTP_PASSWORD = "smtp-password";
    private static final String CONFIG_FIELD_SMTP_TLS = "smtp-tls";
    private static final String CONFIG_FIELD_SMTP_SSL = "smtp-ssl";
    private static final String CONFIG_FIELD_HEADERS = "headers";
    private static final String CONFIG_FIELD_IMAGES = "images";
    private static final String CONFIG_FIELD_FROM = "from";
    private static final String CONFIG_FIELD_RETRY_COUNT = "retry-count";
    private static final String CONFIG_FIELD_RETRY_WAIT_SEC = "retry-wait-sec";
    private static final String CONFIG_FIELD_USE_PRIORITIES = "use-priorities";

    //
    // Flowdock configuration fields
    //
    private static final String CONFIG_FIELD_API_TOKEN = "api-token";

    //
    // Default configuration values
    //
    private static final String DEF_DB_NAME = "notifier.db";
    private static final String DEF_DB_PASSWORD = "J01Ak9iW35+00D";
    private static final Boolean DEF_DB_COMPACT = Boolean.TRUE;
    private static final String DEF_LOCAL_ADDRESS = "spikex.notifier";
    private static final long DEF_DISPATCHER_INTERVAL = 1000L;
    private static final int DEF_DISPATCHER_BATCH_SIZE = 10;
    private static final boolean DEF_DISPATCHER_MAIL_HTML_ESCAPE = true;
    private static final boolean DEF_DISPATCHER_FILE_HTML_UNESCAPE = true;

    private static final String QUEUE_NAME = "notifier-queue";
    private static final String CONF_NAME = "notifier";

    public NotifierConfig(final Path path) {

        super(CONF_NAME, path);
        m_rules = new ArrayList();
        m_templates = new HashMap();
        m_destinations = new HashMap();
        m_schedules = new HashMap();
        m_dispatcherInterval = -1L;
        m_dispatcherBatchSize = -1;
        m_mailHtmlEscape = false;
        m_fileHtmlUnescape = false;
    }

    public static String queueName() {
        return QUEUE_NAME;
    }

    public String getLocalAddress() {
        return m_localAddress;
    }

    public long getDispatcherInterval() {
        return m_dispatcherInterval;
    }

    public int getDispatcherBatchSize() {
        return m_dispatcherBatchSize;
    }

    public boolean getMailHtmlEscape() {
        return m_mailHtmlEscape;
    }

    public boolean getFileHtmlUnescape() {
        return m_fileHtmlUnescape;
    }

    public List<Rule> getRules() {
        return m_rules;
    }

    public Map<String, TemplateDef> getTemplates() {
        return m_templates;
    }

    public Map<String, DestinationDef> getDestinations() {
        return m_destinations;
    }

    public Map<String, CronEntry> getSchedules() {
        return m_schedules;
    }

    public DatabaseDef getDatabaseDef() {
        return m_databaseDef;
    }

    public EmailDef getEmailDef() {
        return m_emailDef;
    }

    public FlowdockDef getFlowdockDef() {
        return m_flowdockDef;
    }

    @Override
    protected void build(final YamlResource resource) {

        List<YamlDocument> documents = resource.getData();
        if (documents != null
                && !documents.isEmpty()) {

            YamlDocument conf = documents.get(0);
            m_localAddress = conf.getValue(CONF_KEY_LOCAL_ADDRESS, DEF_LOCAL_ADDRESS);

            // Circumvent problem with YamlDocument and support for longs
            int interval = conf.getValue(CONF_KEY_DISPATCHER_INTERVAL,
                    (int) DEF_DISPATCHER_INTERVAL);
            m_dispatcherInterval = interval;

            m_dispatcherBatchSize = conf.getValue(CONF_KEY_DISPATCHER_BATCH_SIZE,
                    DEF_DISPATCHER_BATCH_SIZE);

            m_mailHtmlEscape = conf.getValue(CONF_KEY_DISPATCHER_MAIL_HTML_ESCAPE,
                    DEF_DISPATCHER_MAIL_HTML_ESCAPE);

            m_fileHtmlUnescape = conf.getValue(CONF_KEY_DISPATCHER_FILE_HTML_UNESCAPE,
                    DEF_DISPATCHER_FILE_HTML_UNESCAPE);

            buildTemplates(resource);
            buildDestinations(resource);
            buildSchedules(resource);
            buildRules(resource);

            buildDatabaseDef(resource);
            buildEmailDef(resource);
        }
    }

    private void buildRules(final YamlResource resource) {

        m_rules.clear();
        YamlDocument conf = resource.getData().get(0);
        List<Map> rules = conf.getList(CONF_KEY_RULES, new ArrayList());

        for (Map rule : rules) {
            String name = (String) rule.get(CONF_KEY_NAME);
            Preconditions.checkNotNull(name, "Missing rule name");
            logger().debug("Rule: {}", name);
            m_rules.add(Rule.create(name, new YamlDocument(resource, rule)));
        }
    }

    private void buildTemplates(final YamlResource resource) {

        m_templates.clear();
        YamlDocument conf = resource.getData().get(0);
        List<Map> templates = conf.getList(CONF_KEY_TEMPLATES, new ArrayList());

        for (Map template : templates) {
            String name = (String) template.get(CONF_KEY_NAME);
            String url = (String) template.get(CONF_KEY_URL);
            List<String> locales = (List) template.get(CONF_KEY_LOCALES);

            Preconditions.checkNotNull(name, "Missing template name");
            Preconditions.checkNotNull(url, "Missing url for template: " + name);

            logger().debug("Template: {}", name);
            m_templates.put(name,
                    new TemplateDef(
                            name,
                            url,
                            (locales != null ? locales : new ArrayList())));
        }
    }

    private void buildDestinations(final YamlResource resource) {

        m_destinations.clear();
        YamlDocument conf = resource.getData().get(0);
        List<Map> destinations = conf.getList(CONF_KEY_DESTINATIONS, new ArrayList());

        for (Map destination : destinations) {
            String name = (String) destination.get(CONF_KEY_NAME);
            List<String> addresses = (List) destination.get(CONF_KEY_ADDRESSES);

            Preconditions.checkNotNull(name, "Missing destination name");
            Preconditions.checkState(addresses != null && addresses.size() > 0,
                    "Missing addresses for destination: " + name);

            logger().debug("Destination: {}", name);
            m_destinations.put(name,
                    new DestinationDef(
                            name,
                            addresses));
        }
    }

    private void buildSchedules(final YamlResource resource) {

        m_schedules.clear();
        YamlDocument conf = resource.getData().get(0);
        List<Map> schedules = conf.getList(CONF_KEY_SCHEDULES, new ArrayList());

        for (Map schedule : schedules) {
            String name = (String) schedule.get(CONF_KEY_NAME);
            String cronline = (String) schedule.get(CONF_KEY_CRONLINE);

            Preconditions.checkNotNull(name, "Missing schedule name");
            Preconditions.checkNotNull(cronline, "Missing cronline for schedule: " + name);

            logger().debug("Schedule: {} cronline: {}", name, cronline);
            m_schedules.put(name, CronEntry.create(cronline));
        }
    }

    private void buildDatabaseDef(final YamlResource resource) {
        YamlDocument conf = resource.getData().get(0);
        String name = DEF_DB_NAME;
        String password = DEF_DB_PASSWORD;
        boolean compact = DEF_DB_COMPACT;
        if (conf.hasKey(CONF_KEY_DATABASE)) {
            YamlDocument dbConf = conf.getDocument(CONF_KEY_DATABASE);
            name = dbConf.getValue(CONF_KEY_NAME, name);
            password = dbConf.getValue(CONF_KEY_PASSWORD, password);
            compact = dbConf.getValue(CONF_KEY_COMPACT, compact);
        }
        m_databaseDef = new DatabaseDef(name, password, compact);
    }

    private void buildEmailDef(final YamlResource resource) {
        YamlDocument conf = resource.getData().get(0);
        m_emailDef = new EmailDef(conf.getDocument(CONF_KEY_EMAIL));
        m_flowdockDef = new FlowdockDef(conf.getDocument(CONF_KEY_FLOWDOCK));
    }

    public static final class DatabaseDef {

        private final String m_name;
        private final String m_password;
        private final boolean m_compact;

        private DatabaseDef(
                final String name,
                final String password,
                final boolean compact) {

            m_name = name;
            m_password = password;
            m_compact = compact;
        }

        public String getName() {
            return m_name;
        }

        public String getPassword() {
            return m_password;
        }

        public boolean getCompact() {
            return m_compact;
        }
    }

    public static final class TemplateDef {

        private final String m_name;
        private final String m_url;
        private final List<String> m_locales;

        private TemplateDef(
                final String name,
                final String url,
                final List<String> locales) {

            m_name = name;
            m_url = url;
            m_locales = locales;
        }

        public String getName() {
            return m_name;
        }

        public String getUrl() {
            return m_url;
        }

        public List<String> getLocales() {
            return m_locales;
        }
    }

    public static final class DestinationDef {

        private final String m_name;
        private final List<String> m_addresses;

        private DestinationDef(
                final String name,
                final List<String> addresses) {

            m_name = name;
            m_addresses = addresses;
        }

        public String getName() {
            return m_name;
        }

        public List<String> geAddresses() {
            return m_addresses;
        }
    }

    public static final class EmailDef {

        private final YamlDocument m_config;

        private EmailDef(final YamlDocument config) {
            m_config = config;
        }

        public boolean isTlsEnabled() {
            return m_config.getValue(CONFIG_FIELD_SMTP_TLS, false);
        }

        public boolean isSslEnabled() {
            return m_config.getValue(CONFIG_FIELD_SMTP_SSL, false);
        }

        public boolean hasSmtpUser() {
            return m_config.hasValue(CONFIG_FIELD_SMTP_USER);
        }

        public boolean hasSmtpPassword() {
            return m_config.hasValue(CONFIG_FIELD_SMTP_PASSWORD);
        }

        public String getFrom() {
            return m_config.getValue(CONFIG_FIELD_FROM, "");
        }

        public String getSmtpHost() {
            return m_config.getValue(CONFIG_FIELD_SMTP_HOST, "localhost");
        }

        public int getSmtpPort() {
            return m_config.getValue(CONFIG_FIELD_SMTP_PORT, 25);
        }

        public String getSmtpUser() {
            return m_config.getValue(CONFIG_FIELD_SMTP_USER);
        }

        public String getSmtpPassword() {
            return m_config.getValue(CONFIG_FIELD_SMTP_PASSWORD);
        }

        public boolean getUsePriorities() {
            return m_config.getValue(CONFIG_FIELD_USE_PRIORITIES, true);
        }

        public int getRetryCount() {
            return m_config.getValue(CONFIG_FIELD_RETRY_COUNT, 0);
        }

        public int getRetryWaitSec() {
            return m_config.getValue(CONFIG_FIELD_RETRY_WAIT_SEC, 30);
        }

        public Map getHeaders() {
            return m_config.getMap(CONFIG_FIELD_HEADERS, new HashMap());
        }

        public Map getImages() {
            return m_config.getMap(CONFIG_FIELD_IMAGES, new HashMap());
        }
    }

    public static final class FlowdockDef {

        private final YamlDocument m_config;

        private FlowdockDef(final YamlDocument config) {
            m_config = config;
        }

        public String getApiToken() {
            return m_config.getValue(CONFIG_FIELD_API_TOKEN);
        }
    }
}
