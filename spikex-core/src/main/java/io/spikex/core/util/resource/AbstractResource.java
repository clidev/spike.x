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

import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import com.google.common.eventbus.EventBus;
import java.net.URI;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.Locale;
import io.spikex.core.util.IBuilder;
import io.spikex.core.util.IVersion;
import io.spikex.core.util.Version;

/**
 * Abstract base class for resource implementations.
 *
 * TODO fix javadocs
 * TODO add sanity checks
 *
 * @param <V>
 *
 * @author cli
 */
public abstract class AbstractResource<V> implements IResource<V> {

    /**
     * The supported URI params
     */
    /*
     public static final String URI_PARAM_VERSION = "version";
     public static final String URI_PARAM_LOCALE = "locale";
     public static final String URI_PARAM_ENCODING = "encoding";
     public static final String URI_PARAM_SUFFIX = "suffix";
     */
    //
    private final String m_name; // The resource name (eg. settings)
    private final String m_suffix; // The suffix of the filename
    private final IVersion m_version; // The resource version
    private final Locale m_locale; // The resource locale
    private final Charset m_encoding; // Character set used by the resource (canonical name)
    private final IVersionStrategy m_strategy;
    private final URI m_location; // The resolved full path, including filename
    private final IResourceProvider m_provider; // The resource provider
    private final EventBus m_eventBus; // Used for resource events
    //
    private Object[] m_listeners; // Resource event listeners
    private volatile int m_hashCode;
    //
    private static final String DEF_SUFFIX = "";
    private static final IVersion DEF_VERSION = Version.none();
    private static final Locale DEF_LOCALE = Locale.ROOT;
    private static final Charset DEF_ENCODING = StandardCharsets.UTF_8;
    private static final IVersionStrategy DEF_STRATEGY = new IncreasingVersionStrategy();

    public AbstractResource(
            final String name,
            final IVersion version,
            final Locale locale,
            final Charset encoding,
            final String suffix,
            final IVersionStrategy strategy,
            final URI location,
            final IResourceProvider provider) {

        m_name = name;
        m_version = version;
        m_locale = locale;
        m_encoding = encoding;
        m_suffix = suffix;
        m_strategy = strategy;
        m_location = location;
        m_provider = provider;
        m_eventBus = new EventBus(name);
        m_listeners = new Object[0];
    }

    /**
     * Please see {@link java.lang.Object#equals} for documentation.
     */
    @Override
    public boolean equals(final Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null) {
            return false;
        }
        if (obj.getClass() != getClass()) {
            return false;
        }
        return (hashCode() == obj.hashCode());
    }

    /**
     * Please see {@link java.lang.Object#hashCode} for documentation.
     */
    @Override
    public int hashCode() {
        // Racy single-check is acceptable for hashCode
        // [Effective Java, Joshua Block, Item 71]
        int hashCode = m_hashCode;
        if (hashCode == 0) {
            hashCode = 13;
            hashCode = 7 * hashCode + m_name.hashCode();
            hashCode = 7 * hashCode + m_suffix.hashCode();
            hashCode = 7 * hashCode + m_locale.hashCode();
            hashCode = 7 * hashCode + m_version.hashCode();
            hashCode = 7 * hashCode + m_encoding.hashCode();
            hashCode = 7 * hashCode + m_strategy.hashCode();
            hashCode = 7 * hashCode + m_location.hashCode();
            m_hashCode = hashCode;
        }
        return hashCode;
    }

    @Override
    public final String getName() {
        return m_name;
    }

    @Override
    public String getLocalizedName() {
        return getLocalizedName(getLocale());
    }

    @Override
    public String getQualifiedName() {
        return getQualifiedName(getLocale());
    }

    @Override
    public String getQualifiedName(final Locale locale) {
        //
        // Generate filename (with locale part)
        //
        StringBuilder sb = new StringBuilder(getLocalizedName(locale));
        //
        // Add version suffix if required
        //
        int ver = getVersion().getSequence();
        if (ver >= 0) {
            sb.append(".");
            sb.append(ver);
        }
        //
        // Add file suffix (if defined)
        //
        String suffix = getSuffix();
        if (suffix != null && suffix.length() > 0) {
            sb.append(suffix);
        }

        return sb.toString();
    }

    @Override
    public final IVersion getVersion() {
        return m_version;
    }

    @Override
    public final Locale getLocale() {
        return m_locale;
    }

    @Override
    public final Charset getEncoding() {
        return m_encoding;
    }

    @Override
    public final String getSuffix() {
        return m_suffix;
    }

    @Override
    public IVersionStrategy getVersionStrategy() {
        return m_strategy;
    }

    @Override
    public final URI getLocation() {
        return m_location;
    }

    @Override
    public final boolean exists() {
        return getResourceProvider().exists(this);
    }

    public final Object[] getListeners() {
        return m_listeners;
    }

    public final void addListeners(final Object... listeners) {
        m_listeners = listeners;
        for (Object listener : listeners) {
            m_eventBus.register(listener);
        }
    }

    public final void removeListener(final Object listener) {
        m_eventBus.unregister(listener);
        Object[] tmp = new Object[0];
        int len = m_listeners.length;
        if (len > 1) {
            tmp = new Object[len - 1];
        }
        for (int i = 0, j = 0; i < len; i++) {
            Object lsn = m_listeners[i];
            if (lsn != listener) {
                tmp[j++] = lsn;
            }
        }
        m_listeners = tmp;
    }

    public final void removeListeners() {
        for (Object listener : m_listeners) {
            m_eventBus.unregister(listener);
        }
    }

    @Override
    public abstract V getData();

    @Override
    public abstract boolean isSaveable();

    @Override
    public abstract <T extends IResource> T load() throws ResourceException;

    @Override
    public abstract <T extends IResource> T save() throws ResourceException;

    protected <T extends IResource> IResourceProvider<T> getResourceProvider() {
        return m_provider;
    }

    protected String getLocalizedName(final Locale locale) {
        StringBuilder sb = new StringBuilder(getName().toLowerCase());
        String lang = locale.getLanguage();
        String country = locale.getCountry();
        String variant = locale.getVariant();
        if (lang.length() > 0) {
            sb.append("_");
            sb.append(lang);
        }
        if (country.length() > 0) {
            sb.append("_");
            sb.append(country);
        }
        if (variant.length() > 0) {
            sb.append("_");
            sb.append(variant);
        }
        return sb.toString();
    }

    protected void publishEvent(
            final IResource resource,
            final ResourceChangeEvent.State state) {
        m_eventBus.post(new ResourceChangeEvent(resource, state));
    }

    /**
     * Based on:
     * http://stackoverflow.com/questions/17164375/subclassing-a-java-builder-class
     *
     * @param <E>
     * @param <T>
     */
    protected abstract static class Builder<E extends Builder, T extends AbstractResource>
            implements IBuilder<T> {

        private String m_name;
        private String m_suffix;
        private IVersion m_version;
        private Locale m_locale;
        private Charset m_encoding;
        private IVersionStrategy m_strategy;
        private URI m_location;
        private Object[] m_listeners;

        private final IResourceProvider m_provider;

        protected Builder(final IResourceProvider provider) {
            m_provider = provider;
            m_suffix = DEF_SUFFIX;
            m_version = DEF_VERSION;
            m_locale = DEF_LOCALE;
            m_encoding = DEF_ENCODING;
            m_strategy = DEF_STRATEGY;
            m_listeners = new Object[0];
        }

        protected Builder(
                final IResourceProvider provider,
                final AbstractResource resource) {

            m_provider = provider; // No default
            m_name = resource.getName(); // No default
            m_location = resource.getLocation(); // No default

            m_suffix = Objects.firstNonNull(
                    resource.getSuffix(),
                    DEF_SUFFIX);

            m_version = Objects.firstNonNull(
                    resource.getVersion(),
                    DEF_VERSION);

            m_locale = Objects.firstNonNull(
                    resource.getLocale(),
                    DEF_LOCALE);

            m_encoding = Objects.firstNonNull(
                    resource.getEncoding(),
                    DEF_ENCODING);

            m_strategy = Objects.firstNonNull(
                    resource.getVersionStrategy(),
                    DEF_STRATEGY);

            m_listeners = Objects.firstNonNull(
                    resource.getListeners(),
                    new Object[0]);
        }

        public final E name(final String name) {
            m_name = name;
            return (E) this;
        }

        public final E suffix(final String suffix) {
            m_suffix = suffix;
            return (E) this;
        }

        public final E version(final IVersion version) {
            m_version = version;
            return (E) this;
        }

        public final E locale(final Locale locale) {
            m_locale = locale;
            return (E) this;
        }

        public final E encoding(final Charset encoding) {
            m_encoding = encoding;
            return (E) this;
        }

        public final E strategy(final IVersionStrategy strategy) {
            m_strategy = strategy;
            return (E) this;
        }

        public final E location(final URI location) {
            m_location = location;
            return (E) this;
        }

        public final E listeners(final Object... listeners) {
            m_listeners = listeners;
            return (E) this;
        }
    }

    protected AbstractResource(final Builder builder) {
        //
        // Sanity checks
        //
        Preconditions.checkNotNull(builder.m_provider, "Resource provider is null");
        Preconditions.checkArgument(builder.m_name != null && builder.m_name.length() > 0,
                "Resource name is null or empty");
        Preconditions.checkNotNull(builder.m_suffix, "Resource suffix is null");
        Preconditions.checkNotNull(builder.m_version, "Resource version is null");
        Preconditions.checkNotNull(builder.m_locale, "Resource locale is null");
        Preconditions.checkNotNull(builder.m_encoding, "Resource encoding is null");
        Preconditions.checkNotNull(builder.m_strategy, "Resource version strategy is null");
        //
        // Assign values
        //
        m_provider = builder.m_provider;
        m_name = builder.m_name;
        m_suffix = builder.m_suffix;
        m_version = builder.m_version;
        m_locale = builder.m_locale;
        m_encoding = builder.m_encoding;
        m_strategy = builder.m_strategy;
        m_location = builder.m_location;
        m_eventBus = new EventBus(builder.m_name);
        // Register listeners
        addListeners(builder.m_listeners);
    }
}
