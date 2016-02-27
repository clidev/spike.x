/**
 *
 * Copyright (c) 2015 NG Modular Oy.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */
package io.spikex.core.util.resource;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.PushbackInputStream;
import java.io.Reader;
import java.net.HttpURLConnection;
import java.net.URI;
import java.net.URLConnection;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import io.spikex.core.util.IVersion;
import io.spikex.core.util.Version;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * TODO Atomic writes...
 * @author cli
 */
public abstract class AbstractResourceReaderWriter<T extends IResource>
        implements IResourceReaderWriter<T> {

    public static String PARAM_BUFFER_SIZE_KB = "spikex-buffer-size-kb";
    //
    protected static String PARAM_CONN_DIRECTION = "spikex-conn-direction";
    protected static String PARAM_CONN_TIMEOUT = "spikex-conn-timeout";
    protected static String PARAM_READ_TIMEOUT = "spikex-read-timeout";
    protected static String PARAM_CHARSET_ENCODING = "spikex-conn-encoding";
    protected static String CONN_UPLOAD = "upload";
    protected static String CONN_DOWNLOAD = "download";
    //
    private static final String META_FILE_SUFFIX = ".mf";
    //
    private static final int DEF_CONN_TIMEOUT = 5000; // 5 sec
    private static final int DEF_READ_TIMEOUT = 15000; // 15 sec
    //
    private final Logger m_logger = LoggerFactory.getLogger(getClass());

    public AbstractResourceReaderWriter() {
    }

    @Override
    public URI findResource(
            final URI base,
            final T resource) throws ResourceException {

        //
        // 1. lang + country + variant
        // 2. lang + country
        // 3. lang
        // 4. <plain>
        //
        String name = resource.getName();
        IVersion version = resource.getVersion();
        Locale locale = resource.getLocale();

        String lang = locale.getLanguage();
        String country = locale.getCountry();
        String variant = locale.getVariant();

        getLogger().trace("Base URI: {} name: {} version: {} lang: {} country: {} variant: {}",
                new Object[]{base.toString(), name, version.getSequence(), lang, country, variant});

        List<Locale> locales = new ArrayList();

        if (variant.length() > 0) {
            locales.add(new Locale(
                    locale.getLanguage(),
                    locale.getCountry(),
                    locale.getVariant()));
        }
        if (country.length() > 0) {
            locales.add(new Locale(
                    locale.getLanguage(),
                    locale.getCountry()));
        }
        if (lang.length() > 0) {
            locales.add(new Locale(locale.getLanguage()));
        }
        locales.add(Locale.ROOT); // No locale

        URI location = null;
        boolean found = false;

        for (Locale tryLocale : locales) {

            String filename = resource.getQualifiedName(tryLocale);
            location = base.normalize().resolve(filename);
            getLogger().trace("Looking for resource: {}", location);

            if (existsResource(location)) {
                found = true;
                break;
            }
        }
        if (!found) {
            throw new ResourceException(
                    ResourceException.ERR_RESOURCE_NOT_FOUND,
                    "Resource not found: " + name
                    + " version: " + version.getSequence()
                    + " location: " + location,
                    location);
        } else {
            getLogger().trace("Found resource: {}", location);
        }
        return location;
    }

    @Override
    public IVersion readLatestVersion(
            final URI base,
            final T resource)
            throws ResourceException {

        // Create full path of the resource meta file
        URI location = getMetaFileLocation(base, resource);
        getLogger().trace("Reading latest version from: {}", location);

        IVersion version = Version.none();
        if (existsResource(location)) {

            Map<String, String> params = new HashMap();
            try (InputStream in = createInputStream(location, params)) {

                version = Manifests.readLatestVersion(resource, in);
                getLogger().trace("Latest version: {}", version.getSequence());

            } catch (IOException e) {
                throw new ResourceException(
                        ResourceException.ERR_READ_LATEST_VERSION_FAILED,
                        "Failed to create input stream for location: " + location,
                        location,
                        e);
            }
        }
        return version;
    }

    @Override
    public void writeLatestVersion(
            final URI base,
            final T resource) throws ResourceException {

        // Create full path of the resource meta file
        URI location = getMetaFileLocation(base, resource);
        getLogger().trace("Writing latest version to: {}", location);

        Map<String, String> params = new HashMap();
        try (OutputStream out = createOutputStream(location, params)) {

            Manifests.writeLatestVersion(resource, out);

        } catch (IOException e) {
            throw new ResourceException(
                    ResourceException.ERR_WRITE_LATEST_VERSION_FAILED,
                    "Failed to create output stream for location: " + location,
                    location,
                    e);
        }
    }

    @Override
    public abstract T readSnapshot(
            final URI base,
            final T resource) throws ResourceException;

    @Override
    public abstract URI writeSnapshot(
            final URI base,
            final T resource) throws ResourceException;

    protected boolean existsResource(final URI uri) throws ResourceException {

        boolean found = false;

        if ("file".equals(uri.getScheme())) {
            if (new File(uri).exists()) {
                found = true;
            }
        } else {
            InputStream in = null;
            try {
                URLConnection conn = uri.toURL().openConnection();
                conn.setConnectTimeout(DEF_CONN_TIMEOUT);
                conn.setReadTimeout(DEF_READ_TIMEOUT);
                if ("http".equals(uri.getScheme())
                        || "https".equals(uri.getScheme())) {
                    conn.setRequestProperty("User-Agent",
                            "Mozilla/5.0 ( compatible ) ");
                    conn.setRequestProperty("Accept", "*/*");
                }
                in = conn.getInputStream();
                if (in.available() > 0) {
                    found = true;
                } else {
                    //
                    // Not all streams support available
                    //
                    if (in.markSupported()) {
                        int len = in.read(new byte[4]);
                        in.reset();
                        if (len != -1) {
                            found = true;
                        }
                    } else {
                        PushbackInputStream pin
                                = new PushbackInputStream(in, 4);
                        byte[] data = new byte[4];
                        int len = pin.read(data);
                        pin.unread(data);
                        if (len != -1) {
                            found = true;
                        }
                    }
                }
            } catch (IOException e) {
                getLogger().debug("Possibly non-existent resource: {}", uri, e);
            } finally {
                try {
                    if (in != null) {
                        in.close();
                    }
                } catch (IOException e) {
                    throw new ResourceException(
                            ResourceException.ERR_CLOSE_STREAM_FAILED,
                            "Failed to close input stream",
                            uri,
                            e);
                }
            }
        }
        return found;
    }

    /**
     * Returns the logger that is created in this abstract class using:      <code>
     * LoggerFactory.getLogger(getClass());
     * </code>
     *
     * @return the logger that was created by this abstract class
     */
    protected Logger getLogger() {
        return m_logger;
    }

    protected URI getMetaFileLocation(
            final URI base,
            final IResource resource) {

        // Filename
        StringBuilder sb = new StringBuilder(resource.getName());
        sb.append(META_FILE_SUFFIX);

        return base.normalize().resolve(sb.toString());
    }

    protected URI getResourceFileLocation(
            final URI base,
            final IResource resource) {
        String filename = resource.getQualifiedName();
        return base.normalize().resolve(filename);
    }

    protected InputStream createInputStream(
            final URI location,
            final Map<String, String> params) throws ResourceException {

        InputStream in = null;
        try {
            URLConnection conn = openConnection(location, params);
            in = conn.getInputStream();
        } catch (IOException e) {
            throw new ResourceException(
                    ResourceException.ERR_CREATE_STREAM_FAILED,
                    "Failed to create input stream for URI: " + location,
                    location,
                    e);
        }
        return in;
    }

    protected OutputStream createOutputStream(
            final URI location,
            final Map<String, String> params) throws ResourceException {
        OutputStream out = null;
        try {
            if ("file".equals(location.getScheme())) {
                out = new FileOutputStream(new File(location));
            } else {
                URLConnection conn = openConnection(location, params);
                out = conn.getOutputStream();
            }
        } catch (IOException e) {
            throw new ResourceException(
                    ResourceException.ERR_CREATE_STREAM_FAILED,
                    "Failed to create output stream for URI: " + location,
                    location,
                    e);
        }
        return out;
    }

    private URLConnection openConnection(
            final URI uri,
            final Map<String, String> params) throws IOException {
        //
        URLConnection conn = uri.toURL().openConnection();
        conn.setConnectTimeout(DEF_CONN_TIMEOUT);
        conn.setReadTimeout(DEF_READ_TIMEOUT);
        //
        if (params != null) {
            String timeout = params.get(PARAM_CONN_TIMEOUT);
            if (timeout != null) {
                conn.setConnectTimeout(Integer.parseInt(timeout));
            }
            timeout = params.get(PARAM_READ_TIMEOUT);
            if (timeout != null) {
                conn.setReadTimeout(Integer.parseInt(timeout));
            }
            String direction = params.get(PARAM_CONN_DIRECTION);
            Iterator<String> keys = params.keySet().iterator();
            while (keys.hasNext()) {
                String key = keys.next();
                String value = params.get(key);
                if (key.startsWith("spikex")) {
                    setInternalParam(
                            uri,
                            conn,
                            key,
                            value,
                            CONN_UPLOAD.equals(direction));
                } else {
                    conn.setRequestProperty(key, value);
                }
            }
            getLogger().trace("URL connection request properties: {}",
                    conn.getRequestProperties());
        }
        //
        return conn;
    }

    private void setInternalParam(
            URI base,
            URLConnection connection,
            String key,
            String value,
            boolean upload) {
        //
        String protocol = base.getScheme();
        if ("http".equals(protocol)) {
            //
            // HTTP upload/download
            //
            if (upload) {
                connection.setDoInput(false);
                connection.setDoOutput(true);
                String boundary = Long.toHexString(System.currentTimeMillis());
                connection.setRequestProperty("Content-Type",
                        "multipart/form-data; boundary=" + boundary);
                //
                // Chunked streaming mode
                //
                if (PARAM_BUFFER_SIZE_KB.equals(key)) {
                    Integer size = Integer.parseInt(value);
                    ((HttpURLConnection) connection).setChunkedStreamingMode(size);
                }
            } else {
                connection.setDoInput(true);
                connection.setDoOutput(false);
                connection.setRequestProperty("User-Agent",
                        "Mozilla/5.0 ( compatible ) ");
                connection.setRequestProperty("Accept", "*/*");
            }
        }
    }

    protected String readString(
            final InputStream in,
            final Charset encoding) throws IOException {

        char[] buf = new char[4096];
        Reader r = new InputStreamReader(in, encoding.name());
        StringBuilder sb = new StringBuilder();
        while (true) {
            int n = r.read(buf);
            if (n < 0) {
                break;
            }
            sb.append(buf, 0, n);
        }
        return sb.toString();
    }

    protected <T extends IResource<String>> void writeString(
            final T resource,
            final OutputStream out) throws IOException {

        Charset encoding = resource.getEncoding();
        out.write(resource.getData().getBytes(encoding));
        out.flush();
    }

    protected void writeString(
            final String data,
            final Charset encoding,
            final OutputStream out) throws IOException {

        out.write(data.getBytes(encoding));
        out.flush();
    }
}
