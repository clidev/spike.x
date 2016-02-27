/**
 *
 * Copyright (c) 2015 NG Modular Oy.
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

import java.io.IOException;
import java.net.URI;

/**
 * Resource exception used to indicate failure to load or save a resource.
 *
 * @version $Revision: 316 $
 * @author cli
 * @since Solstice Common 1.0
 */
public final class ResourceException extends IOException {

    // TODO fix error coding all over... use strings instead?
    public static final int ERR_NULL_STREAM = 10000010;
    public static final int ERR_CLOSE_STREAM_FAILED = 10000014;
    public static final int ERR_CREATE_STREAM_FAILED = 10000018;
    public static final int ERR_RESOURCE_NOT_FOUND = 10000030;
    public static final int ERR_READ_LATEST_VERSION_FAILED = 10000040;
    public static final int ERR_WRITE_LATEST_VERSION_FAILED = 10000044;
    public static final int ERR_READ_SNAPSHOT_FAILED = 10000060;
    public static final int ERR_WRITE_SNAPSHOT_FAILED = 10000064;
    public static final int ERR_OPEN_CONNECTION_FAILED = 10000070;
    public static final int ERR_CLOSE_CONNECTION_FAILED = 10000074;
    //
    private final int m_code;
    private final URI m_uri;
    //
    private static final long serialVersionUID = 2052787313408423610L;

    /**
     * Creates a new resource exception.
     * 
     * @param code              the error code
     * @param message           the error message
     */
    public ResourceException(
            int code,
            String message) {
        this(code, message, null, null);
    }

    /**
     * Creates a new resource exception.
     * 
     * @param code              the error code
     * @param message           the error message
     * @param location          the resource URI
     */
    public ResourceException(
            int code,
            String message,
            URI location) {
        this(code, message, location, null);
    }

    /**
     * Creates a new resource exception.
     * 
     * @param code              the error code
     * @param message           the error message
     * @param cause             the original exception
     */
    public ResourceException(
            int code,
            String message,
            Throwable cause) {
        this(code, message, null, cause);
    }

    /**
     * Creates a new resource exception.
     * 
     * @param code              the error code
     * @param message           the error message
     * @param location          the resource URI
     * @param cause             the original exception
     */
    public ResourceException(
            int code,
            String message,
            URI location,
            Throwable cause) {
        super(message, cause);
        m_code = code;
        m_uri = location;
    }

    public int getCode() {
        return m_code;
    }

    public URI getUri() {
        return m_uri;
    }
}
