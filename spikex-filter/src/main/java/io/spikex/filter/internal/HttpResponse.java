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

import com.google.common.net.MediaType;
import org.vertx.java.core.buffer.Buffer;
import org.vertx.java.core.http.HttpServerRequest;
import org.vertx.java.core.http.HttpServerResponse;

/**
 *
 * @author cli
 */
public final class HttpResponse {

    private Buffer m_content;
    private String m_type;

    private final HttpServerRequest m_request;
    private final String m_body;

    public HttpResponse(
            final HttpServerRequest request,
            final String body) {

        m_request = request;
        m_body = body;
        m_type = MediaType.ANY_TEXT_TYPE.toString();
    }

    public boolean hasContent() {
        return (m_content != null);
    }

    public String getBody() {
        return m_body;
    }

    public Buffer getContent() {
        return m_content;
    }

    public String getContentType() {
        return m_type;
    }

    public HttpServerRequest getRequest() {
        return m_request;
    }

    public HttpServerResponse getResponse() {
        return m_request.response();
    }

    public void setStatusCode(final int code) {
        getResponse().setStatusCode(code);
    }

    public void setContent(final Buffer content) {
        m_content = content;
    }

    public void setContentType(final String type) {
        m_type = type;
    }
}
