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
import io.spikex.core.AbstractFilter;
import java.io.IOException;
import java.io.StringReader;
import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.parsers.SAXParser;
import javax.xml.parsers.SAXParserFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.vertx.java.core.Handler;
import org.vertx.java.core.buffer.Buffer;
import org.vertx.java.core.eventbus.EventBus;
import org.vertx.java.core.json.JsonArray;
import org.vertx.java.core.json.JsonObject;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;

/**
 *
 * @author cli
 */
public final class NagiosNrdpHandler implements Handler<HttpResponse> {

    private final NagiosNrdpProcessor m_processor;
    private final SAXParser m_parser;

    private static final String PARAM_XML_DATA = "XMLDATA=";
    private static final String PARAM_CMD = "&cmd=";
    private static final String PARAM_TOKEN = "&token=";

    private static final String CONF_KEY_NRDP_CONFIG = "nrdp-config";

    private final Logger m_logger = LoggerFactory.getLogger(NagiosNrdpHandler.class);

    public NagiosNrdpHandler(
            final AbstractFilter filter,
            final JsonObject config,
            final EventBus eventBus,
            final JsonArray tags) {

        m_processor = new NagiosNrdpProcessor(
                filter,
                config.getObject(CONF_KEY_NRDP_CONFIG, new JsonObject()),
                eventBus,
                tags);

        try {
            SAXParserFactory saxParserFactory = SAXParserFactory.newInstance();
            m_parser = saxParserFactory.newSAXParser();
        } catch (ParserConfigurationException | SAXException e) {
            throw new IllegalStateException("Failed to create XML parser for NRDP handler", e);
        }
    }

    public void init() {
        m_processor.init();
    }

    @Override
    public void handle(final HttpResponse response) {

        try {

            String data = URLDecoder.decode(response.getBody(), StandardCharsets.UTF_8.name());
            String cmd = parseParam(data, PARAM_CMD);
            String token = parseParam(data, PARAM_TOKEN);

            // Validate token before proceeding
            if (m_processor.isValidToken(token)) {

                m_logger.trace("Processing cmd: {} data: {}", cmd, data);
                NagiosNrdpXmlHandler xmlHandler = new NagiosNrdpXmlHandler(m_processor);

                try {
                    //
                    // Parse and process each checkresult
                    //
                    String xmlData = data.substring(
                            PARAM_XML_DATA.length(),
                            data.lastIndexOf(">") + 1).trim(); // End of XML data

                    m_parser.parse(new InputSource(
                            new StringReader(xmlData)), xmlHandler);

                } catch (SAXException | IOException e) {
                    m_logger.error("Failed to parse XML data", e);
                } finally {
                    m_parser.reset();
                }

                int count = xmlHandler.getProcessedCount();

                response.setContentType(MediaType.XML_UTF_8.toString());
                StringBuilder result = new StringBuilder("<?xml version=\"1.0\" encoding=\"UTF-8\"?>");
                result.append("<result>");
                result.append("<status>0</status>");
                result.append("<message>OK</message>");
                result.append("<meta>");
                result.append("<output>");
                result.append(count);
                result.append(" checks processed.</output>");
                result.append("</meta>");
                result.append("</result>");
                response.setContent(new Buffer(result.toString()));
            } else {
                m_logger.error("Received unauthorized token: {}", token);
            }
        } catch (UnsupportedEncodingException e) {
            m_logger.error("Failed to parse NRDP data", e);
        }
    }

    private String parseParam(
            final String data,
            final String param) {

        String value = "";
        int pos = data.indexOf(param);
        if (pos != -1) {
            int n1 = pos + param.length();
            int n2 = data.indexOf("&", n1);
            if (n2 == -1) {
                n2 = data.length();
            }
            value = data.substring(n1, n2);
        }
        return value;
    }
}
