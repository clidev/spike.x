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

import com.google.common.base.Strings;
import org.xml.sax.Attributes;
import org.xml.sax.SAXException;
import org.xml.sax.helpers.DefaultHandler;

/**
 *
 * @author cli
 */
public final class NagiosNrdpXmlHandler extends DefaultHandler {

    private String m_type;
    private String m_host;
    private String m_service;
    private String m_state;
    private String m_output;
    private int m_count;

    private String m_currentElement;

    private final NagiosNrdpProcessor m_processor;

    private static final String ATTR_NAME_TYPE = "type";
    private static final String ELEM_NAME_CHECKRESULT = "checkresult";
    private static final String ELEM_NAME_HOSTNAME = "hostname";
    private static final String ELEM_NAME_SERVICENAME = "servicename";
    private static final String ELEM_NAME_STATE = "state";
    private static final String ELEM_NAME_OUTPUT = "output";
    private static final String ELEM_NONE = "";

    public NagiosNrdpXmlHandler(final NagiosNrdpProcessor processor) {
        m_processor = processor;
        m_count = 0;
    }

    public int getProcessedCount() {
        return m_count;
    }

    @Override
    public void startElement(
            final String uri,
            final String localName,
            final String qName,
            final Attributes attributes) throws SAXException {

        m_currentElement = qName;

        if (ELEM_NAME_CHECKRESULT.equals(qName)) {
            m_type = attributes.getValue(ATTR_NAME_TYPE);
        }
    }

    @Override
    public void characters(
            final char buffer[],
            final int start,
            final int length) throws SAXException {

        String str = new String(buffer, start, length);
        String element = m_currentElement;

        if (!Strings.isNullOrEmpty(element)) {
            switch (element.toLowerCase()) {
                case ELEM_NAME_HOSTNAME:
                    m_host = str;
                    break;
                case ELEM_NAME_SERVICENAME:
                    m_service = str;
                    break;
                case ELEM_NAME_STATE:
                    m_state = str;
                    break;
                case ELEM_NAME_OUTPUT:
                    m_output = str;
                    break;
            }
            m_currentElement = ELEM_NONE; // Reset element state
        }
    }

    @Override
    public void endElement(
            final String uri,
            final String localName,
            final String qName) throws SAXException {

        // Ready to process checkresult data
        if (ELEM_NAME_CHECKRESULT.equals(qName)) {
            if (m_processor.processCheckResult(m_host, m_service, m_state, m_output)) {
                m_count++;
            }
        }
    }
}
