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
package io.spikex.notifier.internal;

import com.google.common.base.Strings;
import static io.spikex.core.helper.Events.EVENT_PRIORITY_HIGH;
import static io.spikex.core.helper.Events.EVENT_PRIORITY_LOW;
import static io.spikex.core.helper.Events.EVENT_PRIORITY_NORMAL;
import io.spikex.core.helper.Variables;
import io.spikex.notifier.NotifierConfig.EmailDef;
import java.io.IOException;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import javax.mail.Message;
import org.codemonkey.simplejavamail.Email;
import org.codemonkey.simplejavamail.Mailer;
import org.codemonkey.simplejavamail.TransportStrategy;
import org.jsoup.Jsoup;
import org.jsoup.safety.Whitelist;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.unbescape.html.HtmlEscape;

/**
 * TODO provide option to specify the plain-text email template
 * <p>
 * @author cli
 */
public final class EmailClient {

    private final EmailDef m_config;
    private final Variables m_variables;
    private final boolean m_escapeHtml;

    private static final String MIME_IMAGE_PNG = "image/png";

    private final Logger m_logger = LoggerFactory.getLogger(EmailClient.class);

    public EmailClient(
            final EmailDef config,
            final Variables variables,
            final boolean escapeHtml) {

        m_config = config;
        m_variables = variables;
        m_escapeHtml = escapeHtml;
    }

    public void send(
            final URI destUri,
            final String subject,
            final String body,
            final String priority) {

        Email email = new Email();
        String sender = m_variables.translate(m_config.getFrom());
        String fromName = sender;

        // Short name of sender
        int pos = sender.indexOf('@');
        if (pos > 0) {
            fromName = sender.substring(0, pos);
        }

        String toName = destUri.getSchemeSpecificPart();;
        String recipient = toName;

        // Short name of recipient
        pos = toName.indexOf('@');
        if (pos > 0) {
            toName = toName.substring(0, pos);
        }

        // Priority header
        if (m_config.getUsePriorities()) {
            switch (priority) {
                case EVENT_PRIORITY_LOW:
                    email.addHeader("X-Priority", "5 (Lowest)");
                    // Spam filters want also the X-MimeOLE header with this one
                    //email.addHeader("X-MSMail-Priority", "Low");
                    email.addHeader("Importance", "Low");
                    break;
                case EVENT_PRIORITY_NORMAL:
                    email.addHeader("X-Priority", "3 (Normal)");
                    // Spam filters want also the X-MimeOLE header with this one
                    //email.addHeader("X-MSMail-Priority", "Normal");
                    email.addHeader("Importance", "Normal");
                    break;
                case EVENT_PRIORITY_HIGH:
                    email.addHeader("X-Priority", "1 (Highest)");
                    // Spam filters want also the X-MimeOLE header with this one
                    //email.addHeader("X-MSMail-Priority", "High");
                    email.addHeader("Importance", "High");
                    break;
            }
        }

        // Headers
        {
            Map<String, Object> headers = m_config.getHeaders();
            Set<String> keys = headers.keySet();
            for (String key : keys) {
                String value = String.valueOf(headers.get(key));
                m_logger.debug("Adding header: {}={}", key, value);
                email.addHeader(key, value);
            }
        }

        email.setFromAddress(fromName, sender);
        email.addRecipient(toName, recipient, Message.RecipientType.TO);
        email.setSubject(subject);

        //
        // Escape body
        //
        String text = body;
        if (m_escapeHtml) {
            text = HtmlEscape.escapeHtml4Xml(text);
        }
        email.setTextHTML(text);

        // Plain text (preserve some line breaks)
        text = Jsoup.clean(body.replace("\n", "|"), Whitelist.none());
        text = text.replace("||", "\n").replaceAll("(\\S)\\|", "$1\n");
        text = text.replaceAll("\\s?\\|", "").replace(" \n \n", "");
        email.setText(text);

        // Locally available images
        {
            Map<String, String> images = m_config.getImages();
            Set<String> keys = images.keySet();
            for (String key : keys) {
                //
                // Only embedd images that are referenced as CIDs
                //
                String png = (String) images.get(key);
                String cid = "cid:" + key;
                if (body.contains(cid)) {
                    if (!Strings.isNullOrEmpty(png)) {
                        try {
                            String path = m_variables.translate(png);
                            m_logger.debug("Embedding image: {}", path);
                            Path pngPath = Paths.get(path).toAbsolutePath();
                            byte[] bytes = Files.readAllBytes(pngPath);
                            email.addEmbeddedImage(key, bytes, MIME_IMAGE_PNG);
                        } catch (IOException e) {
                            m_logger.error("Failed to embedd PNG image to email", e);
                        }
                    }
                } else {
                    m_logger.debug("No reference to \"{}\" found, ignoring image: {}", cid, png);
                }
            }
        }

        String smtpHost = m_config.getSmtpHost();
        int smtpPort = m_config.getSmtpPort();
        String smtpUser = null;
        if (m_config.hasSmtpUser()) {
            smtpUser = m_config.getSmtpUser();
        }
        String smtpPassword = null;
        if (m_config.hasSmtpPassword()) {
            smtpPassword = m_config.getSmtpPassword();
        }
        boolean tlsEnabled = m_config.isTlsEnabled();
        boolean sslEnabled = m_config.isSslEnabled();

        m_logger.debug("Sending email from: {} to: {} subject: {} "
                + "(smtp host: {} port: {} user: {} tls: {} ssl: {})",
                sender, toName, subject, smtpHost, smtpPort, smtpUser, tlsEnabled, sslEnabled);

        int retryCount = m_config.getRetryCount();
        int retryWaitSec = m_config.getRetryWaitSec();

        for (int i = 0; i < retryCount; i++) {
            try {
                // Bombs away...
                if (tlsEnabled) {
                    new Mailer(smtpHost, smtpPort, smtpUser, smtpPassword, TransportStrategy.SMTP_TLS)
                            .sendMail(email);
                } else if (sslEnabled) {
                    new Mailer(smtpHost, smtpPort, smtpUser, smtpPassword, TransportStrategy.SMTP_SSL)
                            .sendMail(email);
                } else {
                    new Mailer(smtpHost, smtpPort, smtpUser, smtpPassword)
                            .sendMail(email);
                }
                i = retryCount; // Done
            } catch (Exception e) {
                m_logger.error("Failed to send email", e);
                try {
                    TimeUnit.SECONDS.sleep(retryWaitSec);
                } catch (InterruptedException ie) {
                    m_logger.error("Retry sleep was interrupted", ie);
                }
            }
        }
    }
}
