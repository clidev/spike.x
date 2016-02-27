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
package io.spikex.filter;

import com.google.common.base.Preconditions;
import io.spikex.core.AbstractFilter;
import io.spikex.core.util.CronEntry;
import io.spikex.filter.internal.Rule;
import io.spikex.filter.internal.Throttle;
import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.mapdb.DB;
import org.mapdb.DBMaker;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.vertx.java.core.Handler;
import org.vertx.java.core.json.JsonArray;
import org.vertx.java.core.json.JsonObject;

/**
 * Example:
 * <pre>
 *  "chain": [
 *      { "Limit":
 *                  "discard-on-mismatch": true,
 *                  "schedules": {
 *                            "worktime": "8-15 * * * Mon-Fri",
 *                            "evening": "16-21 * * * Mon-Fri Europe/Samara",
 *                            "anytime": "* * * * *"
 *                          },
 *                  "throttles": {
 *                            "burst-per-minute": {
 *                                              "rate": 1,
 *                                              "interval": 60,
 *                                              "unit": "sec",
 *                                              "queue": true,
 *                                              "queue-size": 10000
 *                                          },
 *                            "one-per-hour": {
 *                                              "rate": 1,
 *                                              "interval": 1,
 *                                              "unit": "hour",
 *                                              "checksum-field": "@message"
 *                                          }
 *                          },
 *                  "rules": [
 *                              {
 *                                  "match-field": "@day",
 *                                  "value-lte": 20,  // equals, lte, gte, lt, gt, not-in, in
 *                                  "throttle": "burst-per-minute"
 *                              },
 *                              {
 *                                  "id": "rule-error-1h",
 *                                  "match-tag": "ERROR",
 *                                  "schedule": "anytime",
 *                                  "throttle": "one-per-hour"
 *                              }
 *                      ]
 *      }
 * ]
 * </pre>
 *
 * @author cli
 */
public class Limit extends AbstractFilter {

    private final Map<String, CronEntry> m_schedules;
    private final List<Rule> m_rules;

    // Persistent (keep track of last check and recent checksums)
    private Map<String, Throttle> m_throttles; // throttle-id => throttle

    private DB m_db;
    private long m_timerId;
    private boolean m_discardOnMismatch;

    private static final String CONF_KEY_SCHEDULES = "schedules";
    private static final String CONF_KEY_THROTTLES = "throttles";
    private static final String CONF_KEY_ID = "id";
    private static final String CONF_KEY_RULES = "rules";
    private static final String CONF_KEY_DATABASE_NAME = "database-name";
    private static final String CONF_KEY_DATABASE_PASSWORD = "database-password";
    private static final String CONF_KEY_DATABASE_COMPACT = "database-compact-on-startup";
    private static final String CONF_KEY_DISCARD_ON_MISMATCH = "discard-on-mismatch";

    private static final String DB_NAME = "limit.db";
    private static final String DB_PASSWD = "891Akq9aFnX-0U";
    private static final String DB_THROTTLES_MAP_NAME = "limit-throttles";

    private static final String SCHEDULE_ANYTIME_NAME = "anytime";
    private static final String SCHEDULE_ANYTIME_ENTRY = "* * * * *";

    public Limit() {
        m_schedules = new HashMap();
        m_rules = new ArrayList();
    }

    @Override
    protected void startFilter() {
        //
        // Initialize local database
        //
        String dbName = config().getString(CONF_KEY_DATABASE_NAME, DB_NAME);
        String dbPassword = config().getString(CONF_KEY_DATABASE_PASSWORD, DB_PASSWD);
        File dbFile = new File(dataPath().toFile(), dbName);
        m_db = DBMaker.newFileDB(dbFile)
                .closeOnJvmShutdown()
                .checksumEnable()
                .compressionEnable()
                .encryptionEnable(dbPassword)
                .make();

        // Compact on startup by default
        if (config().getBoolean(CONF_KEY_DATABASE_COMPACT, true)) {
            logger().info("Compacting database: {}", dbFile.getAbsolutePath());
            m_db.compact();
        }
        //
        // Populate maps with persisted throttles and checksums
        //        
        Throttle.MapDbSerializer serializer = new Throttle.MapDbSerializer();
        m_throttles = m_db.createHashMap(DB_THROTTLES_MAP_NAME)
                .valueSerializer(serializer)
                .makeOrGet();
        //
        // Cron-like schedules
        //
        {
            JsonObject schedules = config().getObject(CONF_KEY_SCHEDULES, new JsonObject());
            Iterator<String> fields = schedules.getFieldNames().iterator();
            while (fields.hasNext()) {
                String name = fields.next();
                String schedule = schedules.getString(name);
                m_schedules.put(name, CronEntry.create(schedule));
            }
        }
        // Add anytime schedule if no schedules defined
        if (m_schedules.isEmpty()) {
            m_schedules.put(SCHEDULE_ANYTIME_NAME, CronEntry.create(SCHEDULE_ANYTIME_ENTRY));
        }
        //
        // Throttles
        //
        {
            JsonObject throttles = config().getObject(CONF_KEY_THROTTLES, new JsonObject());
            Preconditions.checkState(throttles.size() > 0, "No throttles have been defined");
            Iterator<String> fields = throttles.getFieldNames().iterator();
            List<String> defined = new ArrayList();

            while (fields.hasNext()) {

                String name = fields.next();
                JsonObject throttleDef = throttles.getObject(name);
                Throttle throttle = Throttle.create(name, throttleDef);

                if (m_throttles.containsKey(name)) {
                    Throttle tmp = m_throttles.get(name);
                    tmp.update(throttle);
                    m_throttles.put(name, tmp);
                } else {
                    m_throttles.put(name, throttle);
                }
                defined.add(name);
            }

            // Remove undefined throttles
            Iterator<String> keys = m_throttles.keySet().iterator();
            while (keys.hasNext()) {
                String key = keys.next();
                if (!defined.contains(key)) {
                    keys.remove();
                }
            }
        }
        //
        // Rules
        //
        {
            JsonArray rules = config().getArray(CONF_KEY_RULES, new JsonArray());
            Preconditions.checkState(rules.size() > 0, "No rules have been defined");
            for (int i = 0; i < rules.size(); i++) {
                JsonObject ruleDef = rules.get(i);
                String id = ruleDef.getString(CONF_KEY_ID, "rule-" + (i + 1));
                Rule rule = Rule.create(id, ruleDef);
                m_rules.add(rule);
                Preconditions.checkState(m_throttles.containsKey(rule.getAction()),
                        "rule \"" + rule.getId() + "\" is referencing a missing throttle: "
                        + rule.getAction());
            }
        }

        // Discard events on mismatch (true by default)
        m_discardOnMismatch = config().getBoolean(CONF_KEY_DISCARD_ON_MISMATCH, true);

        // Start throttle cleaner
        m_timerId = vertx.setPeriodic(30000, new ThrottleCleaner(m_throttles));
    }

    @Override
    protected void stopFilter() {
        // Stop throttle cleaner
        vertx.cancelTimer(m_timerId);
        // Close database
        if (!m_db.isClosed()) {
            m_db.commit();
            m_db.close();
        }
    }

    @Override
    protected void handleEvent(final JsonObject event) {
        //
        // Find matching rule
        //
        boolean match = false;
        String throttle = "";
        DateTime now = null;
        String timezone = "";
        boolean discardOnMismatch = m_discardOnMismatch;
        Map<String, CronEntry> schedules = m_schedules;
        List<Rule> rules = m_rules;
        for (Rule rule : rules) {
            //
            // Matching schedule?
            //
            CronEntry entry = schedules.get(rule.getSchedule());
            if (!timezone.equals(entry.getTimezone())) {
                timezone = entry.getTimezone();
                now = DateTime.now(DateTimeZone.forID(timezone));
            }
            if (entry.isDefined(now)) {

                // schedule matched, now match against rule
                throttle = rule.getAction();

                try {
                    // Match against tag and/or field value
                    logger().trace("Evaluating rule: {}", rule);
                    match = rule.match(event);
                } catch (NumberFormatException e) {
                    logger().error("Failed to match rule", e);
                    match = false;
                }
            }
        }
        // Throttle only if we had a match
        if (match) {
            throttleEvent(event, throttle);
        } else {
            if (!discardOnMismatch) {
                emitEvent(event);
            }
        }
    }

    private void throttleEvent(
            final JsonObject event,
            final String throttle) {

        logger().trace("Limiting rate of event: {} throttle: {}", event, throttle);
        Throttle thr = m_throttles.get(throttle);
        if (thr != null) {
            //
            // Check if we should use a checksum throttle instead
            //
            String id = thr.getId();
            if (thr.hasChecksumField()) {
                id = thr.resolveId(event);
                if (m_throttles.containsKey(id)) {
                    thr = m_throttles.get(id);
                } else {
                    // Nada found, create a new one
                    thr = Throttle.create(id, thr);
                }
            }
            //
            // Test for rate limiting and update throttle state (if emitting allowed)
            //
            if (thr.allowEmit()) {
                thr = thr.update();
                emitEvent(event);
            }
            m_throttles.put(id, thr);
        }
    }

    private static final class ThrottleCleaner implements Handler<Long> {

        private final Map<String, Throttle> m_throttles; // throttle-id => throttle
        private final Logger m_logger = LoggerFactory.getLogger(ThrottleCleaner.class);

        private ThrottleCleaner(Map<String, Throttle> throttles) {
            m_throttles = throttles;
        }

        @Override
        public void handle(Long timerId) {
            //
            // Remove old checksum throttles
            //
            long now = System.currentTimeMillis();
            Iterator<String> keys = m_throttles.keySet().iterator();
            while (keys.hasNext()) {
                String key = keys.next();
                Throttle thr = m_throttles.get(key);
                if (thr.hasChecksumField()
                        && thr.hasExpired(now)) {
                    m_logger.debug("Removing expired checksum throttle: {}", key);
                    keys.remove();
                }
            }
        }
    }
}
