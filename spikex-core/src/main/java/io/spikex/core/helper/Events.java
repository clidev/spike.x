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
package io.spikex.core.helper;

import com.eaio.uuid.UUID;
import com.google.common.base.Preconditions;
import io.spikex.core.AbstractFilter;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.List;
import org.vertx.java.core.json.JsonArray;
import org.vertx.java.core.json.JsonObject;

/**
 * JsonObject event factory and validator. Use this class to create new Spike.x
 * compliant events. Short string fields should not be over 256 characters.
 *
 * TODO Check that this class is used consistently throughout Spike.x TODO Move
 * this class to utils
 *
 * @author cli
 */
public final class Events {

    /**
     * Event identifier (UUID)
     */
    public static final String EVENT_FIELD_ID = "@id";

    /**
     * The host identifier - free form string (short string)
     */
    public static final String EVENT_FIELD_HOST = "@host";

    /**
     * The source of the event - free form string (short string)
     */
    public static final String EVENT_FIELD_SOURCE = "@source";

    /**
     * Event timestamp - the number of milliseconds that have elapsed since
     * 00:00:00 Coordinated Universal Time (UTC), Thursday, 1 January 1970, not
     * counting leap seconds.
     */
    public static final String EVENT_FIELD_TIMESTAMP = "@timestamp";

    /**
     * Event timezone - by default UTC
     */
    public static final String EVENT_FIELD_TIMEZONE = "@timezone";

    /**
     * The type of the event - must be one of the predefined event types
     */
    public static final String EVENT_FIELD_TYPE = "@type";

    /**
     * The event tags - free form list of strings
     */
    public static final String EVENT_FIELD_TAGS = "@tags";

    /**
     * The event chain - chain name of event
     */
    public static final String EVENT_FIELD_CHAIN = "@chain";

    /**
     * The event priority or severity - must be on of the pre-defined priorities
     */
    public static final String EVENT_FIELD_PRIORITY = "@priority";

    /**
     * The notification event title or subject - free form string
     */
    public static final String EVENT_FIELD_TITLE = "@title";

    /**
     * The notification event message - free form string
     */
    public static final String EVENT_FIELD_MESSAGE = "@message";

    /**
     * The notification event destinations - list of destination addresses
     */
    public static final String EVENT_FIELD_DESTINATIONS = "@destinations";

    /**
     * The metric event value
     */
    public static final String EVENT_FIELD_VALUE = "@value";

    /**
     * The metric event datasource name
     */
    public static final String EVENT_FIELD_DSNAME = "@dsname";

    /**
     * The metric event datasource type - must be one of the pre-defined types
     */
    public static final String EVENT_FIELD_DSTYPE = "@dstype";

    /**
     * The metric event datasource timestamp precision - must be one of the
     * pre-defined types
     */
    public static final String EVENT_FIELD_DSTIME_PRECISION = "@dsprecision";

    /**
     * The metric event sampling interval in milliseconds - use "-" if not
     * applicable
     */
    public static final String EVENT_FIELD_INTERVAL = "@interval";

    /**
     * The metric event data instance - use "-" if not applicable
     */
    public static final String EVENT_FIELD_INSTANCE = "@instance";

    /**
     * The metric event data subgroup - use "-" if not applicable
     */
    public static final String EVENT_FIELD_SUBGROUP = "@subgroup";

    /**
     * The metric event unit (optional)
     */
    public static final String EVENT_FIELD_UNIT = "@unit";

    /**
     * The list of events in a batch
     */
    public static final String EVENT_FIELD_BATCH_EVENTS = "@batch-events";

    /**
     * Batch size for batch events
     */
    public static final String EVENT_FIELD_BATCH_SIZE = "@batch-size";

    //
    // Supported event types
    //
    /**
     * The type used for events that carry sensor or performance data.
     */
    public static final String EVENT_TYPE_METRIC = "metric";

    /**
     * The type used for events that carry messages (alerts, warnings, etc..)
     */
    public static final String EVENT_TYPE_NOTIFICATION = "notification";

    /**
     * The type used for events that contain other events
     */
    public static final String EVENT_TYPE_BATCH = "batch";

    //
    // Supported datasource types
    //
    public static final String DSTYPE_GAUGE = "GAUGE";
    public static final String DSTYPE_COUNTER = "COUNTER";
    public static final String DSTYPE_STRING = "STRING";

    //
    // Supported datasource timestamp precisions (InfluxDB compatible)
    //
    public static final String DSTIME_PRECISION_NANOS = "n";
    public static final String DSTIME_PRECISION_MICROS = "u";
    public static final String DSTIME_PRECISION_MILLIS = "ms"; // Default in Spike.x
    public static final String DSTIME_PRECISION_SEC = "s";
    public static final String DSTIME_PRECISION_MIN = "m";
    public static final String DSTIME_PRECISION_HOUR = "h";

    //
    // Supported event priorities
    //
    public static final String EVENT_PRIORITY_LOW = "low";
    public static final String EVENT_PRIORITY_NORMAL = "normal";
    public static final String EVENT_PRIORITY_HIGH = "high";

    public static final String SPIKEX_ORIGIN_TAG = "spikex";

    //
    // UTC timezone
    //
    public static final ZoneId TIMEZONE_UTC = ZoneId.of("UTC");

    /**
     * Creates a new metric event based on the given filter using the local
     * system timezone. The timestamp is taken from System.currentTimeMillis().
     * The timezone is UTC and timestamp precision is in milliseconds.
     *
     * @param filter the filter that created the event
     * @param host the event host
     * @param dsname the datasource name (system.memory, process.name, etc.)
     * @param dstype the datasource type (GAUGE, COUNTER, STRING)
     * @param subgroup the data subgroup (used, free, etc.)
     * @param instance the data instance (cpu0, cpu1, /mnt, etc.)
     * @param interval the sampling interval in milliseconds
     * @param value the value of the metric
     * <p>
     * @return the new event
     */
    public static JsonObject createMetricEvent(
            final AbstractFilter filter,
            final String host,
            final String dsname,
            final String dstype,
            final String subgroup,
            final String instance,
            final long interval,
            final Object value) {

        return createMetricEvent(
                filter,
                System.currentTimeMillis(),
                TIMEZONE_UTC,
                host,
                dsname,
                dstype,
                DSTIME_PRECISION_MILLIS,
                subgroup,
                instance,
                interval,
                value);
    }

    /**
     * Creates a new metric event based on the given filter.
     *
     * @param filter the filter that created the event
     * @param timestamp the event timestamp
     * @param timezone the timezone to use for the timestamp
     * @param host the event host
     * @param dsname the datasource name (system.memory, process.name, etc.)
     * @param dstype the datasource type (GAUGE, COUNTER, STRING)
     * @param dsprecision the timestamp precision (ns, us, ms, s, m or h)
     * @param subgroup the data subgroup (used, free, etc.)
     * @param instance the data instance (cpu0, cpu1, /mnt, etc.)
     * @param interval the sampling interval in milliseconds
     * @param value the value of the metric
     * <p>
     * @return the new event
     */
    public static JsonObject createMetricEvent(
            final AbstractFilter filter,
            final long timestamp,
            final ZoneId timezone,
            final String host,
            final String dsname,
            final String dstype,
            final String dsprecision,
            final String subgroup,
            final String instance,
            final long interval,
            final Object value) {

        Preconditions.checkNotNull(filter);
        Preconditions.checkNotNull(timezone);
        Preconditions.checkNotNull(host);
        Preconditions.checkNotNull(dsname);
        Preconditions.checkNotNull(dstype);
        Preconditions.checkNotNull(dsprecision);
        Preconditions.checkNotNull(value);

        JsonObject event = new JsonObject();
        event.putString(EVENT_FIELD_ID, new UUID().toString());
        event.putString(EVENT_FIELD_SOURCE, filter.getName());
        event.putNumber(EVENT_FIELD_TIMESTAMP, timestamp);
        event.putString(EVENT_FIELD_TIMEZONE, timezone.getId());
        event.putString(EVENT_FIELD_TYPE, EVENT_TYPE_METRIC);
        event.putString(EVENT_FIELD_CHAIN, filter.getChainName());
        event.putString(EVENT_FIELD_PRIORITY, EVENT_PRIORITY_NORMAL); // Default
        event.putString(EVENT_FIELD_HOST, host);
        event.putString(EVENT_FIELD_DSNAME, dsname);
        event.putString(EVENT_FIELD_DSTYPE, dstype);
        event.putString(EVENT_FIELD_DSTIME_PRECISION, dsprecision);
        event.putString(EVENT_FIELD_SUBGROUP, subgroup);
        event.putString(EVENT_FIELD_INSTANCE, instance);
        event.putNumber(EVENT_FIELD_INTERVAL, interval);
        event.putValue(EVENT_FIELD_VALUE, value);
        event.putArray(EVENT_FIELD_TAGS, new JsonArray());
        return event;
    }

    /**
     * Creates a new notification event based on the given filter using the
     * local system timezone. The timestamp is taken from
     * System.currentTimeMillis(). The timezone is UTC and timestamp precision 
     * is in milliseconds.
     *
     * @param filter the filter that created the event
     * @param host the event host
     * @param priority the event priority
     * @param title the title of the notification
     * @param message the message of the notification
     * <p>
     * @return the new event
     */
    public static JsonObject createNotificationEvent(
            final AbstractFilter filter,
            final String host,
            final String priority,
            final String title,
            final String message) {

        return createNotificationEvent(
                filter,
                System.currentTimeMillis(),
                TIMEZONE_UTC,
                host,
                priority,
                title,
                message);
    }

    /**
     * Creates a new notification event based on the given filter.
     *
     * @param filter the filter that created the event
     * @param timestamp the event timestamp
     * @param timezone the timezone to use for the timestamp
     * @param host the event host
     * @param priority the event priority
     * @param title the title of the notification
     * @param message the message of the notification
     * <p>
     * @return the new event
     */
    public static JsonObject createNotificationEvent(
            final AbstractFilter filter,
            final long timestamp,
            final ZoneId timezone,
            final String host,
            final String priority,
            final String title,
            final String message) {

        Preconditions.checkNotNull(filter);
        Preconditions.checkNotNull(timezone);
        Preconditions.checkNotNull(host);
        Preconditions.checkNotNull(priority);
        Preconditions.checkNotNull(title);
        Preconditions.checkNotNull(message);

        JsonObject event = new JsonObject();
        event.putString(EVENT_FIELD_ID, new UUID().toString());
        event.putString(EVENT_FIELD_SOURCE, filter.getName());
        event.putNumber(EVENT_FIELD_TIMESTAMP, timestamp);
        event.putString(EVENT_FIELD_TIMEZONE, timezone.getId());
        event.putString(EVENT_FIELD_TYPE, EVENT_TYPE_NOTIFICATION);
        event.putString(EVENT_FIELD_CHAIN, filter.getChainName());
        event.putString(EVENT_FIELD_HOST, host);
        event.putString(EVENT_FIELD_PRIORITY, priority);
        event.putString(EVENT_FIELD_TITLE, title);
        event.putString(EVENT_FIELD_MESSAGE, message);
        event.putArray(EVENT_FIELD_DESTINATIONS, new JsonArray());
        event.putArray(EVENT_FIELD_TAGS, new JsonArray());
        return event;
    }

    /**
     * Creates a new batch event based on the given filter.
     *
     * @param filter the filter that created the event
     * @param events the list of events to add to the batch
     * <p>
     * @return the new batch event
     */
    public static JsonObject createBatchEvent(
            final AbstractFilter filter,
            final List<String> events) {

        Preconditions.checkNotNull(filter);
        Preconditions.checkNotNull(events);

        JsonObject batch = new JsonObject();
        batch.putString(EVENT_FIELD_ID, new UUID().toString());
        batch.putString(EVENT_FIELD_SOURCE, filter.getName());
        batch.putNumber(EVENT_FIELD_TIMESTAMP, System.currentTimeMillis());
        batch.putString(EVENT_FIELD_TIMEZONE, ZoneId.of("UTC").getId());
        batch.putString(EVENT_FIELD_TYPE, EVENT_TYPE_BATCH);
        batch.putString(EVENT_FIELD_CHAIN, filter.getChainName());
        batch.putString(EVENT_FIELD_PRIORITY, EVENT_PRIORITY_NORMAL); // Default

        JsonArray jsonEvents = new JsonArray();
        for (String event : events) {
            jsonEvents.addObject(new JsonObject(event));
        }

        batch.putString(EVENT_FIELD_TYPE, EVENT_TYPE_BATCH);
        batch.putArray(EVENT_FIELD_BATCH_EVENTS, jsonEvents);
        batch.putNumber(EVENT_FIELD_BATCH_SIZE, events.size());
        return batch;
    }

    /**
     * Returns the event time in ISO 8601 format.
     *
     * @param event the event from which to retrieve the timestamp and timezone
     * <p>
     * @return the ISO 8601 formatted UTC timestamp
     */
    public static String timestamp(final JsonObject event) {
        long millis = event.getLong(EVENT_FIELD_TIMESTAMP);
        String zoneId = event.getString(EVENT_FIELD_TIMEZONE);
        Instant epoch = Instant.ofEpochMilli(millis);
        return ZonedDateTime.ofInstant(epoch, ZoneId.of(zoneId)).toString();
    }

    /**
     * Returns the current UTC date and time in ISO 8601 format.
     *
     * @return the ISO 8601 formatted UTC timestamp
     */
    public static String timestamp() {
        return timestamp(ZoneId.of("UTC"));
    }

    /**
     * Returns the current date and time in ISO 8601 format.
     *
     * @param timezone the timezone to use for the timestamp
     * <p>
     * @return the ISO 8601 formatted timestamp
     */
    public static String timestamp(final ZoneId timezone) {
        ZonedDateTime now = ZonedDateTime.now(timezone);
        return now.toString();
    }
}
