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

import com.google.common.base.Preconditions;
import io.spikex.core.util.XXHash32;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.concurrent.TimeUnit;
import org.mapdb.Serializer;
import org.vertx.java.core.json.JsonObject;

/**
 * Rate limiting algorithm implemented based on:
 * http://en.wikipedia.org/wiki/Token_bucket
 *
 * @author cli
 */
public final class Throttle {

    private final String m_id;
    private final long m_rate;
    private final long m_interval;
    private final long m_lastEmit; // milliseconds
    private final long m_lastBucketSize; // amount of tokens

    private final String m_checksumField;

    private final transient IntervalFillStrategy m_refillStrategy; // Not serialized!
    private transient long m_curBucketSize;  // Not serialized!

    private static final String CONFIG_FIELD_RATE = "rate";
    private static final String CONFIG_FIELD_INTERVAL = "interval";
    private static final String CONFIG_FIELD_UNIT = "unit";
    private static final String CONFIG_FIELD_CHECKSUM_FIELD = "checksum-field";

    private static final String UNIT_DAY = "day";
    private static final String UNIT_HOUR = "hour";
    private static final String UNIT_MIN = "min";
    private static final String UNIT_SEC = "sec";
    private static final String UNIT_MS = "ms";

    private static final int HASH_SALT = 2389231;

    private Throttle(
            final String id,
            final long rate,
            final long interval,
            final long lastEmit,
            final long lastBucketSize,
            final String checksumField) {

        m_id = id;
        m_rate = rate; // tokens
        m_interval = interval; // period
        m_lastEmit = lastEmit;
        m_lastBucketSize = lastBucketSize;
        m_checksumField = checksumField;
        m_refillStrategy = new IntervalFillStrategy(rate, interval, lastEmit);
        m_curBucketSize = lastBucketSize;
    }

    public boolean hasExpired(final long now) {

        boolean expired = false;
        long interval = getInterval() * 3;
        long lastEmit = getLastEmit();

        if (now > (lastEmit + interval)) {
            expired = true;
        }

        return expired;
    }

    public boolean hasChecksumField() {
        return (getChecksumField().length() > 0);
    }

    public String getId() {
        return m_id;
    }

    public long getRate() {
        return m_rate;
    }

    public long getInterval() {
        return m_interval;
    }

    public long getCurrentBucketSize() {
        return m_curBucketSize;
    }

    public long getLastBucketSize() {
        return m_lastBucketSize;
    }

    public long getLastEmit() {
        return m_lastEmit;
    }

    public String getChecksumField() {
        return m_checksumField;
    }

    public boolean allowEmit() {
        // Try to consume one token (success => do not limit)  
        long capacity = m_rate;
        long newTokens = Math.min(capacity, m_refillStrategy.refill());
        long curBucketSize = Math.min(m_curBucketSize + newTokens, capacity);

        boolean allow = false;
        if (curBucketSize > 0) {
            curBucketSize--;
            allow = true;
        }

        m_curBucketSize = curBucketSize;
        return allow;
    }

    public String resolveId(final JsonObject event) {

        StringBuilder id = new StringBuilder(getId());
        String checksumField = getChecksumField();
        id.append("-");
        id.append(checksumField);
        //
        // Calculate checksum
        //
        if (event.containsField(checksumField)) {
            String strValue;
            Object value = event.getValue(checksumField);
            if (value instanceof String) {
                strValue = (String) value;
            } else {
                strValue = String.valueOf(value);
            }
            id.append("-");
            id.append(XXHash32.hash(strValue, HASH_SALT));
        }
        return id.toString();
    }

    public Throttle update(final Throttle other) {
        return new Throttle(
                getId(),
                other.getRate(),
                other.getInterval(),
                getLastEmit(),
                getLastBucketSize(),
                other.getChecksumField());
    }

    public Throttle update() {
        return new Throttle(
                getId(),
                getRate(),
                getInterval(),
                m_refillStrategy.getLastEmit(),
                m_curBucketSize,
                getChecksumField());
    }

    public static Throttle create(
            final String id,
            final Throttle throttle) {

        return new Throttle(
                id,
                throttle.getRate(),
                throttle.getInterval(),
                System.currentTimeMillis() - 100L,
                0L,
                throttle.getChecksumField());
    }

    public static Throttle create(
            final String id,
            final JsonObject config) {

        Preconditions.checkArgument(config.containsField(CONFIG_FIELD_RATE),
                CONFIG_FIELD_RATE + " must be defined");

        Preconditions.checkArgument(config.containsField(CONFIG_FIELD_INTERVAL),
                CONFIG_FIELD_INTERVAL + " must be defined");

        long rate = config.getLong(CONFIG_FIELD_RATE);
        long interval = config.getLong(CONFIG_FIELD_INTERVAL);

        Preconditions.checkArgument(rate > 0, "rate must be greater than zero");
        Preconditions.checkArgument(interval > 0, "interval must be greater than zero");

        String unit = config.getString(CONFIG_FIELD_UNIT, UNIT_MS); // Default is ms
        switch (unit) {
            case UNIT_DAY:
                interval = TimeUnit.DAYS.toMillis(interval);
                break;
            case UNIT_HOUR:
                interval = TimeUnit.HOURS.toMillis(interval);
                break;
            case UNIT_MIN:
                interval = TimeUnit.MINUTES.toMillis(interval);
                break;
            case UNIT_SEC:
                interval = TimeUnit.SECONDS.toMillis(interval);
                break;
        }

        String checksumField = "";
        if (config.containsField(CONFIG_FIELD_CHECKSUM_FIELD)) {
            checksumField = config.getString(CONFIG_FIELD_CHECKSUM_FIELD);
            Preconditions.checkArgument(checksumField != null && checksumField.length() > 0,
                    CONFIG_FIELD_CHECKSUM_FIELD + " is empty");
        }

        return new Throttle(
                id,
                rate,
                interval,
                System.currentTimeMillis() - 100L,
                0L,
                checksumField);
    }

    public static class MapDbSerializer extends Serializer<Throttle> {

        @Override
        public void serialize(
                final DataOutput out,
                final Throttle throttle) throws IOException {

            out.writeUTF(throttle.getId());
            out.writeLong(throttle.getRate());
            out.writeLong(throttle.getInterval());
            out.writeLong(throttle.getLastEmit());
            out.writeLong(throttle.getLastBucketSize());
            out.writeUTF(throttle.getChecksumField());
        }

        @Override
        public Throttle deserialize(
                final DataInput in,
                int available) throws IOException {

            return new Throttle(
                    in.readUTF(),
                    in.readLong(),
                    in.readLong(),
                    in.readLong(),
                    in.readLong(),
                    in.readUTF());
        }

        @Override
        public int fixedSize() {
            return -1;
        }
    }
}
