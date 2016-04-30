/**
 *
 * Copyright (c) 2016 NG Modular Oy.
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
import io.spikex.core.AbstractFilter;
import io.spikex.core.helper.Events;
import static io.spikex.core.helper.Events.DSTIME_PRECISION_MILLIS;
import static io.spikex.core.helper.Events.DSTYPE_GAUGE;
import static io.spikex.core.helper.Events.EVENT_FIELD_TAGS;
import static io.spikex.core.helper.Events.EVENT_FIELD_UNIT;
import static io.spikex.core.helper.Events.TIMEZONE_UTC;
import io.spikex.core.util.HostOs;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.vertx.java.core.Handler;
import org.vertx.java.core.eventbus.EventBus;
import org.vertx.java.core.json.JsonArray;
import org.vertx.java.core.json.JsonObject;

/**
 *
 * @author cli
 */
public final class ThingseeHandler implements Handler<HttpResponse> {

    private final AbstractFilter m_filter;
    private final JsonObject m_config;
    private final EventBus m_eventBus;
    private final JsonArray m_tags;

    private static final String EVENT_FIELD_SENSES = "senses";
    private static final String EVENT_FIELD_ENGINE = "engine";
    private static final String EVENT_FIELD_SENSOR_ID = "sId";
    private static final String EVENT_FIELD_SENSOR_VALUE = "val";
    private static final String EVENT_FIELD_SENSOR_TIMESTAMP = "ts";
    private static final String EVENT_FIELD_ENGINE_PID = "pId";
    private static final String EVENT_FIELD_ENGINE_PUID = "puId";
    private static final String EVENT_FIELD_ENGINE_EVID = "evId";
    private static final String EVENT_FIELD_ENGINE_STID = "stId";

    private static final String DSNAME_LOCATION_LATITUDE = "thingsee.location.latitude";
    private static final String DSNAME_LOCATION_LONGITUDE = "thingsee.location.longitude";
    private static final String DSNAME_LOCATION_ALTITUDE = "thingsee.location.altitude";
    private static final String DSNAME_LOCATION_ACCURACY = "thingsee.location.accuracy";
    private static final String DSNAME_LOCATION_GEOFENCE = "thingsee.location.geofence";
    private static final String DSNAME_SPEED_GPS = "thingsee.speed.gps";
    private static final String DSNAME_BATTERY_LEVEL = "thingsee.battery.level";
    private static final String DSNAME_ENVIRONMENT_TEMPERATURE = "thingsee.environment.temperature";
    private static final String DSNAME_ENVIRONMENT_HUMIDITY = "thingsee.environment.humidity";
    private static final String DSNAME_ENVIRONMENT_AMBIENT_LIGHT = "thingsee.environment.ambient.light";
    private static final String DSNAME_ENVIRONMENT_AIR_PRESSURE = "thingsee.environment.air.pressure";

    private static final String TAG_LOCATION = "location";
    private static final String TAG_ENVIRONMENT = "environment";
    private static final String TAG_SPEED = "speed";
    private static final String TAG_BATTERY = "battery";

    private static final String INSTANCE_LATITUDE = "latitude";
    private static final String INSTANCE_LONGITUDE = "longitude";
    private static final String INSTANCE_ALTITUDE = "altitude";
    private static final String INSTANCE_ACCURACY = "accuracy";
    private static final String INSTANCE_GEOFENCE = "geofence";
    private static final String INSTANCE_TEMPERATURE = "temperature";
    private static final String INSTANCE_HUMIDITY = "humidity";
    private static final String INSTANCE_AMBIENT_LIGHT = "ambient-light";
    private static final String INSTANCE_AIR_PRESSURE = "air-pressure";

    private static final String UNIT_DEGREES = "degrees";
    private static final String UNIT_METERS = "meters";
    private static final String UNIT_MS = "m/s";
    private static final String UNIT_PERCENT = "%";
    private static final String UNIT_CELCIUS = "C";
    private static final String UNIT_LUX = "lux";
    private static final String UNIT_AIR_PRESSURE = "hPa";

    private final Logger m_logger = LoggerFactory.getLogger(ThingseeHandler.class);

    public ThingseeHandler(
            final AbstractFilter filter,
            final JsonObject config,
            final EventBus eventBus,
            final JsonArray tags) {

        m_filter = filter;
        m_config = config;
        m_eventBus = eventBus;
        m_tags = tags;
    }

    @Override
    public void handle(final HttpResponse response) {

        // Try to parse json and emit new event
        String body = response.getBody();
        if (!Strings.isNullOrEmpty(body)) {

            JsonArray dataArray = new JsonArray(body);
            if (dataArray.size() > 0) {
                JsonObject data = (JsonObject) dataArray.get(0);

                // [
                //   {
                //     "engine":{"pId":"571bd76bf8516409368e30d3","puId":1,"stId":1,"evId":0,"ts":1461444804871},
                //     "senses":[
                //                {"sId":"0x00010100","val":60.1971855164,"ts":1461444804871},
                //                {"sId":"0x00010200","val":24.6812019348,"ts":1461444804871},
                //                {"sId":"0x00010300","val":32,"ts":1461444804871}
                //              ]
                //   }
                // ]
                JsonArray senses = data.getArray(EVENT_FIELD_SENSES, new JsonArray());
                for (int i = 0; i < senses.size(); i++) {

                    JsonObject sense = senses.get(i);
                    String sensorId = sense.getString(EVENT_FIELD_SENSOR_ID);
                    Object sensorValue = sense.getValue(EVENT_FIELD_SENSOR_VALUE);
                    long sensorTimestamp = sense.getLong(EVENT_FIELD_SENSOR_TIMESTAMP,
                            System.currentTimeMillis());
                    String dsname;
                    String subgroup;
                    String unit = "";
                    String instance = "";
                    JsonArray tags = new JsonArray();
                    for (int j = 0; m_tags != null && j < m_tags.size(); j++) {
                        tags.add(m_tags.get(j));
                    }

                    switch (sensorId) {
                        // Location, Latitude (degrees)
                        case "0x00010100":
                            dsname = DSNAME_LOCATION_LATITUDE;
                            unit = UNIT_DEGREES;
                            subgroup = TAG_LOCATION;
                            instance = INSTANCE_LATITUDE;
                            tags.addString(TAG_LOCATION);
                            break;
                        // Location, Longitude (degrees)
                        case "0x00010200":
                            dsname = DSNAME_LOCATION_LONGITUDE;
                            unit = UNIT_DEGREES;
                            subgroup = TAG_LOCATION;
                            instance = INSTANCE_LONGITUDE;
                            tags.addString(TAG_LOCATION);
                            break;
                        // Location, Altitude (meters)
                        case "0x00010300":
                            dsname = DSNAME_LOCATION_ALTITUDE;
                            unit = UNIT_METERS;
                            subgroup = TAG_LOCATION;
                            instance = INSTANCE_ALTITUDE;
                            tags.addString(TAG_LOCATION);
                            break;
                        // Location, Accuracy (meters)
                        case "0x00010400":
                            dsname = DSNAME_LOCATION_ACCURACY;
                            unit = UNIT_METERS;
                            subgroup = TAG_LOCATION;
                            instance = INSTANCE_ACCURACY;
                            tags.addString(TAG_LOCATION);
                            break;
                        // Location, is inside geofence (boolean)
                        case "0x00010500":
                            dsname = DSNAME_LOCATION_GEOFENCE;
                            subgroup = TAG_LOCATION;
                            instance = INSTANCE_GEOFENCE;
                            tags.addString(TAG_LOCATION);
                            break;
                        // GPS speed (m/s)
                        case "0x00020100":
                            dsname = DSNAME_SPEED_GPS;
                            unit = UNIT_MS;
                            subgroup = TAG_SPEED;
                            tags.addString(TAG_SPEED);
                            break;
                        // Current battery level (%)
                        case "0x00030200":
                            dsname = DSNAME_BATTERY_LEVEL;
                            unit = UNIT_PERCENT;
                            subgroup = TAG_BATTERY;
                            tags.addString(TAG_BATTERY);
                            break;
                        // Environment, Temperature (Celcius)
                        case "0x00060100":
                            dsname = DSNAME_ENVIRONMENT_TEMPERATURE;
                            unit = UNIT_CELCIUS;
                            subgroup = TAG_ENVIRONMENT;
                            instance = INSTANCE_TEMPERATURE;
                            tags.addString(TAG_ENVIRONMENT);
                            break;
                        // Environment, Humidity (%)
                        case "0x00060200":
                            dsname = DSNAME_ENVIRONMENT_HUMIDITY;
                            unit = UNIT_PERCENT;
                            subgroup = TAG_ENVIRONMENT;
                            instance = INSTANCE_HUMIDITY;
                            tags.addString(TAG_ENVIRONMENT);
                            break;
                        // Environment, Ambient light (lux)
                        case "0x00060300":
                            dsname = DSNAME_ENVIRONMENT_AMBIENT_LIGHT;
                            unit = UNIT_LUX;
                            subgroup = TAG_ENVIRONMENT;
                            instance = INSTANCE_AMBIENT_LIGHT;
                            tags.addString(TAG_ENVIRONMENT);
                            break;
                        // Environment, Air pressure (hPa)
                        case "0x00060400":
                            dsname = DSNAME_ENVIRONMENT_AIR_PRESSURE;
                            unit = UNIT_AIR_PRESSURE;
                            subgroup = TAG_ENVIRONMENT;
                            instance = INSTANCE_AIR_PRESSURE;
                            tags.addString(TAG_ENVIRONMENT);
                            break;
                        // Timer, Unix time (s)
                        case "0x00080100":
                            String timestamp = sense.getString(EVENT_FIELD_SENSOR_VALUE);
                            m_logger.info("Received timer event: {}", timestamp);
                            continue;
                        default:
                            m_logger.error("Ignoring unsupported sensor: {}", sensorId);
                            continue;
                    }

                    m_logger.debug("Received {}: {}", dsname, sensorValue);

                    JsonObject event = Events.createMetricEvent(
                            m_filter,
                            sensorTimestamp,
                            TIMEZONE_UTC,
                            HostOs.hostName(),
                            dsname,
                            DSTYPE_GAUGE,
                            DSTIME_PRECISION_MILLIS,
                            subgroup,
                            instance,
                            0,
                            sensorValue);

                    // Unit (if any)
                    event.putString(EVENT_FIELD_UNIT, unit);

                    // Thingsee specific fields
                    JsonObject engine = data.getObject(EVENT_FIELD_ENGINE, new JsonObject());
                    event.putString(EVENT_FIELD_ENGINE_PID, engine.getString(EVENT_FIELD_ENGINE_PID, ""));
                    event.putNumber(EVENT_FIELD_ENGINE_PUID, engine.getNumber(EVENT_FIELD_ENGINE_PUID));
                    event.putNumber(EVENT_FIELD_ENGINE_EVID, engine.getNumber(EVENT_FIELD_ENGINE_EVID));
                    event.putNumber(EVENT_FIELD_ENGINE_STID, engine.getNumber(EVENT_FIELD_ENGINE_STID));

                    // Add tags
                    event.putArray(EVENT_FIELD_TAGS, tags);

                    String destAddr = m_filter.getDestinationAddress();
                    if (destAddr != null && destAddr.length() > 0) {
                        m_eventBus.publish(destAddr, event);
                    }
                }
            }
        } else {
            m_logger.warn("Ignoring empty request body");
        }
    }
}
