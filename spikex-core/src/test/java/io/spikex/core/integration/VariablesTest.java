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
package io.spikex.core.integration;

import junit.framework.Assert;
import static io.spikex.core.AbstractVerticle.SHARED_SENSORS_KEY;
import io.spikex.core.helper.Variables;
import org.junit.Test;
import org.vertx.java.core.json.JsonObject;
import org.vertx.java.core.shareddata.ConcurrentSharedMap;
import org.vertx.testtools.TestVerticle;
import org.vertx.testtools.VertxAssert;

/**
 * Test variables.
 *
 * @author cli
 */
public class VariablesTest extends TestVerticle {

    @Test
    public void testSharedSensorData() {

        ConcurrentSharedMap<String, Object> sensorData
                = vertx.sharedData().getMap(SHARED_SENSORS_KEY);
        JsonObject config = container.config();
        Variables vars = new Variables(config, vertx);
        sensorData.put("test.id", 10L);
        Assert.assertEquals(Long.valueOf(10L), vars.translate("%{#sensor.test.id}"));
        // Stop test
        VertxAssert.testComplete();
    }
}
