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
package io.spikex.filter.unit;

import com.google.common.util.concurrent.Uninterruptibles;
import java.io.File;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import junit.framework.Assert;
import io.spikex.filter.integration.EventCreator;
import io.spikex.filter.internal.Throttle;
import org.junit.Test;
import org.mapdb.DB;
import org.mapdb.DBMaker;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.vertx.java.core.json.JsonObject;

/**
 * Throttle tester.
 *
 * @author cli
 */
public class ThrottleTest {

    private final Logger m_logger = LoggerFactory.getLogger(ThrottleTest.class);

    @Test
    public void testOnePerSecond() {
        JsonObject config = new JsonObject();
        config.putNumber("rate", 1);
        config.putNumber("interval", 1);
        config.putString("unit", "sec");

        Throttle t1 = Throttle.create("one-per-second", config);

        Assert.assertTrue("Throttle did not allow emit immediately after creation",
                t1.allowEmit());
        Assert.assertFalse("Throttle did not limit rate after first emit",
                t1.allowEmit());
        Assert.assertFalse("Throttle expired after first emit",
                t1.hasExpired(System.currentTimeMillis()));
        Assert.assertFalse("Throttle did not limit rate",
                t1.allowEmit());

        // Sleep just over 1 sec
        Uninterruptibles.sleepUninterruptibly(1500L, TimeUnit.MILLISECONDS);

        Assert.assertTrue("Throttle did not allow emit after >1 sec wait",
                t1.allowEmit());
        Assert.assertFalse("Throttle did not limit rate after second emit",
                t1.allowEmit());
        Assert.assertFalse("Throttle did not limit rate",
                t1.allowEmit());

        // Sleep under 1 sec
        Uninterruptibles.sleepUninterruptibly(500L, TimeUnit.MILLISECONDS);

        Assert.assertFalse("Throttle did not limit rate",
                t1.allowEmit());

        // Sleep just over 3 secs (expiry => 3 * interval)
        Uninterruptibles.sleepUninterruptibly(3500L, TimeUnit.MILLISECONDS);

        Assert.assertTrue("Throttle did not expire after long wait",
                t1.hasExpired(System.currentTimeMillis()));
    }

    @Test
    public void testUpdateAndChecksum() {
        JsonObject config = new JsonObject();
        config.putNumber("rate", 3); // Allow three messages per second
        config.putNumber("interval", 1);
        config.putString("unit", "sec");
        config.putString("checksum-field", "@message");

        Throttle t1 = Throttle.create("three-per-second", config);
        Assert.assertTrue("Throttle did not allow emit immediately after creation",
                t1.allowEmit());
        t1 = t1.update(); // Update throttle state

        Assert.assertTrue("Second emit disallowed", t1.allowEmit());
        t1 = t1.update(); // Update throttle state

        Uninterruptibles.sleepUninterruptibly(150L, TimeUnit.MILLISECONDS);

        Assert.assertTrue("Third emit disallowed", t1.allowEmit());
        t1 = t1.update(); // Update throttle state

        Uninterruptibles.sleepUninterruptibly(150L, TimeUnit.MILLISECONDS);

        Assert.assertFalse("Throttle did not limit rate after third emit",
                t1.allowEmit());
        t1 = t1.update(); // Update throttle state

        Assert.assertFalse("Throttle expired after first emit",
                t1.hasExpired(System.currentTimeMillis()));
        Assert.assertFalse("Throttle did not limit rate",
                t1.allowEmit());
        t1 = t1.update(); // Update throttle state

        JsonObject event = EventCreator.create("Throttle");
        String id = t1.resolveId(event);
        m_logger.info("Created throttle identifier: {}", id);

        // Sleep just over 1 sec
        Uninterruptibles.sleepUninterruptibly(1500L, TimeUnit.MILLISECONDS);

        Assert.assertTrue("Throttle did not allow emit after >1 sec wait",
                t1.allowEmit());
        t1 = t1.update(); // Update throttle state

        Uninterruptibles.sleepUninterruptibly(200L, TimeUnit.MILLISECONDS);

        Assert.assertTrue("Second emit disallowed", t1.allowEmit());
        t1 = t1.update(); // Update throttle state

        Assert.assertTrue("Third emit disallowed", t1.allowEmit());
        t1 = t1.update(); // Update throttle state

        Assert.assertFalse("Throttle did not limit rate after third emit",
                t1.allowEmit());

        // Sleep just over 1 sec
        Uninterruptibles.sleepUninterruptibly(1500L, TimeUnit.MILLISECONDS);

        Assert.assertTrue("Throttle did not allow emit after >1 sec wait",
                t1.allowEmit());
    }

    @Test
    public void testSerialization() {
        JsonObject config = new JsonObject();
        config.putNumber("rate", 100); // Allow 100 messages per five minutes
        config.putNumber("interval", 5);
        config.putString("unit", "min");
        Throttle t1 = Throttle.create("100-per-5-min", config);

        // "emit" 52 messages in a burst
        for (int i = 0; i < 52; i++) {
            Assert.assertTrue("Throttle did not allow emit", t1.allowEmit());
            t1 = t1.update(); // Update throttle state
        }

        //
        // Save throttle to disk (serialize)
        //
        String mapName = "map-throttles";
        String filename = "throttle-100-per-5-min.bin";
        File file = new File("build", filename);
        DB db = DBMaker.newFileDB(file)
                .checksumEnable()
                .compressionEnable()
                .make();

        Throttle.MapDbSerializer serializer = new Throttle.MapDbSerializer();
        Map<String, Throttle> throttles = db.createHashMap(mapName)
                .valueSerializer(serializer)
                .makeOrGet();

        throttles.put(t1.getId(), t1);
        db.commit();
        db.close();

        //
        // Read throttle from disk (de-serialize)
        //
        db = DBMaker.newFileDB(file)
                .closeOnJvmShutdown()
                .deleteFilesAfterClose()
                .checksumEnable()
                .compressionEnable()
                .make();

        throttles = db.createHashMap(mapName)
                .valueSerializer(serializer)
                .makeOrGet();

        Throttle t2 = throttles.get(t1.getId());
        Assert.assertEquals(48L, t2.getCurrentBucketSize());
    }
}
