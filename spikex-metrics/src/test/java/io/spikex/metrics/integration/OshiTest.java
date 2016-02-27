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
package io.spikex.metrics.integration;

import static io.spikex.core.AbstractFilter.CONF_KEY_CHAIN_NAME;
import static io.spikex.core.AbstractVerticle.CONF_KEY_CLUSTER_NAME;
import static io.spikex.core.AbstractVerticle.CONF_KEY_CONF_PATH;
import static io.spikex.core.AbstractVerticle.CONF_KEY_DATA_PATH;
import static io.spikex.core.AbstractVerticle.CONF_KEY_HOME_PATH;
import static io.spikex.core.AbstractVerticle.CONF_KEY_LOCAL_ADDRESS;
import static io.spikex.core.AbstractVerticle.CONF_KEY_NODE_NAME;
import static io.spikex.core.AbstractVerticle.CONF_KEY_TMP_PATH;
import static io.spikex.core.AbstractVerticle.CONF_KEY_USER;
import io.spikex.core.util.HostOs;
import io.spikex.metrics.Activator;
import java.util.Arrays;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.vertx.java.core.AsyncResult;
import org.vertx.java.core.AsyncResultHandler;
import org.vertx.java.core.json.JsonObject;
import org.vertx.testtools.TestVerticle;
import org.vertx.testtools.VertxAssert;
import oshi.SystemInfo;
import oshi.hardware.HardwareAbstractionLayer;
import oshi.hardware.Processor;
import oshi.software.os.OSFileStore;
import oshi.util.FormatUtil;
import oshi.util.Util;

/**
 *
 * @author cli
 */
public class OshiTest extends TestVerticle {

    private static final Logger m_logger = LoggerFactory.getLogger(OshiTest.class);

    @Test
    public void testSystemInfo() {

        // OSHI supports Windows, OS X and Linux
        if (HostOs.isWindows() || HostOs.isMac() || HostOs.isLinux()) {

            // Operating system
            SystemInfo si = new SystemInfo();
            m_logger.info("{}", si.getOperatingSystem());

            // Hardware: processors
            HardwareAbstractionLayer hal = si.getHardware();
            for (Processor cpu : hal.getProcessors()) {
                m_logger.info("{}", cpu);
            }

            // Hardware: memory
            m_logger.info("Available mem: {}", hal.getMemory().getAvailable());
            m_logger.info("Total mem: {}", hal.getMemory().getTotal());

            // Uptime
            m_logger.info("Uptime: {}", hal.getProcessors()[0].getSystemUptime());

            // Resolve ticks per second
            outputSystemCpuLoad(hal, 1000L);

            // Resolve ticks per 3 seconds
            outputSystemCpuLoad(hal, 3000L);

            // Per CPU load
            outputProcessorLoad(hal, 1000L);
            
            m_logger.info("CPU load: {} (ticks)",
                    hal.getProcessors()[0].getSystemCpuLoadBetweenTicks() * 100d);
            m_logger.info("CPU load: {} (OS MXBean)",
                    hal.getProcessors()[0].getSystemCpuLoad() * 100d);

            double loadAvg = hal.getProcessors()[0].getSystemLoadAverage();
            m_logger.info("CPU load average: {}", (loadAvg < 0 ? "NA" : loadAvg));

            // Hardware: filesystem
            for (OSFileStore fs : hal.getFileStores()) {
                m_logger.info("{} ({}) - used: {} free: {} total: {}",
                        fs.getName(),
                        fs.getDescription(),
                        FormatUtil.formatBytes(fs.getTotalSpace() - fs.getUsableSpace()),
                        FormatUtil.formatBytes(fs.getUsableSpace()),
                        FormatUtil.formatBytes(fs.getTotalSpace()));
            }
        }

        VertxAssert.testComplete();
    }

    @Test
    public void testActivator() {

        JsonObject config = createBaseConfig();

        container.deployWorkerVerticle(Activator.class.getName(), config, 1, false,
                new AsyncResultHandler<String>() {
                    @Override
                    public void handle(final AsyncResult<String> ar) {
                        if (ar.succeeded()) {

                            // Stop test
                            VertxAssert.testComplete();

                        } else {
                            m_logger.error("Failed to deploy verticle", ar.cause());
                            Assert.fail();
                        }
                    }
                });
    }

    private void outputSystemCpuLoad(
            final HardwareAbstractionLayer hal,
            final long sleepTime) {

        long[] ticks0 = hal.getProcessors()[0].getSystemCpuLoadTicks();
        m_logger.info("CPU ticks @0: {}", Arrays.toString(ticks0));
        Util.sleep(sleepTime); // Sleep n seconds
        long[] ticks1 = hal.getProcessors()[0].getSystemCpuLoadTicks();
        m_logger.info("CPU ticks @1: {}", Arrays.toString(ticks1));

        long user = ticks1[0] - ticks0[0]; // user ticks
        long nice = ticks1[1] - ticks0[1]; // nice ticks
        long sys = ticks1[2] - ticks0[2]; // sys ticks
        long idle = ticks1[3] - ticks0[3]; // idle ticks
        long total = user + nice + sys + idle;

        m_logger.info("User: {} Nice: {} System: {} Idle: {} (tick period: {})",
                100d * user / total,
                100d * nice / total,
                100d * sys / total,
                100d * idle / total,
                sleepTime);
    }

    private void outputProcessorLoad(
            final HardwareAbstractionLayer hal,
            final long sleepTime) {

        Processor[] cpus = hal.getProcessors();

        // Initial values
        long[][] prevCpuTicks = new long[cpus.length][4];
        for (int i = 0; i < cpus.length; i++) {
            long[] cpuTicks = cpus[i].getProcessorCpuLoadTicks();
            prevCpuTicks[i][0] = cpuTicks[0];
            prevCpuTicks[i][1] = cpuTicks[1];
            prevCpuTicks[i][2] = cpuTicks[2];
            prevCpuTicks[i][3] = cpuTicks[3];
        }

        Util.sleep(sleepTime); // Sleep n seconds

        for (int i = 0; i < cpus.length; i++) {
            
            long[] cpuTicks = cpus[i].getProcessorCpuLoadTicks();

            long user = Math.abs(prevCpuTicks[i][0] - cpuTicks[0]);
            long nice = Math.abs(prevCpuTicks[i][1] - cpuTicks[1]);
            long sys = Math.abs(prevCpuTicks[i][2] - cpuTicks[2]);
            long idle = Math.abs(prevCpuTicks[i][3] - cpuTicks[3]);
            long total = user + nice + sys + idle;

            m_logger.info("CPU {} => User: {} Nice: {} System: {} Idle: {} (tick period: {})",
                    i,
                    100d * user / total,
                    100d * nice / total,
                    100d * sys / total,
                    100d * idle / total,
                    sleepTime);
        }
    }

    private JsonObject createBaseConfig() {
        JsonObject config = new JsonObject();
        config.putString(CONF_KEY_CHAIN_NAME, "ohsi-test");
        config.putString(CONF_KEY_LOCAL_ADDRESS, "my-local-address");
        config.putString(CONF_KEY_NODE_NAME, "node-name");
        config.putString(CONF_KEY_CLUSTER_NAME, "cluster-name");
        config.putString(CONF_KEY_HOME_PATH, "build");
        config.putString(CONF_KEY_CONF_PATH, "build");
        config.putString(CONF_KEY_DATA_PATH, "build");
        config.putString(CONF_KEY_TMP_PATH, "build");
        config.putString(CONF_KEY_USER, "spikex");
        m_logger.info("Host operating system: {}", HostOs.operatingSystemFull());
        return config;
    }
}
