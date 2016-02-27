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
package io.spikex.metrics.internal;

import static io.spikex.metrics.Metric.CPU_COMBINED_PERC;
import static io.spikex.metrics.Metric.CPU_IDLE_PERC;
import static io.spikex.metrics.Metric.CPU_LOAD_AVERAGE;
import static io.spikex.metrics.Metric.CPU_NICE_PERC;
import static io.spikex.metrics.Metric.CPU_SYS_PERC;
import static io.spikex.metrics.Metric.CPU_USER_PERC;
import static io.spikex.metrics.Metric.FILESYSTEM_FREE;
import static io.spikex.metrics.Metric.FILESYSTEM_TOTAL;
import static io.spikex.metrics.Metric.FILESYSTEM_USED;
import static io.spikex.metrics.Metric.LOAD_AVERAGE;
import static io.spikex.metrics.Metric.LOAD_AVERAGE_PERC;
import static io.spikex.metrics.Metric.MEM_FREE;
import static io.spikex.metrics.Metric.MEM_FREE_PERC;
import static io.spikex.metrics.Metric.MEM_TOTAL;
import static io.spikex.metrics.Metric.MEM_USED;
import static io.spikex.metrics.Metric.MEM_USED_PERC;
import static io.spikex.metrics.Metric.UPTIME;
import java.util.Map;
import oshi.SystemInfo;
import oshi.hardware.HardwareAbstractionLayer;
import oshi.hardware.Memory;
import oshi.hardware.Processor;
import oshi.software.os.OSFileStore;

/**
 *
 * @author cli
 */
public final class OshiMetrics extends AbstractMetricPublisher {

    private Processor[] m_processors;
    private long[][] m_cpuTicks;

    private final SystemInfo m_systemInfo;

    private static final String INSTANCE_CPU_PREFIX = "cpu";
    private static final String INSTANCE_TOTAL = "total";
    
    public OshiMetrics(final SystemInfo systemInfo) {
        m_systemInfo = systemInfo;
    }

    public void init() {
        m_processors = m_systemInfo.getHardware().getProcessors();
        m_cpuTicks = new long[m_processors.length][4];
        for (int i = 0; i < m_processors.length; i++) {
            long[] cpuTicks = m_processors[i].getProcessorCpuLoadTicks();
            m_cpuTicks[i][0] = cpuTicks[0];
            m_cpuTicks[i][1] = cpuTicks[1];
            m_cpuTicks[i][2] = cpuTicks[2];
            m_cpuTicks[i][3] = cpuTicks[3];
        }
    }

    public void publish(final Map<String, Object> sharedData) {

        publishCpuPerc(sharedData);
        publishCpuLoad(sharedData);
        publishMem(sharedData);
        publishLoadAverage(sharedData);
        publishUptime(sharedData);
        publishFilesystem(sharedData);
    }

    private void publishCpuPerc(final Map<String, Object> sharedData) {

        Processor[] cpus = m_processors;
        for (int i = 0; i < cpus.length; i++) {

            // User, nice, sys, idle
            long[] cpuTicks = cpus[i].getProcessorCpuLoadTicks();

            long user = Math.abs(m_cpuTicks[i][0] - cpuTicks[0]);
            long nice = Math.abs(m_cpuTicks[i][1] - cpuTicks[1]);
            long sys = Math.abs(m_cpuTicks[i][2] - cpuTicks[2]);
            long idle = Math.abs(m_cpuTicks[i][3] - cpuTicks[3]);
            long total = user + nice + sys + idle;

            if (total > 0L) {

                // User, nice, sys, idle, combined
                String instance = INSTANCE_CPU_PREFIX + i;
                publishMetric(sharedData, CPU_USER_PERC, 100.0d * user / total, instance);
                publishMetric(sharedData, CPU_NICE_PERC, 100.0d * nice / total, instance);
                publishMetric(sharedData, CPU_SYS_PERC, 100.0d * sys / total, instance);
                publishMetric(sharedData, CPU_IDLE_PERC, 100.0d * idle / total, instance);
                publishMetric(sharedData, CPU_COMBINED_PERC, 100.0d * (user + sys) / total, instance);
            }

            m_cpuTicks[i][0] = cpuTicks[0];
            m_cpuTicks[i][1] = cpuTicks[1];
            m_cpuTicks[i][2] = cpuTicks[2];
            m_cpuTicks[i][3] = cpuTicks[3];
        }
    }

    private void publishCpuLoad(final Map<String, Object> sharedData) {

        Processor[] cpus = m_processors;
        for (int i = 0; i < cpus.length; i++) {
            double cpuLoad = m_processors[i].getProcessorCpuLoadBetweenTicks();
            String instance = INSTANCE_CPU_PREFIX + i;
            publishMetric(sharedData, CPU_LOAD_AVERAGE, cpuLoad, instance);
        }
    }

    private void publishMem(final Map<String, Object> sharedData) {

        Memory mem = m_systemInfo.getHardware().getMemory();

        long free = mem.getAvailable();
        long total = mem.getTotal();
        long used = total - free;

        // Used, free, total
        publishMetric(sharedData, MEM_USED, used);
        publishMetric(sharedData, MEM_FREE, free);
        publishMetric(sharedData, MEM_TOTAL, total);

        // Used and free perc
        publishMetric(sharedData, MEM_USED_PERC, 100.0d * used / total);
        publishMetric(sharedData, MEM_FREE_PERC, 100.0d * free / total);
    }

    private void publishLoadAverage(final Map<String, Object> sharedData) {
        Processor[] cpus = m_processors;
        publishMetric(sharedData, LOAD_AVERAGE, cpus[0].getSystemLoadAverage(), INSTANCE_TOTAL);
        publishMetric(sharedData, LOAD_AVERAGE_PERC, 100.0d * cpus[0].getSystemLoadAverage(), INSTANCE_TOTAL);
    }

    private void publishUptime(final Map<String, Object> sharedData) {
        Processor[] cpus = m_processors;
        publishMetric(sharedData, UPTIME, cpus[0].getSystemUptime());
    }

    private void publishFilesystem(final Map<String, Object> sharedData) {
        HardwareAbstractionLayer hal = m_systemInfo.getHardware();
        for (OSFileStore fs : hal.getFileStores()) {

            long free = fs.getUsableSpace();
            long total = fs.getTotalSpace();
            long used = total - free;

            String instance = fs.getName() + " (" + fs.getDescription() + ")";
            publishMetric(sharedData, FILESYSTEM_USED, used, instance);
            publishMetric(sharedData, FILESYSTEM_FREE, free, instance);
            publishMetric(sharedData, FILESYSTEM_TOTAL, total, instance);
        }
    }
}
