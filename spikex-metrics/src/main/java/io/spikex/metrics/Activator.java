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
package io.spikex.metrics;

import com.codahale.metrics.CachedGauge;
import com.codahale.metrics.Counter;
import com.codahale.metrics.Gauge;
import com.codahale.metrics.Metric;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.MetricSet;
import com.codahale.metrics.jvm.BufferPoolMetricSet;
import com.codahale.metrics.jvm.GarbageCollectorMetricSet;
import com.codahale.metrics.jvm.MemoryUsageGaugeSet;
import com.codahale.metrics.jvm.ThreadStatesGaugeSet;
import io.spikex.core.AbstractActivator;
import static io.spikex.metrics.Metric.JVM_MEM_PERC;
import static io.spikex.metrics.Metric.METRIC_DSTYPE;
import static io.spikex.metrics.Metric.TYPE_COUNTER;
import static io.spikex.metrics.Metric.TYPE_GAUGE;
import io.spikex.metrics.internal.OSMXBeanMetrics;
import io.spikex.metrics.internal.OshiMetrics;
import java.lang.management.ManagementFactory;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import oshi.SystemInfo;
import oshi.hardware.HardwareAbstractionLayer;
import oshi.hardware.Processor;
import oshi.software.os.OSFileStore;
import oshi.util.FormatUtil;

/**
 *
 * @author cli
 */
public final class Activator extends AbstractActivator {

    private OshiMetrics m_oshiMetrics;
    private OSMXBeanMetrics m_osMXBeanMetrics;
    private MetricRegistry m_registry;
    private Map<String, Object> m_sharedData;

    @Override
    protected void startVerticle() {

        m_sharedData = vertx.sharedData().getMap(SHARED_METRICS_KEY);

        // OSHI system information
        SystemInfo systemInfo = new SystemInfo();
        m_oshiMetrics = new OshiMetrics(systemInfo);
        m_oshiMetrics.init();

        // Java OS MXBean information
        m_osMXBeanMetrics = new OSMXBeanMetrics();
        m_osMXBeanMetrics.init();
        
        // Register metrics
        m_registry = new MetricRegistry();

        logger().debug("Registering JVM garbage collection metric set");
        registerMetrics("jvm.gc", new GarbageCollectorMetricSet(), m_registry);

        logger().debug("Registering JMV buffer metric set");
        registerMetrics("jvm.buffers", new BufferPoolMetricSet(
                ManagementFactory.getPlatformMBeanServer()), m_registry);

        logger().debug("Registering JMV memory usage metric set");
        registerMetrics("jvm.memory", new MemoryUsageGaugeSet(), m_registry);

        logger().debug("Registering JMV thread states metric set");
        registerMetrics("jvm.threads", new ThreadStatesGaugeSet(), m_registry);

        // Aggregate metrics
        registerAggregateMetrics(m_registry);

        // Output system information
        outputSysInfo(systemInfo);
    }

    @Override
    protected void handleTimerEvent() {
        //
        // Save latest CPU, memory and other statistics in shared map
        //
        Map<String, Object> sharedData = m_sharedData;
        publishGauges(sharedData);
        publishCounters(sharedData);
        m_oshiMetrics.publish(sharedData);
        m_osMXBeanMetrics.publish(sharedData);
    }

    private void registerMetrics(
            final String prefix,
            final MetricSet metricSet,
            final MetricRegistry registry) {

        for (Map.Entry<String, Metric> entry : metricSet.getMetrics().entrySet()) {
            if (entry.getValue() instanceof MetricSet) {
                registerMetrics(prefix + "." + entry.getKey(), (MetricSet) entry.getValue(), registry);
            } else {
                registry.register(prefix + "." + entry.getKey(), entry.getValue());
            }
        }
    }

    private void registerAggregateMetrics(final MetricRegistry registry) {
        //
        // JVM used percentage
        //
        Map<String, Gauge> gauges = registry.getGauges();
        final Gauge<Long> gaugeJvmMemMax = gauges.get("jvm.memory.total.max");
        final Gauge<Long> gaugeJvmMemUsed = gauges.get("jvm.memory.total.used");

        registry.register(JVM_MEM_PERC.key(), new CachedGauge<Double>(5L, TimeUnit.SECONDS) {

            @Override
            protected Double loadValue() {
                double memMax = gaugeJvmMemMax.getValue();
                double memUsed = gaugeJvmMemUsed.getValue();
                return (memUsed / memMax) * 100.0d;
            }
        });
    }

    private void outputSysInfo(final SystemInfo sysInfo) {

        logger().info("======================================================");
        logger().info("{}", sysInfo.getOperatingSystem());
        logger().info("------------------------------------------------------");

        HardwareAbstractionLayer hal = sysInfo.getHardware();
        Processor[] cpus = hal.getProcessors();
        logger().info("CPU count: {}", cpus.length);
        int n = 0;
        for (Processor cpu : cpus) {
            logger().info("{}: {}", n++, cpu);
        }

        long memTotal = hal.getMemory().getTotal();
        long memAvail = hal.getMemory().getAvailable();
        long memUsed = memTotal - memAvail;

        logger().info("Used memory: {}", FormatUtil.formatBytes(memUsed));
        logger().info("Available memory: {}", FormatUtil.formatBytes(memAvail));
        logger().info("Total memory: {}", FormatUtil.formatBytes(memTotal));

        for (OSFileStore fs : hal.getFileStores()) {
            logger().info("{} ({}) Used: {} Free: {} Total: {}",
                    fs.getName(),
                    fs.getDescription(),
                    FormatUtil.formatBytes(fs.getTotalSpace() - fs.getUsableSpace()),
                    FormatUtil.formatBytes(fs.getUsableSpace()),
                    FormatUtil.formatBytes(fs.getTotalSpace()));
        }

        logger().info("======================================================");
    }

    private void publishGauges(final Map<String, Object> sharedData) {

        Map<String, Gauge> gauges = m_registry.getGauges();
        for (Map.Entry<String, Gauge> entry : gauges.entrySet()) {

            String key = entry.getKey();
            Gauge gauge = entry.getValue();
            Object value = gauge.getValue();

            if (value instanceof Set) {
                StringBuilder sb = new StringBuilder();
                Set<Object> values = (Set) value;
                for (Object val : values) {
                    sb.append(String.valueOf(val));
                    sb.append(",");
                }
                int len = sb.length() - 1;
                if (len > 0) {
                    value = sb.substring(0, len);
                } else {
                    value = sb.toString();
                }
            }

            logger().trace("{} = {} ({})",
                    key,
                    value,
                    value.getClass().getName());

            sharedData.put(key, value); // Actual value
            sharedData.put(key + METRIC_DSTYPE, TYPE_GAUGE); // Datasource type (GAUGE, COUNTER, etc..)
        }
    }

    private void publishCounters(final Map<String, Object> sharedData) {

        Map<String, Counter> counters = m_registry.getCounters();
        for (Map.Entry<String, Counter> entry : counters.entrySet()) {

            String key = entry.getKey();
            Counter counter = entry.getValue();
            long value = counter.getCount();

            logger().trace("{} = {} ({})",
                    key,
                    value,
                    Long.class.getName());

            sharedData.put(key, value); // Actual value
            sharedData.put(key + METRIC_DSTYPE, TYPE_COUNTER); // Datasource type (GAUGE, COUNTER, etc..)
        }
    }
}
