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

import com.sun.management.OperatingSystemMXBean;
import static io.spikex.metrics.Metric.SWAP_FREE;
import static io.spikex.metrics.Metric.SWAP_FREE_PERC;
import static io.spikex.metrics.Metric.SWAP_TOTAL;
import static io.spikex.metrics.Metric.SWAP_USED;
import static io.spikex.metrics.Metric.SWAP_USED_PERC;
import java.lang.management.ManagementFactory;
import java.util.Map;

/**
 *
 * @author cli
 */
public final class OSMXBeanMetrics extends AbstractMetricPublisher {

    private OperatingSystemMXBean m_osMxBean;

    public void init() {
        // Java OS MXBean
        m_osMxBean = (com.sun.management.OperatingSystemMXBean) ManagementFactory.getOperatingSystemMXBean();
    }

    public void publish(final Map<String, Object> sharedData) {
        publishSwap(sharedData);
    }

    private void publishSwap(final Map<String, Object> sharedData) {

        long free = m_osMxBean.getFreeSwapSpaceSize();
        long total = m_osMxBean.getTotalSwapSpaceSize();
        long used = total - free;

        publishMetric(sharedData, SWAP_USED, used);
        publishMetric(sharedData, SWAP_FREE, free);
        publishMetric(sharedData, SWAP_TOTAL, total);

        publishMetric(sharedData, SWAP_USED_PERC, 100.0d * used / total);
        publishMetric(sharedData, SWAP_FREE_PERC, 100.0d * free / total);
    }
}
