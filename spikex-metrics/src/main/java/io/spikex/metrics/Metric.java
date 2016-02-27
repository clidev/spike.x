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

/**
 *
 * @author cli
 */
public enum Metric {

    // Per cpu
    CPU_USER_PERC("system.cpu.user.perc", "total system cpu user percent"),
    CPU_NICE_PERC("system.cpu.nice.perc", "total system cpu nice percent"),
    CPU_SYS_PERC("system.cpu.sys.perc", "total system cpu sys percent"),
    CPU_IDLE_PERC("system.cpu.idle.perc", "total system cpu idle percent"),
    CPU_COMBINED_PERC("system.cpu.combined.perc", "combined cpu sys and user percent"),
    CPU_LOAD_AVERAGE("system.cpu.load.avg", "cpu load average for the past n seconds"),
    
    MEM_USED("system.memory.used", "total used system memory in bytes"),
    MEM_FREE("system.memory.free", "total free system memory in bytes"),
    MEM_TOTAL("system.memory.total", "total amount of system memory in bytes"),
    MEM_USED_PERC("system.memory.used.perc", "total used system memory percent"),
    MEM_FREE_PERC("system.memory.free.perc", "total free system memory percent"),

    SWAP_USED("system.swap.used", "total used swap memory in bytes"),
    SWAP_FREE("system.swap.free", "total free swap memory in bytes"),
    SWAP_TOTAL("system.swap.total", "total amount of swap memory in bytes"),
    SWAP_USED_PERC("system.swap.used.perc", "total used swap memory percent"),
    SWAP_FREE_PERC("system.swap.free.perc", "total free swap memory percent"),
    
    // Per filesystem
    FILESYSTEM_USED("filesystem.used", "total used bytes of filesystem"),
    FILESYSTEM_FREE("filesystem.free", "total free bytes of filesystem"),
    FILESYSTEM_TOTAL("filesystem.total", "total size of filesystem in bytes"),

    LOAD_AVERAGE("system.load.avg.1m", "system load average for the last minute"),
    LOAD_AVERAGE_PERC("system.load.avg.perc.1m", "system load average percent for the last minute"),
    UPTIME("system.uptime", "system uptime in seconds since boot"),
    JVM_MEM_PERC("jvm.memory.perc", "JVM mem usage percent (aggregate)");

    private final String m_key;
    private final String m_desc;
    private final String m_type;

    // Common data source types
    public static final String TYPE_GAUGE = "GAUGE";
    public static final String TYPE_COUNTER = "COUNTER";
    public static final String TYPE_DERIVE = "DERIVE";
    public static final String TYPE_STRING = "STRING";

    public static final String METRIC_DSTYPE = ".dstype";

    Metric(final String key,
            final String desc) {

        this(key, desc, TYPE_GAUGE);
    }

    Metric(final String key,
            final String desc,
            final String type) {

        m_key = key;
        m_desc = desc;
        m_type = type;
    }

    public String key() {
        return m_key;
    }

    public String desc() {
        return m_desc;
    }

    public String getType() {
        return m_type;
    }
}
