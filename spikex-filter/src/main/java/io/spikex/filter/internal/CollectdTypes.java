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

import java.util.HashMap;
import java.util.Map;

/**
 *
 * @author cli
 */
public final class CollectdTypes {

    // Json keys
    public static final String COLLECTD_KEY_HOST = "host";
    public static final String COLLECTD_KEY_TIME = "time";
    public static final String COLLECTD_KEY_INTERVAL = "interval";
    public static final String COLLECTD_KEY_PLUGIN = "plugin";
    public static final String COLLECTD_KEY_PLUGIN_INSTANCE = "plugin_instance";
    public static final String COLLECTD_KEY_TYPE = "type";
    public static final String COLLECTD_KEY_TYPE_INSTANCE = "type_instance";
    public static final String COLLECTD_KEY_VALUES = "values";
    public static final String COLLECTD_KEY_DSTYPES = "dstypes";
    public static final String COLLECTD_KEY_DSNAMES = "dsnames";

    /* https://collectd.org/wiki/index.php/Binary_protocol
     0x0000	Host	String	The name of the host to associate with subsequent data values
     0x0001	Time	Numeric	The timestamp to associate with subsequent data values, unix time format (seconds since epoch)
     0x0008	Time (high resolution)	Numeric	The timestamp to associate with subsequent data values. Time is defined in 2–30 seconds since epoch. New in Version 5.0.
     0x0002	Plugin	String	The plugin name to associate with subsequent data values, e.g. "cpu"
     0x0003	Plugin instance	String	The plugin instance name to associate with subsequent data values, e.g. "1"
     0x0004	Type	String	The type name to associate with subsequent data values, e.g. "cpu"
     0x0005	Type instance	String	The type instance name to associate with subsequent data values, e.g. "idle"
     0x0006	Values	other	Data values, see above
     0x0007	Interval	Numeric	Interval used to set the "step" when creating new RRDs unless rrdtool plugin forces StepSize. Also used to detect values that have timed out.
     0x0009	Interval (high resolution)	Numeric	The interval in which subsequent data values are collected. The interval is given in 2–30 seconds. New in Version 5.0.
     0x0100	Message (notifications)	String	
     0x0101	Severity	Numeric	
     0x0200	Signature (HMAC-SHA-256)	other (todo)	
     0x0210	Encryption (AES-256/OFB/SHA-1)	other (todo)	
     */
    public static final int TYPE_HOST = 0x0000;
    public static final int TYPE_TIME_EPOCH = 0x0001;
    public static final int TYPE_TIME_HIGHRES = 0x0008;
    public static final int TYPE_PLUGIN = 0x0002;
    public static final int TYPE_PLUGIN_INSTANCE = 0x0003;
    public static final int TYPE_TYPE = 0x0004;
    public static final int TYPE_TYPE_INSTANCE = 0x0005;
    public static final int TYPE_VALUES = 0x0006;
    public static final int TYPE_INTERVAL_RRD = 0x0007;
    public static final int TYPE_INTERVAL_HIGHRES = 0x0009;
    public static final int TYPE_MESSAGE = 0x0100;
    public static final int TYPE_SEVERITY = 0x0101;
    public static final int TYPE_SIGNATURE = 0x0200;
    public static final int TYPE_ENCRYPTION = 0x0210;

    //
    // Define only multi-value types
    // compression		uncompressed:DERIVE:0:U, compressed:DERIVE:0:U
    // df			used:GAUGE:0:1125899906842623, free:GAUGE:0:1125899906842623
    // disk_latency		read:GAUGE:0:U, write:GAUGE:0:U
    // disk_merged		read:DERIVE:0:U, write:DERIVE:0:U
    // disk_octets		read:DERIVE:0:U, write:DERIVE:0:U
    // disk_ops		read:DERIVE:0:U, write:DERIVE:0:U
    // disk_time		read:DERIVE:0:U, write:DERIVE:0:U
    // disk_io_time		io_time:DERIVE:0:U, weighted_io_time:DERIVE:0:U
    // dns_octets		queries:DERIVE:0:U, responses:DERIVE:0:U
    // if_dropped		rx:DERIVE:0:U, tx:DERIVE:0:U
    // if_errors		rx:DERIVE:0:U, tx:DERIVE:0:U
    // if_octets		rx:DERIVE:0:U, tx:DERIVE:0:U
    // if_packets		rx:DERIVE:0:U, tx:DERIVE:0:U
    // io_octets		rx:DERIVE:0:U, tx:DERIVE:0:U
    // io_packets		rx:DERIVE:0:U, tx:DERIVE:0:U
    // load			shortterm:GAUGE:0:5000, midterm:GAUGE:0:5000, longterm:GAUGE:0:5000
    // memcached_octets	rx:DERIVE:0:U, tx:DERIVE:0:U
    // mysql_octets		rx:DERIVE:0:U, tx:DERIVE:0:U
    // node_octets		rx:DERIVE:0:U, tx:DERIVE:0:U
    // ps_count		processes:GAUGE:0:1000000, threads:GAUGE:0:1000000
    // ps_cputime		user:DERIVE:0:U, syst:DERIVE:0:U
    // ps_disk_octets		read:DERIVE:0:U, write:DERIVE:0:U
    // ps_disk_ops		read:DERIVE:0:U, write:DERIVE:0:U
    // ps_pagefaults		minflt:DERIVE:0:U, majflt:DERIVE:0:U
    // serial_octets		rx:DERIVE:0:U, tx:DERIVE:0:U
    // smart_attribute         current:GAUGE:0:255, worst:GAUGE:0:255, threshold:GAUGE:0:255, pretty:GAUGE:0:U
    // vmpage_faults		minflt:DERIVE:0:U, majflt:DERIVE:0:U
    // vmpage_io		in:DERIVE:0:U, out:DERIVE:0:U
    // voltage_threshold	value:GAUGE:U:U, threshold:GAUGE:U:U
    // 
    public static final Map<String, String[]> TYPES_DB = new HashMap();

    static {
        TYPES_DB.put("compression", new String[]{"uncompressed", "compressed"});
        TYPES_DB.put("df", new String[]{"used", "free"});
        TYPES_DB.put("disk_latency", new String[]{"read", "write"});
        TYPES_DB.put("disk_merged", new String[]{"read", "write"});
        TYPES_DB.put("disk_octets", new String[]{"read", "write"});
        TYPES_DB.put("disk_ops", new String[]{"read", "write"});
        TYPES_DB.put("disk_time", new String[]{"read", "write"});
        TYPES_DB.put("disk_io_time", new String[]{"io_time", "weighted_io_time"});
        TYPES_DB.put("dns_octets", new String[]{"queries", "responses"});
        TYPES_DB.put("if_dropped", new String[]{"rx", "tx"});
        TYPES_DB.put("if_errors", new String[]{"rx", "tx"});
        TYPES_DB.put("if_octets", new String[]{"rx", "tx"});
        TYPES_DB.put("if_packets", new String[]{"rx", "tx"});
        TYPES_DB.put("io_octets", new String[]{"rx", "tx"});
        TYPES_DB.put("io_packets", new String[]{"rx", "tx"});
        TYPES_DB.put("load", new String[]{"shortterm", "midterm", "longterm"});
        TYPES_DB.put("memcached_octets", new String[]{"rx", "tx"});
        TYPES_DB.put("mysql_octets", new String[]{"rx", "tx"});
        TYPES_DB.put("node_octets", new String[]{"rx", "tx"});
        TYPES_DB.put("ps_count", new String[]{"processes", "threads"});
        TYPES_DB.put("ps_cputime", new String[]{"user", "syst"});
        TYPES_DB.put("ps_disk_octets", new String[]{"read", "write"});
        TYPES_DB.put("ps_disk_ops", new String[]{"read", "write"});
        TYPES_DB.put("ps_pagefaults", new String[]{"minflt", "majflt"});
        TYPES_DB.put("serial_octets", new String[]{"rx", "tx"});
        TYPES_DB.put("smart_attribute", new String[]{"current", "worst", "threshold", "pretty"});
        TYPES_DB.put("vmpage_faults", new String[]{"minflt", "majflt"});
        TYPES_DB.put("vmpage_io", new String[]{"in", "out"});
        TYPES_DB.put("voltage_threshold", new String[]{"value", "threshold"});
    }
}
