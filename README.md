# Sense. React. Visualize.
Reactive event monitoring and data analysis built on top of [Vert.x](http://vertx.io/vertx2).
Spike.x provides components for resource monitoring, for data filtering and streaming, 
for sending of notifications and for storing of metrics and events in various backends.

Spike.x can be used out-of-the-box for the following use cases:
* Filtering, streaming and analysis of events:
  * Tail logs and send events to [Elasticsearch](https://www.elastic.co) or [InfluxDB](https://influxdb.com/)
  * Collect JVM, CPU, network, filesystem and memory metrics
  * Collect database metrics using custom SQL
  * Query, analyze and present the data with [Kibana](https://www.elastic.co/products/kibana) or [Grafana](http://grafana.org)

* Event monitoring and sending of notifications:
  * Monitor key data parameters 
  * Trigger events based on custom rules
  * Log and send notifications to interested parties
  * Send alarms only to specific parties after office hours

**CAUTION:** Spike.x 0.9.x is in its infancy. It will take some time before it can be considered production ready.

# Getting started

You can grab the latest Spike.x installation package from [Bintray](https://bintray.com/spikex/generic/installer/view).

Simply launch the installer and follow the instructions.

**IMPORTANT:** The installer requires Java to function and Spike.x requires JDK 1.8 or newer to run.

The main concepts of Spike.x are "chains" and "filters". The filters do the actual work. Chains contain filters that are tied together. You can define many chains, for instance, one chain that receives input from Collectd, another one that receives input from a custom tool and a third one that funnels all the metrics to a log file. Filters communicate using input and output addresses.

Technically a filter is a Vert.x verticle that receives input, sends output or does both. Spike.x comes with many filters that can be chained together in many ways. Here's a list of some of the built-in filters:

* Tail - reads lines from a log
* Mutate - modifies an event (supports conditional modification)
* Limit - performs event limiting
* Grok - matches regexps against an event field
* HttpServer - receives events via HTTP (supports collectd write-http plugin) 
* Batch - creates a batch of events before publishing
* NSQ - publishes or subscribes events to/from NSQ
* InfluxDB - stores events in InfluxDB 0.10.x
* Elasticsearch - stores events in Elasticsearch

When you start Spike.x it loads modules from its deploy directory. 

Spike.x depends on many well-established open source libraries. The following is a list of some of the core dependencies:

* SLF4J - Simple Logging Facade for Java
* Logback - SLF4J implementation
* Guava - Google's core libraries
* Bouncy Castle Crypto APIs for Java
* Hazelcast - Open Source In-Memory Data Grid (part of Vert.x)
* LZ4 Java - LZ4 compression and xxhash hashing for Java
* SnakeYAML - YAML parser and emitter for Java
* GS Collections - A supplement or replacement for the Java Collections Framework

# User guide

# Supported platforms
Spike.x can be installed on almost any platform that supports JDK 1.8 or newer, but some of the filters only support a specific platform.

The following platforms are currently supported:

* Windows 64-bit (Windows 7, Windows 2003 Server, Windows 2008 Server, etc.)
* Linux 64-bit (Ubuntu, CentOs, RedHat, etc.)
* Apple OS X 64-bit (Yosemite or newer)
* FreeBSD 64-bit (FreeBSD 10 and newer)
