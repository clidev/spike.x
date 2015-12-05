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

# Chains and filters

We introduce the following extensions to a verticle: activator and filter.

An activator is simply a verticle that is responsible for deploying and undeploying of verticles within a module. The activator is the main verticle of a module.

A filter is a verticle that receives input, sends output or does both. Spike.x comes with many filters that can be chained together in many ways. Here's a list of some of the built-in filters:

* Tail - reads lines from a log
* Mutate - modifies an event (supports conditional modification)
* Limit - performs event limiting
* Grok - matches regexps against an event field
* Rrd4j - creates RRD graphs
* HttpServer - receives events via HTTP (supports collectd wtite-http plugin) 
* Buffer - buffers events before publishing
* Elasticsearh - stores events in Elasticsearch
* InfluxDB - stores events in InfluxDB 0.9.x

Commands are used to control the behaviour of verticles. We send commands to activators in order to deploy or undeploy filters. These commands are reserved for controlling Spike.x and are an internal detail. 

Spike.x has a Main class that is responsible for bootstrapping and starting the Vert.x platform. It also takes care of daemonizing Spike.x if needed on platforms that support this. Please see the spikex startup script for details.

When you start Spike.x it tries to load any required modules from a local deploy directory. 

Spike.x depends on many well-established open source libraries. The following is a list of some of the core dependencies:

* SLF4J - Simple Logging Facade for Java
* Logback - SLF4J implementation
* Guava - Google's core libraries
* Bouncy Castle Crypto APIs for Java
* Hazelcast - Open Source In-Memory Data Grid (part of Vert.x)
* LZ4 Java - LZ4 compression and xxhash hashing for Java
* SnakeYAML - YAML parser and emitter for Java
* GS Collections - A supplement or replacement for the Java Collections Framework

# Getting started

# User guide

# Supported platforms
Spike.x can be installed on many platforms that support JDK 1.8 or newer.

The following platforms are currently supported:

* Windows 64-bit (Windows 7, Windows 2003 Server, Windows 2008 Server, etc.)
* Linux 64-bit (Ubuntu, CentOs, RedHat, etc.)
* Apple OS X 64-bit (Yosemite or newer)
* FreeBSD 64-bit (FreeBSD 10 and newer)
