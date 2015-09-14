# Sense. React. Visualize.
Reactive event monitoring and analysis built on top of [Vert.x](http://vertx.io/vertx2) for resource-aware applications.
Spike.x provides components for resource monitoring, for data filtering and streaming, 
for sending of notifications, for storing of events in various backends 
and for many other use cases.

Spike.x can be used out-of-the-box for the following use cases:
* Filtering, streaming and analysis of events::
  * Tail logs and send events to [Elasticsearch](http://www.elasticsearch.org) or [InfluxDB](http://influxdb.com)
  * Collect CPU, network and memory metrics
  * Collect database metrics using custom SQL
  * Query, analyze and present the data with [Kibana](http://www.elasticsearch.org/overview/kibana) or [Grafana](http://grafana.org)

* Monitoring and sending of notifications::
  * Monitor key data parameters 
  * Trigger events based on custom rules
  * Log and send alarms to interested parties
  * Send alarms only to specific parties after office hours

**CAUTION:** Spike.x 0.9.x is in its infancy. It will take some time before it can be considered production ready.

# Concepts

Spike.x brings a few concepts of its own to the table. We introduce the following concepts: activator, filter and command.

An activator is simply a verticle that is responsible for deploying and undeploying of verticles within a module. The activator is the main verticle of a module.

A filter is verticle that receives input, sends output or does both. Spike.x comes with many pre-built filters for many common uses:

* Tail - reads lines from a log
* Mutate - modifies an event
* Limit - performs event limiting
* Grok - matches regexps against an event field
* Rrd4j - creates RRD graphs
* HttpServer - receives events via HTTP (supports collectd wtite-http plugin) 
* Buffer - buffers events before publishing
* NSQ - published or subscribes to events
* Elasticsearh - stores events in Elasticsearch
* InfluxDB - stores events in InfluxDB 0.9.x

Commands are used to control the behaviour of verticles. We send commands to activators in order to deploy or undeploy filters. These commands are reserved for controlling Spike.x. But you can create your own custom commands.

Spike.x has a Main class that is responsible for bootstrapping and starting the Vert.x platform. It also takes care of daemonizing Spike.x on platforms that support daemons. Please see the spikex startup script for details.

When you start Spike.x it tries to load any required modules from a local or remote repository. The same mechanism is used to load updated modules. You can of course configure this to suit your needs.

Naturally we do not want to reinvent the wheel. Spike.x depends on many well-established open source libraries. The following are available to any Spike.x based application:

* SLF4J - Simple Logging Facade for Java
* Logback - SLF4J implementation
* Guava - Google's core libraries
* Joda-Time - Java date and time API (also part of Java 8)
* Bouncy Castle Crypto APIs for Java
* Kryo - Fast, efficient Java serialization and cloning
* Hazelcast - Open Source In-Memory Data Grid (part of Vert.x)
* UUID - an implementation of the UUIDs and GUIDs specification in Java
* LZ4 Java - LZ4 compression and xxhash hashing for Java
* SnakeYAML - YAML parser and emitter for Java
* Boon - Simple opinionated Java for the novice to expert level Java Programmer
* GS Collections - A supplement or replacement for the Java Collections Framework

# Supported platforms
Spike.x can be installed on almost any platform that supports JDK 1.8 or newer.

The following platforms are currently supported:

* Windows 64-bit (Windows 7, Windows 2003 Server, Windows 2008 Server, etc.)
* Linux 64-bit (Ubuntu, CentOs, RedHat, etc.)
* Apple OS X 64-bit (Yosemite or newer)
* FreeBSD 64-bit (FreeBSD 10 and newer)
* Oracle Solaris 11.x 64-bit
