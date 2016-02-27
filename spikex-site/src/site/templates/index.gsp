<%include "header.gsp"%>
<%include "menu.gsp"%>

<div id="headerwrap">
    <div class="container">
        <div class="row">
            <div class="col-lg-6 col-lg-offset-3">
                <h1>Spike.x</h1>
                <h3>Sense. React. Visualize.</h3>
            </div>
        </div>
    </div>
</div>

<section id="intro">
    <div class="container">
        <div class="row centered">
            <p>Reactive event monitoring and data analysis built on top of <a href="http://vertx.io/vertx2">Vert.x</a>.</p>
            <p>Spike.x provides components for resource monitoring, for data filtering and streaming, 
                for sending of notifications and for storing of metrics and events in various backends.
            </p>
            <p>
                <a href="http://grafana.org">Grafana</a> / 
                <a href="https://influxdata.com/time-series-platform/influxdb">InfluxDB</a> / 
                <a href="https://www.elastic.co/products/kibana">Kibana</a> / 
                <a href="https://www.elastic.co/products/elasticsearch">Elasticsearch</a> / 
                <a href="http://nsq.io">NSQ</a>
            </p>
        </div>
    </div>
</section>

<section id="downloadwrap">
    <div class="container">
        <div class="row centered">
            <div class="col-lg-12 col-md-12 col-sm-12 ucblock">
                <p>
                    <h4>Download:</h4>
                </p>
                <p>
                    <a href="https://bintray.com/artifact/download/spikex/generic/spikex-${config.spikex_version}-installer.exe">spikex-${config.spikex_version}-installer.exe</a> / 
                    <a href="https://bintray.com/artifact/download/spikex/generic/spikex-${config.spikex_version}-installer.jar">spikex-${config.spikex_version}-installer.jar</a> / 
                    <a href="https://bintray.com/artifact/download/spikex/generic/spikex-${config.spikex_version}.tar.gz">spikex-${config.spikex_version}.tar.gz</a> / 
                    <a href="https://bintray.com/artifact/download/spikex/generic/spikex-${config.spikex_version}.zip">spikex-${config.spikex_version}.zip</a>
                </p>
            </div>
        </div>
    </div>
</section>

<section id="usecases">
    <div class="container">
        <div class="row centered">
            <div class="col-lg-6 col-md-6 col-sm-6 ucblock">
                <i class="fa fa-bar-chart"></i>
                <dl>
                    <dt>Event collection, query, analysis and presentation</dt>
                    <dd>Tail logs and send data to <a href="https://www.elastic.co/products/elasticsearch">Elasticsearch</a> or
                        <a href="https://influxdata.com/time-series-platform/influxdb">InfluxDB</a>.</dd>
                    <dd>Collect JVM, CPU, network, filesystem and memory metrics.</dd>
                    <dd>Collect database metrics using custom SQL.</dd>
                    <dd>Query, analyze and present the data with <a href="http://grafana.org">Grafana</a> or
                        <a href="https://www.elastic.co/products/kibana">Kibana</a>.</dd>
                </dl>
            </div>
            <div class="col-lg-6 col-md-6 col-sm-6 ucblock">                    
                <i class="fa fa-bell"></i>
                <dl>
                    <dt>Notifications</dt>
                    <dd>Monitor key data parameters.</dd>
                    <dd>Trigger alarms based on custom rules.</dd>
                    <dd>Log and send notifications to interested parties.</dd>
                </dl>
            </div>
        </div>
    </div>
</section>

<%include "footer.gsp"%>