Zipkin Mesos Framework
======================

Zipkin is a distributed tracing system. It helps gather timing data needed to troubleshoot latency problems in microservice architectures. It manages both the collection and lookup of this data through a Collector and a Query service.  

This Zipkin Mesos Framework is a scheduler that runs Zipkin on Mesos.

This Zipkin Mesos framework is being actively developed by Elodina Inc. and is available as a free trial. In the event that community support is sufficient, Elodina plans to release the framework as an open source project distributed under the Apache License, Version 2.0.

Prerequisites
-------------

* Java 7 (or higher)
* Apache Mesos 0.19 or newer
* Standalone jar files for Zipkin collector, query and web servers (or sources to build from)

Clone and build the project

    # git clone https://github.com/elodina/zipkin-mesos-framework.git
    # cd zipkin-mesos-framework
    # ./gradlew jar

Download Zipkin standalone jars
    
    # wget http://search.maven.org/remotecontent?filepath=io/zipkin/zipkin-collector-service/1.14.1/zipkin-collector-service-1.14.1-all.jar
    # wget http://search.maven.org/remotecontent?filepath=io/zipkin/zipkin-query-service/1.14.1/zipkin-query-service-1.14.1-all.jar
    # wget http://search.maven.org/remotecontent?filepath=io/zipkin/zipkin-web/1.14.1/zipkin-web-1.14.1-all.jar
    
Environment Configuration
--------------------------

Before running `./zipkin-mesos.sh`, set the location of libmesos:

    # export MESOS_NATIVE_JAVA_LIBRARY=/usr/local/lib/libmesos.so

If the host running scheduler has several IP addresses you may also need to

    # export LIBPROCESS_IP=<IP_ACCESSIBLE_FROM_MASTER>

Scheduler Configuration
----------------------

The scheduler is configured through the command line.

Following options are available:

```
Usage: scheduler [options] [config.properties]

Option               Description                            
------               -----------                            
--api                Api url. Example: http://master:7000   
--bind-address       Scheduler bind address (master,        
                       0.0.0.0, 192.168.50.*, if:eth1).     
                       Default - all                        
--debug <Boolean>    Debug mode. Default - false            
--framework-name     Framework name. Default - zipkin       
--framework-role     Framework role. Default - *            
--framework-timeout  Framework timeout (30s, 1m, 1h).       
                       Default - 30d        
--log                Log file to use. Default - stdout.     
--master             Master connection settings. Examples:  
                      - master:5050                         
                      - master:5050,master2:5050            
                      - zk://master:2181/mesos              
                      - zk://username:password@master:2181  
                      - zk://master:2181,master2:2181/mesos 
--principal          Principal (username) used to register  
                       framework. Default - none            
--secret             Secret (password) used to register     
                       framework. Default - none            
--storage            Storage for cluster state. Examples:   
                      - file:zipkin-mesos.json              
                      - zk:/zipkin-mesos                    
                     Default - file:zipkin-mesos.json       
--user               Mesos user to run tasks. Default - none   
```

Run the scheduler
-----------------

Start Zipkin scheduler using this command:

    # ./zipkin-mesos.sh scheduler --master master:5050 --user root --api http://master:6666

Quick start
-----------

In order not to pass the API url to each CLI call lets export the URL as follows:

```
# export ZM_API=http://master:6666
```

First lets bring up Zipkin traces collector with the default settings. Further in the readme you can see how to change 
these from the defaults.

```
# ./zipkin-mesos.sh collector add 0
Added servers 0

instance:
  id: 0
  state: Added
  config:
    cpu: 0.5
    mem: 256.0
    port: auto
    adminPort: auto
    env: 
    flags: 
    configFile: collector-cassandra.scala

```

There are two major things you want to configure when bringing up Zipkin collector: receiver, from which the collector
will consume traces and the storage, to which the traces will be sent and then grabbed by the query service.

By default, collector will consume traces via Scribe. In order to configure a collector to use Kafka one should add
`KAFKA_ZOOKEEPER` environment variable, and point it to the address of Zookeeper, where Kafka cluster is running. Also,
you may set `KAFKA_TOPIC` in order to consume from particular topic, by default topic name is `zipkin`

In order to set the collector storage, one should first select the storage type by pointing to appropriate Scala config 
file. By default it is set for using Cassandra database, although you may also use Redis or MySQL. After setting the 
storage type you may want to set the appropriate environment variables. For example `CASSANDRA_CONTACT_POINTS`, 
`CASSANDRA_USERNAME`, `CASSANDRA_PASSWORD` for Cassandra connection credentials.

Another important thing you want to configure is traces sample rate. It is set by configuring `COLLECTOR_SAMPLE_RATE`. 
It stands for percentage of how often traces are actually dropped to the storage, where `1.0` means 100%. 

So, at the end, our initial configuration may look like this:

```
# ./zipkin-mesos.sh collector config 0 --env KAFKA_ZOOKEEPER=master:2181,KAFKA_TOPIC=notzipkin,CASSANDRA_CONTACT_POINTS=localhost,CASSANDRA_USERNAME=user,CASSANDRA_PASSWORD=pwd,COLLECTOR_SAMPLE_RATE=0.01
Updated configuration for Zipkin collector instance(s) 0

instance:
  id: 0
  state: Added
  config:
    cpu: 0.5
    mem: 256.0
    port: auto
    adminPort: auto
    env: CASSANDRA_CONTACT_POINTS=localhost,COLLECTOR_SAMPLE_RATE=0.01,CASSANDRA_USERNAME=user,KAFKA_ZOOKEEPER=master:218,KAFKA_TOPIC=notzipkin,CASSANDRA_PASSWORD=pwd
    flags: 
    configFile: collector-cassandra.scala
```

Now lets start the server. This call to CLI will block until the server is actually started, but will wait no more than 
a configured timeout. Timeout can be passed via `--timeout` flag and defaults to `60s`. If a timeout of `0ms` is passed 
CLI won't wait for servers to start at all and will reply with "Scheduled servers ..." message.

```
# ./zipkin-mesos.sh collector start 0 --timeout 30s
Started collector instance(s) 0

instance:
  id: 0
  state: Running
  endpoint: http://slave0:31001
  config:
    cpu: 0.5
    mem: 256.0
    port: auto
    adminPort: auto
    env: KAFKA_ZOOKEEPER=master:2181,KAFKA_TOPIC=notzipkin,COLLECTOR_PORT=31001,COLLECTOR_ADMIN_PORT=31002
    flags: 
    configFile: collector-dev.scala
```

Note, that we can see the endpoint, where collector instance is running by having a look at `endpoint` field.
Also note that along with the collector server, an admin server will be up and running on the same host. You may check 
out its port by having a look at `COLLECTOR_ADMIN_PORT` variable.

By now you should have a single collector instance running. Here's how you stop it:

```
# ./zipkin-mesos.sh collector stop 0
Stopped collector instance(s) 0
```

If you want to remove the server from the cluster completely you may skip `stop` step and call `remove` directly (this will call `stop` under the hood anyway):

```
./zipkin-mesos.sh collector remove 0
Removed collector instance(s) 0
```

Now, you may start a Query server. Usage is pretty similar. Here, you will just want to configure the storage type and
the storage credentials. Let's add and configure an instance:

```
# ./zipkin-mesos.sh query add 0 --env CASSANDRA_CONTACT_POINTS=localhost,CASSANDRA_USERNAME=user,CASSANDRA_PASSWORD=pwd
Added servers 0
```

Start, stop and remove are pretty much the same, just replace `collector` with `query` in your calls to the CLI.

```
# ./zipkin-mesos.sh query start 0
# ./zipkin-mesos.sh query stop 0
# ./zipkin-mesos.sh query remove 0
```

Now, you may start the web service in order to see the UI representation of your traces. Recall that after query service
has been started, you may see it's endpoint in the `endpoint` field. This is where you want to point your web 
service to send RESTful HTTP requests to. This is configured by setting the `zipkin.web.query.dest` flag:
 
```
# ./zipkin-mesos.sh web add 0 --flags zipkin.web.query.dest=slave0:31001
Added servers 0
```

Start, stop and remove calls are the same, just add `web` in your calls to the CLI.

```
# ./zipkin-mesos.sh web start 0
# ./zipkin-mesos.sh web stop 0
# ./zipkin-mesos.sh web remove 0
```

After the start, you may open the web service's `endpoint` address in your browser, there you will see your traces info.

Verifying all components running
================================

In order to verify that all the services running correctly, simply run a `ping` task on this project. Make sure to 
configure task to produce traces to the Kafka topic, from which your collector is consuming traces:
 
```
# KAFKA_BROKER=localhost:9092 KAFKA_TOPIC=notzipkin ./gradlew ping
``` 

This will post a dummy trace annotation to the specified topic. You should be able to see it in Zipkin web UI.
