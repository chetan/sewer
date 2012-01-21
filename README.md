# Sewer - a high performance, reliable pixel server

Sewer is built for a single purpose: serving "204 No Content" responses via an embedded [Jetty](http://www.eclipse.org/jetty/) server and writing access logs to HDFS as quickly and reliably as possible.

Sewer was heavily inspired by [Apache Flume](https://cwiki.apache.org/FLUME/).


### Reliability

Sewer is designed to write directly to HDFS from the node which generates the event. As such, it is capable of surviving a *single downstream failure* and automatically retrying when the downstream issue has been resolved.

Types of errors that will be recovered from include:

* Network errors
* NameNode unreachable
* NameNode in safe-mode
* DateNode errors:
* HDFS create/close fails
* etc

Reliability is achieved by simultaneously sending each event to both HDFS and the local disk. When an event batch is successfully flushed and closed, the local buffer is deleted. On failure, the buffer remains and moves into a retry queue.


### Performance

For maximum I/O performance, both the local buffers and HDFS writes are compressed using the best available compressor (currently defaults to GZIP when hadoop-native is available).

### Benchmarks

#### EC2

* m1.small:   3,622 reqs/sec
* m1.large:  13,293 reqs/sec
* c1.medium: 16,556 reqs/sec
* m1.small via elb:  3,205 reqs/sec
* m1.large via elb:

Methodology: 2x m1.large load generators running 'ab' twice each with the following params:

        ab #{LONG_UA} -k -r -t 600 -n 500000 -c 400 #{URL}
        LONG_UA = 800 byte user agent header to simulate a large payload

Tests run January, 2012

### Log Format


### Configuration


### Not Quite a Flume Replacement

While Sewer uses the same source/sink pattern under the hood, it is not designed to be a drop-in Flume replacement. Flume was designed to be extremely flexible whereas Sewer has been heavily tuned and tested for a single scenario: writing pixels to hdfs. It may be possible to read/write events from/to different systems, however these modes are generally not natively supported (even if the code exists internally :).

And while the code is fairly modular and flexible, there is currently no "Master" server to centrally command and control your nodes; instead, each node is locally configured via a simple properties file. Thus, if you want to reconfigure a node, you must start and stop the Sewer process.
