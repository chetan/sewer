# Sewer - a high performance, reliable pixel server

Sewer is built for a single purpose: serving "204 No Content" responses via an embedded [Jetty](http://www.eclipse.org/jetty/) server and writing access logs to HDFS as quickly and reliably as possible.

Sewer was heavily inspired by [Apache Flume](https://cwiki.apache.org/FLUME/).


### Not Quite a Flume Replacement

While Sewer uses the same source/sink pattern under the hood, it is not designed to be a drop-in Flume replacement. Flume was designed to be extremely flexible whereas Sewer has been heavily tuned and tested for a single scenario: writing pixels to hdfs. It may be possible to read/write events from/to different systems, however these modes are generally not natively supported (even if the code exists internally :).

And while the code is fairly modular and flexible, there is currently no "Master" server to centrally command and control your nodes; instead, each node is locally configured via a simple properties file. Thus, if you want to reconfigure a node, you must start and stop the Sewer process.

### Reliability

Sewer is designed to write directly to HDFS from the node which generates the event. There are no middlemen or aggregation tiers as there are with Flume. As such, it is capable of surviving a *single downstream failure* and automatically retrying when the downstream issue has been resolved. Types of errors might include:

* NameNode unreachable
* Network errors
* NameNode in safe-mode
* HDFS fails to close file
* HDFS create fails (no free space?)
* etc

Reliability is achieved by simultaneously sending each event to both HDFS and the local disk. When a file is successfully closed, the local buffer is deleted. On failure, the buffer remains and moves into a retry queue.

### Log Format


### Configuration


### Performance

For maximum I/O performance, both the local buffers and HDFS writes are compressed using the best available compressor (currently defaults to GZIP when hadoop-native is available).
