
# repos
local '~/.m2/repository'
repository 'http://mirrors.ibiblio.org/pub/mirrors/maven2'
repository "https://repository.cloudera.com/content/repositories/releases/"

# HADOOP - Cloudera CDH4 distro
# HADOOP_VERSION = "2.0.0-cdh4.0.1"
# jar "org.apache.hadoop:hadoop-client:jar:#{HADOOP_VERSION}"
# scope 'test' do
#   jar "org.apache.hadoop:hadoop-common:jar:tests:#{HADOOP_VERSION}"
#   jar "org.apache.hadoop:hadoop-hdfs:jar:tests:#{HADOOP_VERSION}"
#   jar "org.apache.hadoop:hadoop-hdfs:jar:test-sources:#{HADOOP_VERSION}"
# end

# CDH3
HADOOP_VERSION = "0.20.2-cdh3u5"
jar "org.apache.hadoop:hadoop-core:jar:#{HADOOP_VERSION}"
jar "org.apache.hadoop:hadoop-tools:jar:#{HADOOP_VERSION}"
scope 'test' do
  jar "org.apache.hadoop:hadoop-test:jar:#{HADOOP_VERSION}"
end

# LOGGING
jar 'org.slf4j:slf4j-api:jar:1.6.4'
jar 'org.slf4j:slf4j-log4j12:jar:1.6.4'
jar 'log4j:log4j:jar:1.2.16'
jar 'log4j:apache-log4j-extras:jar:1.1'

jar 'net.minidev:json-smart:jar:1.0.9-1'

jar 'com.google.guava:guava:jar:11.0'

# COMMONS
jar "commons-cli:commons-cli:jar:1.2"
jar 'org.apache.commons:commons-lang3:jar:3.1'
jar "commons-pool:commons-pool:jar:1.6"
jar "commons-daemon:commons-daemon:jar:1.0.8"

# JSON
jar "com.fasterxml.jackson.core:jackson-core:jar:2.0.6"
jar "com.fasterxml.jackson.core:jackson-databind:jar:2.0.6"
jar "com.fasterxml.jackson.core:jackson-annotations:jar:2.0.6"

# older lib required by some deps
jar "org.codehaus.jackson:jackson-core-asl:jar:1.9.9"
jar "org.codehaus.jackson:jackson-mapper-asl:jar:1.9.9"


# JETTY
JETTY_VERSION = "8.1.4.v20120524"
jar "org.eclipse.jetty.orbit:javax.servlet:jar:3.0.0.v201112011016"
jar "org.eclipse.jetty:jetty-continuation:jar:#{JETTY_VERSION}"
jar "org.eclipse.jetty:jetty-http:jar:#{JETTY_VERSION}"
jar "org.eclipse.jetty:jetty-io:jar:#{JETTY_VERSION}"
jar "org.eclipse.jetty:jetty-jmx:jar:#{JETTY_VERSION}"
jar "org.eclipse.jetty:jetty-server:jar:#{JETTY_VERSION}"
jar "org.eclipse.jetty:jetty-util:jar:#{JETTY_VERSION}"

# JOLOKIA
jar "org.jolokia:jolokia-jvm:jar:agent:1.0.5"

# MISC
jar 'com.googlecode.disruptor:disruptor:jar:2.10.3'
jar 'com.yammer.metrics:metrics-core:jar:2.1.3'
jar 'com.yammer.metrics:metrics-jetty:jar:2.1.3'
jar 'com.yammer.metrics:metrics-graphite:jar:2.1.3'
