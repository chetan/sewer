
# repos
local '~/.m2/repository'
repository 'http://mirrors.ibiblio.org/pub/mirrors/maven2'
repository "https://repository.cloudera.com/content/repositories/releases/"

# HADOOP - Cloudera CDH distro
HADOOP_VERSION = "2.0.0-cdh4.0.1"
jar "org.apache.hadoop:hadoop-client:jar:#{HADOOP_VERSION}"

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

jar "org.codehaus.jackson:jackson-core-asl:jar:1.9.3"
jar "org.codehaus.jackson:jackson-mapper-asl:jar:1.9.3"


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
jar 'com.googlecode.disruptor:disruptor:jar:2.8'
jar 'com.yammer.metrics:metrics-core:jar:2.0.2'
jar 'com.yammer.metrics:metrics-jetty:jar:2.0.2'

scope 'test' do
	jar "org.apache.hadoop:hadoop-hdfs:jar:tests:#{HADOOP_VERSION}"
end
