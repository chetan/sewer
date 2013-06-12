
COMMONS_CODEC       = [ "commons-codec:commons-codec:jar:1.4" ]
COMMONS_COLLECTIONS = [ "commons-collections:commons-collections:jar:3.2" ]
COMMONS_FILEUPLOAD  = [ "commons-fileupload:commons-fileupload:jar:1.2.1" ]
COMMONS_IO          = [ "commons-io:commons-io:jar:2.1" ]
COMMONS_LANG        = [ "commons-lang:commons-lang:jar:2.4" ]
COMMONS_LANG3       = [ 'org.apache.commons:commons-lang3:jar:3.1' ]
COMMONS_LOGGING     = [ "commons-logging:commons-logging:jar:1.1.1" ]
COMMONS_POOL        = [ "commons-pool:commons-pool:jar:1.6" ]
COMMONS_NET         = [ "commons-net:commons-net:jar:2.0" ]
COMMONS_DISCOVERY   = [ "commons-discovery:commons-discovery:jar:0.4" ]
COMMONS_CLI         = [ "commons-cli:commons-cli:jar:1.2" ]
COMMONS_EL          = [ "commons-el:commons-el:jar:1.0" ]
COMMONS_CSV         = [ "commons-csv:commons-csv:jar:1.0" ]
COMMONS_DAEMON      = [ "commons-daemon:commons-daemon:jar:1.0.8" ]
COMMONS_CONFIGURATION = [ "commons-configuration:commons-configuration:jar:1.9" ]

SOLR_COMMONS_CSV = [ "org.apache.solr:solr-commons-csv:jar:1.4.1" ]

LOGGER = [
    'org.slf4j:slf4j-api:jar:1.6.4',
    'org.slf4j:slf4j-log4j12:jar:1.6.4',
    'log4j:log4j:jar:1.2.16',
    'log4j:apache-log4j-extras:jar:1.1'
    ]

JSON_SMART = [ 'net.minidev:json-smart:jar:1.0.9-1' ]

JACKSON = [
    "com.fasterxml.jackson.core:jackson-core:jar:2.0.6",
    "com.fasterxml.jackson.core:jackson-databind:jar:2.0.6",
    "com.fasterxml.jackson.core:jackson-annotations:jar:2.0.6"
]

JACKSON_OLD = [
    "org.codehaus.jackson:jackson-core-asl:jar:1.9.9",
    "org.codehaus.jackson:jackson-mapper-asl:jar:1.9.9"
    ]

HTTPCLIENT = [
    "commons-httpclient:commons-httpclient:jar:3.1",
    ] + COMMONS_CODEC + COMMONS_LOGGING

JUNIT = [ "junit:junit:jar:4.10" ]

ANT = [ "org.apache.ant:ant:jar:1.8.0" ]

JETTY_VERSION = "8.1.11.v20130520"
JETTY = [
  "org.eclipse.jetty.orbit:javax.servlet:jar:3.0.0.v201112011016",
  "org.eclipse.jetty:jetty-continuation:jar:#{JETTY_VERSION}",
  "org.eclipse.jetty:jetty-http:jar:#{JETTY_VERSION}",
  "org.eclipse.jetty:jetty-io:jar:#{JETTY_VERSION}",
  "org.eclipse.jetty:jetty-jmx:jar:#{JETTY_VERSION}",
  "org.eclipse.jetty:jetty-server:jar:#{JETTY_VERSION}",
  "org.eclipse.jetty:jetty-util:jar:#{JETTY_VERSION}"
  ]




JETTY_VERSION_6 = "6.1.14"
JETTY_6 = [
    "org.mortbay.jetty:servlet-api:jar:2.5-20081211",
    "org.mortbay.jetty:jetty-util:jar:#{JETTY_VERSION_6}",
    "org.mortbay.jetty:jetty:jar:#{JETTY_VERSION_6}",
    "org.mortbay.jetty:jsp-2.1:jar:#{JETTY_VERSION_6}",
    "org.mortbay.jetty:jsp-api-2.1:jar:#{JETTY_VERSION_6}",
    ]

HADOOP_API = [
    "javax.activation:activation:jar:1.1",
    "javax.xml.bind:jaxb-api:jar:2.2.2",
    "javax.xml.stream:stax-api:jar:1.0-2",
    "javax.ws.rs:jsr311-api:jar:1.1.1"
    ]

HADOOP_DEPS = [
    "hsqldb:hsqldb:jar:1.8.0.10",
    "net.java.dev.jets3t:jets3t:jar:0.7.1",
    "net.sf.kosmosfs:kfs:jar:0.3",
    "org.eclipse.jdt:core:jar:3.1.1",
    "oro:oro:jar:2.0.8",
    "tomcat:jasper-compiler:jar:5.5.12",
    "tomcat:jasper-runtime:jar:5.5.12",
    "xmlenc:xmlenc:jar:0.52"
    ] + HADOOP_API

GUAVA    = [ "com.google.guava:guava:jar:11.0.2" ]
AVRO     = [ "org.apache.avro:avro:jar:1.7.3" ]
PROTOBUF = [ "com.google.protobuf:protobuf-java:jar:2.4.0a" ]

CDH_VERSION       = "cdh4.2.1"
HADOOP_MR_VERSION = "2.0.0-mr1-#{CDH_VERSION}"
HADOOP_VERSION    = "2.0.0-#{CDH_VERSION}"
HADOOP_ZK_VERSION = "3.4.5-#{CDH_VERSION}"

HADOOP = [
    "org.apache.hadoop:hadoop-core:jar:#{HADOOP_MR_VERSION}",
    "org.apache.hadoop:hadoop-tools:jar:#{HADOOP_MR_VERSION}",
    "org.apache.hadoop:hadoop-annotations:jar:#{HADOOP_VERSION}",
    "org.apache.hadoop:hadoop-auth:jar:#{HADOOP_VERSION}",
    "org.apache.hadoop:hadoop-client:jar:#{HADOOP_VERSION}",
    "org.apache.hadoop:hadoop-common:jar:#{HADOOP_VERSION}",
    "org.apache.hadoop:hadoop-hdfs:jar:#{HADOOP_VERSION}",
    "org.apache.zookeeper:zookeeper:jar:#{HADOOP_ZK_VERSION}"
] + ANT + COMMONS_CLI + COMMONS_CODEC + COMMONS_EL + COMMONS_LOGGING +
    COMMONS_NET + JETTY_6 + JUNIT + HADOOP_DEPS + HTTPCLIENT + GUAVA +
    COMMONS_CONFIGURATION + COMMONS_LANG + AVRO + PROTOBUF + COMMONS_COLLECTIONS

HADOOP_TEST = [
  "org.apache.hadoop:hadoop-test:jar:#{HADOOP_MR_VERSION}",
  "org.apache.hadoop:hadoop-minicluster:jar:#{HADOOP_MR_VERSION}",
  "org.apache.hadoop:hadoop-common:jar:tests:#{HADOOP_VERSION}",
  "org.apache.hadoop:hadoop-common:jar:test-sources:#{HADOOP_VERSION}",
  "org.apache.hadoop:hadoop-hdfs:jar:tests:#{HADOOP_VERSION}",
  "org.apache.hadoop:hadoop-hdfs:jar:test-sources:#{HADOOP_VERSION}",
] + JETTY + JACKSON


METRICS = [
    'com.yammer.metrics:metrics-core:jar:2.2.0',
    'com.yammer.metrics:metrics-jetty:jar:2.2.0',
    'com.yammer.metrics:metrics-graphite:jar:2.2.0'
    ]

DISRUPTOR = [ 'com.googlecode.disruptor:disruptor:jar:2.10.4' ]

JOLOKIA_JVM = [
    "com.googlecode.json-simple:json-simple:jar:1.1",
    "org.jolokia:jolokia-jvm-agent:jar:1.0.5"
    ]
download(artifact("org.jolokia:jolokia-jvm-agent:jar:1.0.5") => "http://mirrors.ibiblio.org/pub/mirrors/maven2/org/jolokia/jolokia-jvm/1.0.5/jolokia-jvm-1.0.5-agent.jar")

class Buildr::Artifact
  def <=>(other)
    self.id <=> other.id
  end
end

def add_artifacts(*args)
  args = [args].flatten
  arts = args.find_all{ |a| a.kind_of? Buildr::Artifact }
  arts += artifacts( args.reject{ |a| a.kind_of? Buildr::Artifact }.reject{|j| j =~ /:pom:/}.sort.uniq )
  arts.sort
end
