
COMMONS_CODEC = [ "commons-codec:commons-codec:jar:1.4" ]
COMMONS_COLLECTIONS = [ "commons-collections:commons-collections:jar:3.2" ]
COMMONS_FILEUPLOAD = [ "commons-fileupload:commons-fileupload:jar:1.2.1" ]
COMMONS_IO = [ "commons-io:commons-io:jar:1.3.2" ]
COMMONS_LANG = [ "commons-lang:commons-lang:jar:2.4" ]
COMMONS_LANG3 = [ 'org.apache.commons:commons-lang3:jar:3.1' ]
COMMONS_LOGGING = [ "commons-logging:commons-logging:jar:1.1.1" ]
COMMONS_POOL = [ "commons-pool:commons-pool:jar:1.5.7" ]
COMMONS_NET = [ "commons-net:commons-net:jar:2.0" ]
COMMONS_DISCOVERY = [ "commons-discovery:commons-discovery:jar:0.4" ]
COMMONS_CLI = [ "commons-cli:commons-cli:jar:1.2" ]
COMMONS_EL = [ "commons-el:commons-el:jar:1.0" ]
COMMONS_CSV = [ "commons-csv:commons-csv:jar:1.0" ]

SOLR_COMMONS_CSV = [ "org.apache.solr:solr-commons-csv:jar:1.4.1" ]

LOGGER = [ "org.slf4j:slf4j-api:jar:1.6.4", 'org.slf4j:slf4j-log4j12:jar:1.6.4', 'log4j:log4j:jar:1.2.16' ]

JSON_SMART = [ 'net.minidev:json-smart:jar:1.0.9-1' ]

JACKSON = [
    "org.codehaus.jackson:jackson-core-asl:jar:1.9.3",
    "org.codehaus.jackson:jackson-mapper-asl:jar:1.9.3"
    ]

GUAVA = [ 'com.google.guava:guava:jar:11.0' ]

HTTPCLIENT = [
    "commons-httpclient:commons-httpclient:jar:3.1",
    ] + COMMONS_CODEC + COMMONS_LOGGING

JUNIT = [ "junit:junit:jar:4.10" ]

ANT = [ "org.apache.ant:ant:jar:1.8.0" ]

JETTY_VERSION = "7.5.4.v20111024"
JETTY = [
  "javax.servlet:servlet-api:jar:2.5",
  "org.eclipse.jetty:jetty-continuation:jar:#{JETTY_VERSION}",
  "org.eclipse.jetty:jetty-http:jar:#{JETTY_VERSION}",
  "org.eclipse.jetty:jetty-io:jar:#{JETTY_VERSION}",
  "org.eclipse.jetty:jetty-server:jar:#{JETTY_VERSION}",
  "org.eclipse.jetty:jetty-util:jar:#{JETTY_VERSION}"
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
    ]

HADOOP_VERSION = "0.20.2-cdh3u1"
HADOOP = [
    "org.apache.hadoop:hadoop-core:jar:#{HADOOP_VERSION}",
    "org.apache.hadoop:hadoop-tools:jar:#{HADOOP_VERSION}"
] + ANT + COMMONS_CLI + COMMONS_CODEC + COMMONS_EL + COMMONS_LOGGING + COMMONS_NET + JETTY + JUNIT + HADOOP_DEPS + HTTPCLIENT

class Buildr::Artifact
  def <=>(other)
    self.id <=> other.id
  end
end
