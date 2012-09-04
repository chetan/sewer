
require 'lock_jar/buildr'

# Version number for this release
VERSION_NUMBER = "0.4.6"
# Group identifier for your projects
GROUP = "net.pixelcop.sewer"
VENDOR = "Pixelcop Research, Inc."
URL = "https://github.com/chetan/sewer"
MAIN_CLASS = 'net.pixelcop.sewer.node.Node'

# Specify Maven 2.0 remote repositories here, like this:
repositories.remote << "http://mirrors.ibiblio.org/pub/mirrors/maven2"
repositories.remote << "https://repository.cloudera.com/content/repositories/releases/"

require "buildfile_libraries.rb"

lock_jar do
  # repos
	local '~/.m2/repository'

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
end

desc "The Sewer project"
define "sewer" do

  SEWER_JARS = add_artifacts(lock_jars)
  SEWER_TEST_JARS = add_artifacts(lock_jars(["compile", "runtime", "test"]))
  RUN_JARS = add_artifacts( JOLOKIA_JVM )

  project.version = VERSION_NUMBER
  project.group = GROUP
  manifest["Implementation-Vendor"] = VENDOR
  manifest["Implementation-URL"] = URL
  manifest["Implementation-Version"] = VERSION_NUMBER
  manifest["Build-Date"] = Time.new.to_s
  manifest["Copyright"] = "#{VENDOR} (C) #{Time.new.strftime('%Y')}"
  manifest["Build-Jdk"] = `javac -version`
  manifest["Main-Class"] = MAIN_CLASS

  #compile.with SEWER_JARS
  resources

  #test.compile.with SEWER_TEST_JARS
  test.resources

  run.using :main => MAIN_CLASS

  package(:jar)
  package(:sources)
  #package(:javadoc)

  package(:tgz).path("#{id}-#{version}").tap do |path|
    path.include "README.md"
    path.include "LICENSE"
    path.include package(:jar), package(:sources)
    path.path("lib").include lock_jars, RUN_JARS
    path.include "bin"
    path.include "conf"
  end
end

# Backward compatibility:  Buildr 1.4+ uses $HOME/.buildr/buildr.rb
local_config = File.expand_path('buildr.rb', File.dirname(__FILE__))
load local_config if File.exist? local_config

