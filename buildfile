
# Version number for this release
VERSION_NUMBER = "0.4.5"
# Group identifier for your projects
GROUP = "net.pixelcop.sewer"
VENDOR = "Pixelcop Research, Inc."
URL = "https://github.com/chetan/sewer"
MAIN_CLASS = 'net.pixelcop.sewer.node.Node'

# Specify Maven 2.0 remote repositories here, like this:
repositories.remote << "http://mirrors.ibiblio.org/pub/mirrors/maven2"
repositories.remote << "https://repository.cloudera.com/content/repositories/releases/"

require "buildfile_libraries.rb"
SEWER_JARS = add_artifacts( HADOOP, LOGGER, JSON_SMART, GUAVA, COMMONS_LANG3,
                            JACKSON, JETTY, COMMONS_POOL, COMMONS_DAEMON,
                            METRICS, DISRUPTOR )
SEWER_TEST_JARS = SEWER_JARS + add_artifacts( HADOOP_TEST )
RUN_JARS = add_artifacts( JOLOKIA_JVM )

desc "The Sewer project"
define "sewer" do

  project.version = VERSION_NUMBER
  project.group = GROUP
  manifest["Implementation-Vendor"] = VENDOR
  manifest["Implementation-URL"] = URL
  manifest["Implementation-Version"] = VERSION_NUMBER
  manifest["Build-Date"] = Time.new.to_s
  manifest["Copyright"] = "#{VENDOR} (C) #{Time.new.strftime('%Y')}"
  manifest["Build-Jdk"] = `javac -version`
  manifest["Main-Class"] = MAIN_CLASS

  compile.with SEWER_JARS
  resources

  test.compile.with SEWER_TEST_JARS
  test.resources

  run.using :main => MAIN_CLASS

  package(:jar)
  package(:sources)
  #package(:javadoc)

  package(:tgz).path("#{id}-#{version}").tap do |path|
    path.include "README.md"
    path.include "LICENSE"
    path.include package(:jar), package(:sources)
    path.path("lib").include SEWER_JARS, RUN_JARS
    path.include "bin"
    path.include "conf"
  end
end

# Backward compatibility:  Buildr 1.4+ uses $HOME/.buildr/buildr.rb
local_config = File.expand_path('buildr.rb', File.dirname(__FILE__))
load local_config if File.exist? local_config

