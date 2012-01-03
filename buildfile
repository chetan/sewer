
# Version number for this release
VERSION_NUMBER = "0.1.0"
# Group identifier for your projects
GROUP = "net.pixelcop.sewer"
VENDOR = "Pixelcop Research, Inc."
URL = "https://github.com/chetan/sewer"
MAIN_CLASS = 'net.pixelcop.sewer.node.Node'

# Specify Maven 2.0 remote repositories here, like this:
repositories.remote << "http://mirrors.ibiblio.org/pub/mirrors/maven2"
repositories.remote << "https://repository.cloudera.com/content/repositories/releases/"

require "buildfile_libraries.rb"
SEWER_JARS = artifacts( [ HADOOP, LOGGER, JSON_SMART, GUAVA, COMMONS_LANG3 ].flatten.sort.uniq ).sort
SEWER_TEST_JARS = artifacts( [ SEWER_JARS ].flatten.sort.uniq ).sort

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


  package_with_sources
  #package_with_javadoc

  package(:jar)
  package(:tgz).path("#{id}-#{version}").tap do |path|
    path.include "README"
    path.include "LICENSE"
  end
end
