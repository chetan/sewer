
# Version number for this release
VERSION_NUMBER = "0.1.0"
# Group identifier for your projects
GROUP = "net.pixelcop.sewer"
COPYRIGHT = "Pixelcop Research, Inc."

# Specify Maven 2.0 remote repositories here, like this:
repositories.remote << "http://mirrors.ibiblio.org/pub/mirrors/maven2"
repositories.remote << "https://repository.cloudera.com/content/repositories/releases/"

require "buildfile_libraries.rb"
SEWER_JARS = artifacts( [ HADOOP, LOGGER, JSON_SMART, GUAVA ].flatten.sort.uniq ).sort
SEWER_TEST_JARS = artifacts( [ SEWER_JARS ].flatten.sort.uniq ).sort

desc "The Sewer project"
define "sewer" do

  project.version = VERSION_NUMBER
  project.group = GROUP
  manifest["Implementation-Vendor"] = COPYRIGHT

  compile.with SEWER_JARS
  resources

  test.compile.with SEWER_TEST_JARS
  test.resources

  package(:jar)
end
