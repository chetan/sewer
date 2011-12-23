
# Version number for this release
VERSION_NUMBER = "0.1.0"
# Group identifier for your projects
GROUP = "net.pixelcop.sewer"
COPYRIGHT = "Pixelcop Research, Inc."

# Specify Maven 2.0 remote repositories here, like this:
repositories.remote << "http://mirrors.ibiblio.org/pub/mirrors/maven2"

desc "The Sewer project"
define "sewer" do

  project.version = VERSION_NUMBER
  project.group = GROUP
  manifest["Implementation-Vendor"] = COPYRIGHT
  compile.with # Add classpath dependencies
  resources
  test.compile.with # Add classpath dependencies
  test.resources
  package(:jar)
end
