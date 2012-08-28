
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
  artifacts( [ args ].flatten.reject{|j| j =~ /:pom:/}.sort.uniq ).sort
end
