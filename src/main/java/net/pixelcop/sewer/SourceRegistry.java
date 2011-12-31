package net.pixelcop.sewer;

import java.util.HashMap;
import java.util.Map;

import net.pixelcop.sewer.source.HttpPixelSource;
import net.pixelcop.sewer.source.PipeSource;
import net.pixelcop.sewer.source.SyslogTcpSource;
import net.pixelcop.sewer.source.TcpWriteableEventSource;

@SuppressWarnings("rawtypes")
public class SourceRegistry {

  private static final Map<String, Class> registry = new HashMap<String, Class>();

  static {
    register("tcpwrite", TcpWriteableEventSource.class);
    register("syslog", SyslogTcpSource.class);
    register("pipe", PipeSource.class);
    register("pixel", HttpPixelSource.class);
  }

  public static final void register(String name, Class clazz) {
    registry.put(name, clazz);
  }

  public static final Class get(String name) {
    return registry.get(name);
  }

  public static final boolean exists(String name) {
    return registry.containsKey(name);
  }

  public static Map<String, Class> getRegistry() {
    return registry;
  }

}
