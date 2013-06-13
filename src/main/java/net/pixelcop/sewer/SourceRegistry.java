package net.pixelcop.sewer;

import java.util.HashMap;
import java.util.Map;

import net.pixelcop.sewer.source.PipeSource;
import net.pixelcop.sewer.source.SyslogTcpSource;
import net.pixelcop.sewer.source.TcpWriteableEventSource;
import net.pixelcop.sewer.source.debug.ThreadedEventGeneratorSource;
import net.pixelcop.sewer.source.http.HttpPixelSource;

@SuppressWarnings("rawtypes")
public class SourceRegistry {

  private static final Map<String, Class> registry = new HashMap<String, Class>();

  static {
    register("tcpwrite", TcpWriteableEventSource.class);
    register("syslog", SyslogTcpSource.class);
    register("pipe", PipeSource.class);
    register("pixel", HttpPixelSource.class);

    // debug
    register("tgen", ThreadedEventGeneratorSource.class);
  }

  public static final void register(String name, Class clazz) {
    registry.put(name.toLowerCase(), clazz);
  }

  public static Map<String, Class> getRegistry() {
    return registry;
  }

}
