package net.pixelcop.sewer;

import java.util.HashMap;
import java.util.Map;

import net.pixelcop.sewer.sink.DfsSink;
import net.pixelcop.sewer.sink.NullSink;
import net.pixelcop.sewer.sink.SequenceFileSink;
import net.pixelcop.sewer.sink.TcpWriteableEventSink;

@SuppressWarnings("rawtypes")
public class SinkRegistry {

  private static final Map<String, Class> registry = new HashMap<String, Class>();

  static {
    register("null", NullSink.class);
    register("dfs", DfsSink.class);
    register("seqfile", SequenceFileSink.class);
    register("tcpwrite", TcpWriteableEventSink.class);
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

}
