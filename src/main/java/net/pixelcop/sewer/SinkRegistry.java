package net.pixelcop.sewer;

import java.util.HashMap;
import java.util.Map;

import net.pixelcop.sewer.sink.DfsSink;
import net.pixelcop.sewer.sink.SequenceFileSink;
import net.pixelcop.sewer.sink.TcpWriteableEventSink;
import net.pixelcop.sewer.sink.buffer.AsyncBufferSink;
import net.pixelcop.sewer.sink.buffer.BufferPoolSink;
import net.pixelcop.sewer.sink.buffer.DisruptorSink;
import net.pixelcop.sewer.sink.buffer.ThreadLocalBufferSink;
import net.pixelcop.sewer.sink.debug.ConsoleSink;
import net.pixelcop.sewer.sink.debug.DelayedOpenSink;
import net.pixelcop.sewer.sink.debug.NullSink;
import net.pixelcop.sewer.sink.durable.ReliableSequenceFileSink;
import net.pixelcop.sewer.sink.durable.ReliableSink;
import net.pixelcop.sewer.sink.durable.RollSink;

@SuppressWarnings("rawtypes")
public class SinkRegistry {

  private static final Map<String, Class> registry = new HashMap<String, Class>();

  static {
    // endpoints
    register("dfs", DfsSink.class);
    register("seqfile", SequenceFileSink.class);
    register("reliableseq", ReliableSequenceFileSink.class);
    register("tcpwrite", TcpWriteableEventSink.class);

    // decorators
    register("reliable", ReliableSink.class);
    register("roll", RollSink.class);
    register("delayed_open", DelayedOpenSink.class);
    register("buffer", AsyncBufferSink.class);
    register("buffer_pool", BufferPoolSink.class);
    register("local_buffer", ThreadLocalBufferSink.class);
    register("disruptor", DisruptorSink.class);

    // debug
    register("null", NullSink.class);
    register("console", ConsoleSink.class);
    register("stdout", ConsoleSink.class);
  }

  public static final void register(String name, Class clazz) {
    registry.put(name.toLowerCase(), clazz);
  }

  public static Map<String, Class> getRegistry() {
    return registry;
  }

}
