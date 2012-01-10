package net.pixelcop.sewer;

import net.pixelcop.sewer.node.BaseNodeTest;
import net.pixelcop.sewer.sink.debug.NullSink;
import net.pixelcop.sewer.sink.durable.ReliableSink;
import net.pixelcop.sewer.sink.durable.RollSink;

import org.junit.Test;

public class TestSourceSinkFactory extends BaseNodeTest {

  @Test
  public void testCreateSimple() {
    SourceSinkFactory<Sink> factory = new SourceSinkFactory<Sink>("null", SinkRegistry.getRegistry());
    assertNotNull(factory);
    assertNotNull(factory.getClasses());
    assertEquals(1, factory.getClasses().size());
    assertEquals(NullSink.class, factory.getClasses().get(0).getClazz());

  }

  @Test
  public void testCreateChained() {

    SourceSinkFactory<Sink> factory = new SourceSinkFactory<Sink>("roll > reliable > null", SinkRegistry.getRegistry());
    assertNotNull(factory);
    assertNotNull(factory.getClasses());
    assertEquals(3, factory.getClasses().size());


    assertEquals(RollSink.class, factory.getClasses().get(0).getClazz());
    assertEquals(ReliableSink.class, factory.getClasses().get(1).getClazz());
    assertEquals(NullSink.class, factory.getClasses().get(2).getClazz());

    RollSink sink = (RollSink) factory.build();
    assertEquals(30*1000, sink.getInterval());
    assertEquals("RollSink()", factory.getClasses().get(0).toString());
  }

  @Test
  public void testCreateWithArgs() {
    SourceSinkFactory<Sink> factory = new SourceSinkFactory<Sink>("roll(10) > null", SinkRegistry.getRegistry());
    assertNotNull(factory);
    assertNotNull(factory.getClasses());
    assertEquals(2, factory.getClasses().size());

    RollSink sink = (RollSink) factory.build();
    assertEquals(10*1000, sink.getInterval());
    assertEquals("RollSink(10)", factory.getClasses().get(0).toString());
  }

  @Test
  public void testCreateWithQuotedArgs() {
    SourceSinkFactory<Sink> factory = new SourceSinkFactory<Sink>("roll('10') > null", SinkRegistry.getRegistry());
    assertNotNull(factory);
    assertNotNull(factory.getClasses());
    assertEquals(2, factory.getClasses().size());

    RollSink sink = (RollSink) factory.build();
    assertEquals(10*1000, sink.getInterval());
  }

}
