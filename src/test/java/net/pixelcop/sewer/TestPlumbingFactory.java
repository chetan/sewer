package net.pixelcop.sewer;

import net.pixelcop.sewer.node.AbstractNodeTest;
import net.pixelcop.sewer.node.ConfigurationException;
import net.pixelcop.sewer.sink.debug.NullSink;
import net.pixelcop.sewer.sink.durable.ReliableSink;
import net.pixelcop.sewer.sink.durable.RollSink;
import net.pixelcop.sewer.source.debug.NullSource;

import org.junit.Test;

public class TestPlumbingFactory extends AbstractNodeTest {

  @Test
  public void testCreateSimple() throws ConfigurationException {
    PlumbingFactory<Sink> factory = new PlumbingFactory<Sink>("null", SinkRegistry.getRegistry());
    assertNotNull(factory);
    assertNotNull(factory.getClasses());
    assertEquals(1, factory.getClasses().size());
    assertEquals(NullSink.class, factory.getClasses().get(0).getClazz());
  }

  @Test
  public void testCreateChained() throws ConfigurationException {

    PlumbingFactory<Sink> factory = new PlumbingFactory<Sink>("roll > reliable > null", SinkRegistry.getRegistry());
    assertNotNull(factory);
    assertNotNull(factory.getClasses());
    assertEquals(3, factory.getClasses().size());


    assertEquals(RollSink.class, factory.getClasses().get(0).getClazz());
    assertEquals(ReliableSink.class, factory.getClasses().get(1).getClazz());
    assertEquals(NullSink.class, factory.getClasses().get(2).getClazz());

    RollSink sink = (RollSink) factory.build();
    assertNotNull(sink);
    assertEquals(30*1000, sink.getInterval());
    assertEquals("RollSink()", factory.getClasses().get(0).toString());
  }

  @Test
  public void testCreateWithArgs() throws ConfigurationException {
    PlumbingFactory<Sink> factory = new PlumbingFactory<Sink>("roll(10) > null", SinkRegistry.getRegistry());
    assertNotNull(factory);
    assertNotNull(factory.getClasses());
    assertEquals(2, factory.getClasses().size());

    RollSink sink = (RollSink) factory.build();
    assertNotNull(sink);
    assertEquals(10*1000, sink.getInterval());
    assertEquals("RollSink(10)", factory.getClasses().get(0).toString());
  }

  @Test
  public void testCreateWithQuotedArgs() throws ConfigurationException {
    PlumbingFactory<Sink> factory = new PlumbingFactory<Sink>("roll('10') > null", SinkRegistry.getRegistry());
    assertNotNull(factory);
    assertNotNull(factory.getClasses());
    assertEquals(2, factory.getClasses().size());

    RollSink sink = (RollSink) factory.build();
    assertNotNull(sink);
    assertEquals(10*1000, sink.getInterval());
  }

  @Test
  public void testCreateSimpleSource() throws ConfigurationException {
    testSource("null");
    testSource("null()");
    testSource("null(foobar)");
  }

  private void testSource(String config) throws ConfigurationException {
    PlumbingFactory<Source> factory = new PlumbingFactory<Source>(config, SourceRegistry.getRegistry());
    assertNotNull(factory);
    assertNotNull(factory.getClasses());
    assertEquals(1, factory.getClasses().size());
    assertEquals(NullSource.class, factory.getClasses().get(0).getClazz());
  }

  @Test
  public void testAcceptsUppercaseName() throws ConfigurationException {
    testSource("NULL");
  }

}
