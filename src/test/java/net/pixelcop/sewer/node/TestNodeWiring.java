package net.pixelcop.sewer.node;

import java.io.IOException;

import net.pixelcop.sewer.Plumbing;
import net.pixelcop.sewer.sink.debug.NullSink;
import net.pixelcop.sewer.source.debug.NullSource;

import org.junit.Test;

public class TestNodeWiring extends BaseNodeTest {

  @Test
  public void testBasicSetup() throws IOException {

    NodeConfig conf = new NodeConfigurator().configure(null);
    conf.set(NodeConfig.SOURCE, "null");
    conf.set(NodeConfig.SINK, "null");

    TestableNode node = new TestableNode(conf);

    assertNotNull(node.getSource());
    assertNotNull(node.getSinkFactory());
    assertNotNull(node.getConf());

    assertEquals("null", node.getConf().get(NodeConfig.SOURCE));
    assertEquals("null", node.getConf().get(NodeConfig.SINK));

    assertEquals(1, node.getSinkFactory().getClasses().size());

    assertEquals(NullSource.class.getName(), node.getSource().getClass().getName());
    assertEquals(NullSink.class.getName(), node.getSinkFactory().getClasses().get(0).getClazz().getName());

    assertEquals(Plumbing.CLOSED, node.getSource().getStatus());
    node.start();
    try {
      node.await(); // need to wait because we are starting a new thread above
    } catch (InterruptedException e) {
    }
    assertEquals(Plumbing.FLOWING, node.getSource().getStatus());

  }

}
