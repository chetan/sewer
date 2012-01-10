package net.pixelcop.sewer.node;

import static org.junit.Assert.*;

import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;

import net.pixelcop.sewer.Plumbing;
import net.pixelcop.sewer.sink.debug.NullSink;
import net.pixelcop.sewer.source.debug.NullSource;

import org.junit.Test;

public class TestNodeWiring extends BaseNodeTest {

  @Test
  public void testBasicSetup() throws IOException {

    TestableNode node = createNode("null", "null");

    basicTests(node);
  }

  private void basicTests(Node node) {
    assertNotNull(node.getSource());
    assertNotNull(node.getSinkFactory());
    assertNotNull(node.getConf());

    assertEquals("null", node.getConf().get(NodeConfig.SOURCE));
    assertEquals("null", node.getConf().get(NodeConfig.SINK));

    assertEquals(1, node.getSinkFactory().getClasses().size());

    assertEquals(NullSource.class.getName(), node.getSource().getClass().getName());
    assertEquals(NullSink.class.getName(), node.getSinkFactory().getClasses().get(0).getClazz().getName());

    assertEquals(Plumbing.CLOSED, node.getSource().getStatus());

    try {
      if (node instanceof TestableNode) {
        node.start();
        ((TestableNode) node).await(); // need to wait because we are starting a new thread above
      } else {
        Thread.sleep(250);
      }

    } catch (InterruptedException e) {
    }

    assertEquals(Plumbing.FLOWING, node.getSource().getStatus());
  }

  @Test
  public void testBadCommandLine() {
    try {
      new NodeConfigurator().configure(new String[]{ "-x" });
      fail("expected exit");
    } catch (ExitException e) {
      assertEquals(2, e.status);
    }
  }

  @Test
  public void testInvalidConfigFile() {
    try {
      new NodeConfigurator().configure(new String[]{ "-c", "/foo/bar/asdfxyz" });
      fail("expected exit");
    } catch (ExitException e) {
      assertEquals(2, e.status);
    }
  }

  public void testValidConfigFile() throws URISyntaxException {
    File file = new File(getClass().getClassLoader().getResource("config.properties").toURI());
    new NodeConfigurator().configure(new String[]{ "-c", file.toString() });
  }

  @Test
  public void testHelpExits() {
    try {
      new NodeConfigurator().configure(new String[]{ "-h" });
      fail("expected exit");
    } catch (ExitException e) {
      assertEquals(1, e.status);
    }
  }

  @Test
  public void testSourceFailsToOpenCausesExit() throws IOException {

    NodeConfig conf = new NodeConfigurator().configure(new String[]{ "-v" });
    conf.set(NodeConfig.SOURCE, "failopen");
    conf.set(NodeConfig.SINK, "null");

    TestableNode node = null;
    try {
      node = new TestableNode(conf);
    } catch (IOException e) {
      fail("shouldn't raise here");
    }
    node.start();
    try {
      node.await();
    } catch (ExitException e) {
      return;

    } catch (InterruptedException e) {
    }

    fail("should have thrown ExitException");
  }

  @Test
  public void testMainCreatesNewNode() {

    Node.main(null);
    basicTests(Node.getInstance());

  }

}
