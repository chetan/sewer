package net.pixelcop.sewer.node;

import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;

import net.pixelcop.sewer.Plumbing;
import net.pixelcop.sewer.sink.debug.NullSink;
import net.pixelcop.sewer.source.debug.NullSource;

import org.junit.Test;

public class TestNodeWiring extends AbstractNodeTest {

  @Test
  public void testBasicSetup() throws IOException {

    TestableNode node = createNode("null", "null");

    basicTests(node);

    assertEquals(Plumbing.CLOSED, node.getSource().getStatus());
    try {
      node.start();
      node.await(); // need to wait because we are starting a new thread above
    } catch (InterruptedException e) {
    }
    assertEquals(Plumbing.FLOWING, node.getSource().getStatus());
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
  }

  @Test
  public void testBadCommandLine() throws IOException {
    try {
      loadTestConfig("-x");
      fail("expected exit");
    } catch (ExitException e) {
      assertEquals(2, e.status);
    }
  }

  @Test
  public void testInvalidConfigFile() throws IOException {
    try {
      loadTestConfig("-c", "/foo/bar/asdfxyz");
      fail("expected exit");
    } catch (ExitException e) {
      assertEquals(2, e.status);
    }
  }

  public void testValidConfigFile() throws URISyntaxException, IOException {
    File file = new File(getClass().getClassLoader().getResource("config.properties").toURI());
    loadTestConfig("-c", file.toString());
  }

  @Test
  public void testHelpExits() throws IOException {
    try {
      loadTestConfig("-h");
      fail("expected exit");
    } catch (ExitException e) {
      assertEquals(1, e.status);
    }
  }

  @Test
  public void testSourceFailsToOpenCausesExit() throws IOException {

    NodeConfig conf = loadTestConfig("-v");
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

  @Test(expected=ExitException.class)
  public void testNullConfigThrowsException() throws IOException {
    NodeConfig conf = new NodeConfig();
    TestableNode node = new TestableNode(conf);
  }

}
