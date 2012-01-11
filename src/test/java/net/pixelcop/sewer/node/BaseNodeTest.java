package net.pixelcop.sewer.node;

import java.io.IOException;
import java.security.Permission;

import net.pixelcop.sewer.SinkRegistry;
import net.pixelcop.sewer.SourceRegistry;
import net.pixelcop.sewer.sink.TxTestHelper;
import net.pixelcop.sewer.sink.debug.CountingSink;
import net.pixelcop.sewer.sink.debug.FailOpenSink;
import net.pixelcop.sewer.source.debug.EventGeneratorSource;
import net.pixelcop.sewer.source.debug.FailOpenSource;
import net.pixelcop.sewer.source.debug.NullSource;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.runner.RunWith;
import org.junit.runners.BlockJUnit4ClassRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@RunWith(BlockJUnit4ClassRunner.class)
public abstract class BaseNodeTest extends Assert {

  private static class NoExitSecurityManager extends SecurityManager {
    @Override
    public void checkPermission(Permission perm) {
      // allow anything.
    }

    @Override
    public void checkPermission(Permission perm, Object context) {
      // allow anything.
    }

    @Override
    public void checkExit(int status) {
      super.checkExit(status);
      throw new ExitException(status);
    }
  }

  protected static final Logger LOG = LoggerFactory.getLogger(BaseNodeTest.class);

  private SecurityManager securityManager;

  @Before
  public void setup() throws Exception {
    securityManager = System.getSecurityManager();
    System.setSecurityManager(new NoExitSecurityManager());

    SourceRegistry.register("null", NullSource.class);
    SourceRegistry.register("gen", EventGeneratorSource.class);
    SourceRegistry.register("failopen", FailOpenSource.class);

    SinkRegistry.register("counting", CountingSink.class);
    SinkRegistry.register("failopen", FailOpenSink.class);
  }

  @After
  public void teardown() throws Exception {
    System.setSecurityManager(securityManager);
    cleanupNode(TestableNode.instance);
    cleanupNode(Node.instance);
  }

  @After
  public void cleanupTxHelpers() {
    TxTestHelper.cleanupAllHelpers();
  }

  @Before
  public void resetCountingSink() {
    CountingSink.reset();
  }

  /**
   * Configures a new node but does not start it
   *
   * @param source
   * @param sink
   * @return
   * @throws IOException
   */
  public TestableNode createNode(String source, String sink) throws IOException {
    NodeConfig conf = new NodeConfigurator().configure(new String[]{ "-v" });
    conf.set(NodeConfig.SOURCE, source);
    conf.set(NodeConfig.SINK, sink);

    TestableNode node = new TestableNode(conf);
    return node;
  }

  /**
   * Create a node and start it
   *
   * @param source
   * @param sink
   * @return
   * @throws IOException
   */
  public TestableNode createAndStartNode(String source, String sink) throws IOException {
    TestableNode node = createNode(source, sink);
    node.start();
    try {
      node.await();
    } catch (InterruptedException e) {
      fail("node startup interrupted");
    }
    return node;
  }

  public void cleanupNode(Node node) {

    if (node == null || node.getSource() == null) {
      return;
    }

    try {
      node.getSource().close();
    } catch (IOException e) {
      LOG.warn("error closing source", e);
    }

  }

}
