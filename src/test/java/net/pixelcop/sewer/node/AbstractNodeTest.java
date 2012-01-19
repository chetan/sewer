package net.pixelcop.sewer.node;

import java.io.IOException;
import java.security.Permission;

import net.pixelcop.sewer.SinkRegistry;
import net.pixelcop.sewer.SourceRegistry;
import net.pixelcop.sewer.sink.debug.CountingSink;
import net.pixelcop.sewer.sink.debug.FailOpenSink;
import net.pixelcop.sewer.sink.durable.TestableTransactionManager;
import net.pixelcop.sewer.sink.durable.TransactionManager;
import net.pixelcop.sewer.sink.durable.TxTestHelper;
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
public abstract class AbstractNodeTest extends Assert {

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

  protected static final Logger LOG = LoggerFactory.getLogger(AbstractNodeTest.class);

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

  @Before
  public void resetCountingSink() {
    CountingSink.reset();
  }

  @Before
  @After
  public void teardownExistingTxMan() {
    if (TestableTransactionManager.getInstance() != null) {
      LOG.debug("Found existing TxMan, shutting it down");
      try {
        TestableTransactionManager.kill();
      } catch (InterruptedException e) {
      }
    }
  }


  @After
  public void teardown() throws Exception {
    System.setSecurityManager(securityManager);
    securityManager = null;
    TestableNode.cleanup(TestableNode.instance);
  }

  @After
  public void cleanupTxHelpers() {
    TxTestHelper.cleanupAllHelpers();
  }

  @After
  public void cleanupNodes() {
    TestableNode.cleanupAllNodes();
  }

  /**
   * Configures a new node but does not start it. The newly created node will also create a new
   * {@link TransactionManager} with a new temp WAL path.
   *
   * @param source
   * @param sink
   * @return {@link TestableNode}
   * @throws IOException
   */
  public TestableNode createNode(String source, String sink) throws IOException {
    return createNode(source, sink, null);
  }

  /**
   * Configures a new node but does not start it. The newly created node will also create a new
   * {@link TransactionManager} with the given WAL path.
   *
   * @param source
   * @param sink
   * @param tmpWalPath
   * @return {@link TestableNode}
   * @throws IOException
   */
  public TestableNode createNode(String source, String sink, String tmpWalPath) throws IOException {
    NodeConfig conf = new NodeConfigurator().configure(new String[]{ "-v" });
    conf.set(NodeConfig.SOURCE, source);
    conf.set(NodeConfig.SINK, sink);

    TxTestHelper helper = new TxTestHelper(conf, tmpWalPath);
    TestableNode node = new TestableNode(conf);
    node.setTxTestHelper(helper);
    helper.addTxMan();

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

}
