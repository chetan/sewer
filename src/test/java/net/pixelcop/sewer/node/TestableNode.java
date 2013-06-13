package net.pixelcop.sewer.node;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;

import net.pixelcop.sewer.sink.durable.TestableTransactionManager;
import net.pixelcop.sewer.sink.durable.TxTestHelper;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TestableNode extends Node {

  private static final Logger LOG = LoggerFactory.getLogger(TestableNode.class);

  private static final List<TestableNode> nodes = new ArrayList<TestableNode>();

  private int exitCaught = 0;

  private TxTestHelper txTestHelper;

  /**
   * Used to hack node startup for testability
   */
  private final CountDownLatch latch = new CountDownLatch(1);

  public TestableNode(NodeConfig config) throws IOException {
    super(config);
    nodes.add(this);
  }

  @Override
  public void run() {
    try {
      super.run();
    } catch (ExitException e) {
      this.exitCaught = e.status;
    }
    latch.countDown();
  }

  public void await() throws InterruptedException {
    this.latch.await();
    if (exitCaught != 0) {
      throw new ExitException(exitCaught);
    }
  }

  public void setTxTestHelper(TxTestHelper txTestHelper) {
    this.txTestHelper = txTestHelper;
  }

  public TxTestHelper getTxTestHelper() {
    return txTestHelper;
  }

  public void cleanup() {
    cleanup(this);
  }

  public static void cleanup(Node node) {
    if (node == null) {
      return;
    }

    try {
      if (node.getSource() != null) {
        node.getSource().close();
      }
    } catch (IOException e) {
      LOG.warn("error during cleanup", e);
    }

    try {
      TestableTransactionManager.kill();
    } catch (InterruptedException e) {
      LOG.warn("error during cleanup", e);
    }
  }

  public static void cleanupAllNodes() {
    for (TestableNode node : nodes) {
      node.cleanup();
    }
    nodes.clear();
  }

}
