package net.pixelcop.sewer.node;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;

public class TestableNode extends Node {

  /**
   * Used to hack node startup for testability
   */
  private final CountDownLatch latch = new CountDownLatch(1);

  public TestableNode(NodeConfig config) throws IOException {
    super(config);
    instance = this;
  }

  @Override
  public void run() {
    super.run();
    latch.countDown();
  }

  public void await() throws InterruptedException {
    this.latch.await();
  }

}
