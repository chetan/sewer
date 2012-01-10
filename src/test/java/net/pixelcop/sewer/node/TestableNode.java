package net.pixelcop.sewer.node;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;

public class TestableNode extends Node {

  private int exitCaught = 0;

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

}
