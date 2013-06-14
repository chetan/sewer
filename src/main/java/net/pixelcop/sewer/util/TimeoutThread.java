package net.pixelcop.sewer.util;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

public abstract class TimeoutThread extends Thread {

  private CountDownLatch latch;

  private Throwable error;

  public TimeoutThread() {
    latch = new CountDownLatch(1);
  }

  /**
   * Do the actual work
   *
   * @throws IOException
   */
  public abstract void work() throws Exception;

  @Override
  public void run() {
    try {
      work();
    } catch (Throwable t) {
      this.error = t;
    }
    latch.countDown();
  }

  /**
   * Wait the given amount of time for the job to complete
   *
   * @param timeout
   * @param unit
   * @return true if the job completed normally
   */
  public boolean await(long timeout, TimeUnit unit) {
    if (!isAlive()) {
      start();
    }

    try {
      return latch.await(timeout, unit);

    } catch (InterruptedException e) {
      this.interrupt();
      this.error = e;
    }

    return false;
  }

  public Throwable getError() {
    return error;
  }
}