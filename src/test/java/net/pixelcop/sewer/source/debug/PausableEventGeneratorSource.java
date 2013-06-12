package net.pixelcop.sewer.source.debug;

import java.io.IOException;
import java.util.concurrent.CyclicBarrier;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PausableEventGeneratorSource extends EventGeneratorSource {

  private static final Logger LOG = LoggerFactory.getLogger(PausableEventGeneratorSource.class);

  private CyclicBarrier barrier;

  public PausableEventGeneratorSource(String[] args) {
    super(args);
    barrier = new CyclicBarrier(2);
  }

  public CyclicBarrier getBarrier() {
    return barrier;
  }


  @Override
  public void generateEvents() {

    while (count.get() < max) {

      // pause if reached half of max
      if (count.get() == max / 2) {
        try {
          LOG.debug("pausing....");
          barrier.await(); // once to signal to test that we have reached this point

          barrier.await(); // again to let test change state
          LOG.debug("resuming event generation..");
        } catch (Throwable t) {
          LOG.error("broken barrier, bailing out", t);
          return;
        }
      }

      String str = new String("Event " + count.incrementAndGet() + " ");
      if (str.length() < length) {
        str = str + StringUtils.repeat('X', length - str.length());
      }
      try {
        this.sink.append(new StringEvent(str));
      } catch (IOException e) {
        LOG.debug("Error appending", e);
      }

      if (delay == 0) {
        continue;
      }
      try {
        Thread.sleep(delay);
      } catch (InterruptedException e) {
        break;
      }
    }

    try {
      sink.close();
    } catch (IOException e1) {
      LOG.error("Error closing sink", e1);
    }

  }

}
