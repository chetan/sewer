package net.pixelcop.sewer.sink.durable;

import java.io.IOException;

import net.pixelcop.sewer.Sink;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Closes a Sink asynchronously
 * @author chetan
 *
 */
public class SinkCloserThread extends Thread {

  private static final Logger LOG = LoggerFactory.getLogger(SinkCloserThread.class);

  private Sink sink;

  public SinkCloserThread(Sink sink) {
    this.sink = sink;
    setName("SinkCloser " + getId());
  }

  @Override
  public void run() {
    try {
      this.sink.close();

    } catch (IOException e) {
      LOG.warn("Error while closing sink during rotate: " + e.getMessage(), e);
    }
  }
}