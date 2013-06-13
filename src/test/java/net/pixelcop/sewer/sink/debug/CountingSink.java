package net.pixelcop.sewer.sink.debug;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicInteger;

import net.pixelcop.sewer.DrainSink;
import net.pixelcop.sewer.Event;
import net.pixelcop.sewer.Sink;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@DrainSink
public class CountingSink extends Sink {

  private static final Logger LOG = LoggerFactory.getLogger(CountingSink.class);

  private static final AtomicInteger appendCount = new AtomicInteger();
  private static final AtomicInteger openCount = new AtomicInteger();
  private static final AtomicInteger closeCount = new AtomicInteger();

  public CountingSink(String[] args) {
  }

  @Override
  public void close() throws IOException {
    LOG.debug("close");
    setStatus(CLOSING);
    if (subSink != null) {
      subSink.close();
    }
    closeCount.incrementAndGet();
    setStatus(CLOSED);
  }

  @Override
  public void append(Event event) throws IOException {
    appendCount.incrementAndGet();
    if (subSink != null) {
      subSink.append(event);
    }
  }

  @Override
  public void open() throws IOException {
    LOG.debug("open");
    setStatus(OPENING);
    if (createSubSink()) {
      subSink.open();
    }
    openCount.incrementAndGet();
    setStatus(FLOWING);
  }

  /**
   * Gets the number of events appended to the sink
   *
   * @return int Append count
   */
  public static int getAppendCount() {
    return appendCount.get();
  }

  /**
   * Gets the open count
   * @return
   */
  public static int getOpenCount() {
    return openCount.get();
  }

  /**
   * Get the close count
   * @return
   */
  public static int getCloseCount() {
    return closeCount.get();
  }

  /**
   * Reset counters
   */
  public static void reset() {
    appendCount.set(0);
    openCount.set(0);
    closeCount.set(0);
  }

}
