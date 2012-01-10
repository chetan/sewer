package net.pixelcop.sewer.sink.debug;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicInteger;

import net.pixelcop.sewer.Event;
import net.pixelcop.sewer.Sink;

public class CountingSink extends Sink {

  private static final AtomicInteger count = new AtomicInteger();

  public CountingSink(String[] args) {
  }

  @Override
  public void close() throws IOException {
    setStatus(CLOSED);
  }

  @Override
  public void append(Event event) throws IOException {
    synchronized (event) {
      count.incrementAndGet();
    }
  }

  @Override
  public void open() throws IOException {
    setStatus(FLOWING);
  }

  /**
   * Gets the number of events appended to the sink
   *
   * @return int Append count
   */
  public static int getCount() {
    return count.get();
  }

  public static void reset() {
    count.set(0);
  }

}
