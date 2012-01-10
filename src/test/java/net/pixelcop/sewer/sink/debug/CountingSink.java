package net.pixelcop.sewer.sink.debug;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicInteger;

import net.pixelcop.sewer.Event;
import net.pixelcop.sewer.Sink;

public class CountingSink extends Sink {

  private final AtomicInteger count = new AtomicInteger();

  public CountingSink(String[] args) {
  }

  @Override
  public void close() throws IOException {
    setStatus(CLOSED);
  }

  @Override
  public void append(Event event) throws IOException {
    count.incrementAndGet();
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
  public int getCount() {
    return count.get();
  }

}
