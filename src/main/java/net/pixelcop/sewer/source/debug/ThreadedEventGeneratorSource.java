package net.pixelcop.sewer.source.debug;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import net.pixelcop.sewer.Sink;
import net.pixelcop.sewer.Source;

import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ThreadedEventGeneratorSource extends Source {

  class GeneratorThread extends Thread {

    long count;
    AtomicBoolean active;

    public GeneratorThread() {
      count = 0;
      active = new AtomicBoolean(true);
      setName("GeneratorThread-" + getId());
    }

    @Override
    public void run() {

      String str = StringUtils.repeat("X", length);

      while (active.get()) {
        try {
         sink.append(new StringEvent(str));
         count++;
        } catch (IOException e) {
          LOG.debug("Error appending", e);
        }
      }

    }
  }

  private static final Logger LOG = LoggerFactory.getLogger(ThreadedEventGeneratorSource.class);

  private Sink sink;

  protected final List<GeneratorThread> threads;
  protected int length;

  /**
   * Total number of generated events, across all threads (available after close)
   */
  protected long totalCount;

  /**
   * arg 0 = # of threads
   * arg 1 = # of bytes per event
   *
   * @param args
   */
  public ThreadedEventGeneratorSource(String[] args) {
    threads = new ArrayList<GeneratorThread>();

    int t = getInt(args, 0, 1);
    for (int i = 0; i < t; i++) {
      threads.add(new GeneratorThread());
    }

    length = getInt(args, 1, 32);
  }

  @Override
  public void close() throws IOException {
    LOG.debug("closing");
    setStatus(CLOSING);
    for (GeneratorThread thread : threads) {
      thread.active.set(false);
    }
    for (GeneratorThread thread : threads) {
      try {
        thread.join();
      } catch (InterruptedException e) {
      }
      totalCount += thread.count;
    }
    sink.close();
    setStatus(CLOSED);
    LOG.debug("closed");
  }

  @Override
  public Class<?> getEventClass() {
    return StringEvent.class;
  }

  @Override
  public void open() throws IOException {

    setStatus(OPENING);
    this.sink = createSink();

    for (GeneratorThread thread : threads) {
      thread.start();
    }

    setStatus(FLOWING);
  }

  public void resetCounters() {
    for (GeneratorThread thread : threads) {
      thread.count = 0;
    }
  }

  public long getTotalCount() {
    return totalCount;
  }

}
