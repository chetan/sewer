package net.pixelcop.sewer.source.debug;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicInteger;

import net.pixelcop.sewer.Sink;
import net.pixelcop.sewer.Source;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.math.NumberUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class EventGeneratorSource extends Source {

  class GenThread extends Thread {
    private Sink sink;
    public GenThread() throws IOException {
      this.sink = createSink();
    }
    @Override
    public void run() {
      while (count.get() <= max) {
        String str = new String("Event " + count.incrementAndGet() + " ");
        if (str.length() < length) {
          str = str + StringUtils.repeat('X', length - str.length());
        }
        try {
          this.sink.append(new StringEvent(str));
        } catch (IOException e) {
          LOG.debug("Error appending", e);
        }
      }
    }
  }

  private static final Logger LOG = LoggerFactory.getLogger(EventGeneratorSource.class);

  private static final int DEFAULT_MAX = 1000;
  private static final int DEFAULT_LENGTH = 32;

  private final AtomicInteger count = new AtomicInteger();
  private final int max;
  private final int length;

  public EventGeneratorSource(String[] args) {
    if (args != null && args.length >= 1) {
      this.max = NumberUtils.toInt(args[0], DEFAULT_MAX);
      this.length = args.length >= 2 ? NumberUtils.toInt(args[1], DEFAULT_LENGTH) : DEFAULT_LENGTH;

    } else {
      this.max = DEFAULT_MAX;
      this.length = DEFAULT_LENGTH;
    }

  }

  @Override
  public void close() throws IOException {
    setStatus(CLOSED);
  }

  @Override
  public Class<?> getEventClass() {
    return StringEvent.class;
  }

  @Override
  public void open() throws IOException {
    new GenThread().start();
    setStatus(FLOWING);
  }

  public boolean isFinished() {
    return (count.get() == max);
  }

}
