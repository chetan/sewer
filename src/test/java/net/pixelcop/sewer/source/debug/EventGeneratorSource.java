package net.pixelcop.sewer.source.debug;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicInteger;

import net.pixelcop.sewer.Sink;
import net.pixelcop.sewer.Source;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.math.NumberUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A single-threaded source which simply generates the specified number of Events on open() and
 * exits.
 *
 * <p><strong>NOTE:</strong> {@link #open()} is a <em>blocking</em> call.
 *
 * @author chetan
 *
 */
public class EventGeneratorSource extends Source {

  private static final Logger LOG = LoggerFactory.getLogger(EventGeneratorSource.class);

  private static final int DEFAULT_MAX = 1000;
  private static final int DEFAULT_LENGTH = 32;
  private static final int DEFAULT_DELAY = 0;

  protected final AtomicInteger count = new AtomicInteger();

  protected int max;
  protected int length;
  protected int delay;

  protected Sink sink;

  /**
   * arg 0 = # of events
   * arg 1 = length of event
   * arg 2 = delay in ms
   * @param args
   */
  public EventGeneratorSource(String[] args) {
    if (args == null || args.length == 0) {
      return;
    }

    if (args.length >= 1) {
      this.max = NumberUtils.toInt(args[0], DEFAULT_MAX);
    }
    if (args.length >= 2) {
      this.length = NumberUtils.toInt(args[1], DEFAULT_LENGTH);
    }
    if (args.length >= 3) {
      this.delay = NumberUtils.toInt(args[2], DEFAULT_DELAY);
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
    setStatus(OPENING);
    this.sink = createSink();

    setStatus(FLOWING);
    LOG.debug("Generating " + max + " events");
    generateEvents();
    LOG.debug("Done");
    setStatus(CLOSED);
  }

  public boolean isFinished() {
    return (count.get() == max);
  }

  public void generateEvents() {

    while (count.get() < max) {
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
