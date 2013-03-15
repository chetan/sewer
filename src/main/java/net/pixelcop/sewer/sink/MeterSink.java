package net.pixelcop.sewer.sink;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

import net.pixelcop.sewer.Event;
import net.pixelcop.sewer.Sink;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.Meter;

/**
 * Inject's a {@link Meter} into the sink stream
 *
 * @author chetan
 *
 */
public class MeterSink extends Sink {

  private static final Logger LOG = LoggerFactory.getLogger(MeterSink.class);

  private static final String DEFAULT_NAME = "events";

  private final Meter meter;

  public MeterSink(String[] args) {

    String name = null;
    if (args != null && args.length > 0) {
      name = args[0];
      if (name != null) {
        name = name.trim();
      }
    }
    if (name == null || name.isEmpty()) {
      name = DEFAULT_NAME;
    }

    meter = Metrics.newMeter(MeterSink.class, name, name, TimeUnit.SECONDS);
  }

  @Override
  public void close() throws IOException {
    setStatus(CLOSING);
    meter.stop();
    subSink.close();
    setStatus(CLOSED);
  }

  @Override
  public void append(Event event) throws IOException {
    meter.mark();
    subSink.append(event);
  }

  @Override
  public void open() throws IOException {
    setStatus(OPENING);
    if (createSubSink()) {
      subSink.open();
    }
    setStatus(FLOWING);
  }

}
