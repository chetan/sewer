package net.pixelcop.sewer.sink.durable;

import java.io.IOException;

import net.pixelcop.sewer.Event;
import net.pixelcop.sewer.Sink;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Delays opening of the SubSink until the first {@link #append(Event)} call. This allows us to
 * avoid useless empty file creation when no Events are actually being received.
 *
 * @author chetan
 *
 */
public class DelayedOpenSink extends Sink {

  private static final Logger LOG = LoggerFactory.getLogger(DelayedOpenSink.class);

  public DelayedOpenSink(String[] args) {
  }

  @Override
  public void close() throws IOException {
    if (getStatus() == FLOWING) {
      LOG.debug("closing subsink");
      setStatus(CLOSING);
      getSubSink().close();
      setStatus(CLOSED);
    } else {
      LOG.debug("subsink not open, nothing to do");
    }
  }

  @Override
  public void append(Event event) throws IOException {
    if (getStatus() == CLOSED) {
      setStatus(OPENING);
      LOG.debug("append called: opening subsink");
      createSubSink();
      getSubSink().open();
      setStatus(FLOWING);
    }
    while (getStatus() != FLOWING) {
    }
    getSubSink().append(event);
  }

  @Override
  public void open() throws IOException {
    // no-op
    LOG.debug("open called, but delaying subsink.open until append");
  }

}
