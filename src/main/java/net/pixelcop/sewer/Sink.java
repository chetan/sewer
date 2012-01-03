package net.pixelcop.sewer;

import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class Sink extends Plumbing {

  private static final Logger LOG = LoggerFactory.getLogger(Sink.class);

  protected Sink subSink = null;

  /**
   * Append an Event to the Sink
   * @param event
   * @throws IOException
   */
  public abstract void append(Event event) throws IOException;

  public void setSubSink(Sink subSink) {
    this.subSink = subSink;
  }

  public Sink getSubSink() {
    return subSink;
  }

  /**
   * Create the SubSink, if possible
   *
   * @return True if a SubSink was created, false otherwise.
   */
  public boolean createSubSink() {
    if (getSinkFactory() == null) {
      return false;
    }

    subSink = getSinkFactory().build();
    return true;
  }

  @Override
  protected void finalize() throws Throwable {
    if (getStatus() != CLOSED) {
      try {
        close();
      } catch (Throwable t) {
        LOG.warn("Caught during finalizer close(): " + t.getMessage(), t);
        // rethrow? this should be good enough since the object was disposed of anyway
      }
    }
  }

}
