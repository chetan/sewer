package net.pixelcop.sewer;

import java.io.Closeable;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class Sink implements Closeable {

  private static final Logger LOG = LoggerFactory.getLogger(Sink.class);

  public static final int CLOSED    = 0;
  public static final int OPENING   = 1;
  public static final int FLOWING   = 2;
  public static final int ERROR     = -1;


  private AtomicInteger status = new AtomicInteger(CLOSED);
  protected Sink subSink = null;

  protected SourceSinkFactory<Sink> sinkFactory;


  /**
   * Open the Sink
   *
   * @throws IOException
   */
  public abstract void open() throws IOException;

  /**
   * Append an Event to the Sink
   * @param event
   * @throws IOException
   */
  public abstract void append(Event event) throws IOException;


  public void setStatus(int status) {
    this.status.set(status);
  }

  public int getStatus() {
    return status.get();
  }

  public void setSubSink(Sink subSink) {
    this.subSink = subSink;
  }

  public Sink getSubSink() {
    return subSink;
  }

  public void setSinkFactory(SourceSinkFactory<Sink> sinkFactory) {
    this.sinkFactory = sinkFactory;
  }

  public SourceSinkFactory<Sink> getSinkFactory() {
    return sinkFactory;
  }

  /**
   * Create the SubSink, if possible
   *
   * @return True if a SubSink was created, false otherwise.
   */
  public boolean createSubSink() {
    if (sinkFactory == null) {
      return false;
    }

    subSink = sinkFactory.build();
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
