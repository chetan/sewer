package net.pixelcop.sewer;

import java.io.Closeable;
import java.io.IOException;

public abstract class Sink implements Closeable {

  public static final int CLOSED    = 0;
  public static final int OPENING   = 1;
  public static final int FLOWING   = 2;
  public static final int ERROR     = -1;


  protected int status = CLOSED;
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
    this.status = status;
  }

  public int getStatus() {
    return status;
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

}
