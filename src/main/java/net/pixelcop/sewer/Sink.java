package net.pixelcop.sewer;

import java.io.Closeable;
import java.io.IOException;

public abstract class Sink implements Closeable {

  public static final int CLOSED = 0;
  public static final int FLOWING = 1;
  public static final int ERROR = 2;

  protected int status = CLOSED;
  protected Sink subSink = null;

  private SourceSinkFactory<Sink> sinkFactory;


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

  public void createSubSink() {
    if (sinkFactory == null) {
      return;
    }

  }

}
