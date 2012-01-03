package net.pixelcop.sewer;

import java.io.Closeable;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicInteger;

public abstract class Plumbing implements Closeable {

  public static final int CLOSED    = 0;
  public static final int CLOSING   = 3;
  public static final int OPENING   = 1;
  public static final int FLOWING   = 2;
  public static final int ERROR     = -1;

  private SourceSinkFactory<Sink> sinkFactory;

  private AtomicInteger status = new AtomicInteger(CLOSED);

  /**
   * Open the source endpoint (file, network, etc). The source is also responsible
   * for creating and opening it's associated Sink as necessary.
   *
   * @throws IOException
   */
  public abstract void open() throws IOException;

  public void setSinkFactory(SourceSinkFactory<Sink> sinkFactory) {
    this.sinkFactory = sinkFactory;
  }

  public SourceSinkFactory<Sink> getSinkFactory() {
    return sinkFactory;
  }

  public void setStatus(int status) {
    this.status.set(status);
  }

  public int getStatus() {
    return status.get();
  }

  /**
   * Creates and opens the configured Sink
   *
   * @return {@link Sink}
   * @throws IOException
   */
  public Sink createSink() throws IOException {
    Sink sink = this.sinkFactory.build();
    sink.open();
    return sink;
  }

}
