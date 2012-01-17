package net.pixelcop.sewer;

import java.io.Closeable;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

public abstract class Plumbing implements Closeable, StatusProvider {

  public static final Map<Integer, String> STATUS_MAP = new HashMap<Integer, String>();
  static {
    STATUS_MAP.put(CLOSED,  "CLOSED");
    STATUS_MAP.put(CLOSING, "CLOSING");
    STATUS_MAP.put(OPENING, "OPENING");
    STATUS_MAP.put(FLOWING, "FLOWING");
    STATUS_MAP.put(ERROR,   "ERROR");
  }

  private PlumbingFactory<Sink> sinkFactory;

  private AtomicInteger status = new AtomicInteger(CLOSED);

  /**
   * Open the source endpoint (file, network, etc). The source is also responsible
   * for creating and opening it's associated Sink as necessary.
   *
   * @throws IOException
   */
  public abstract void open() throws IOException;

  public void setSinkFactory(PlumbingFactory<Sink> sinkFactory) {
    this.sinkFactory = sinkFactory;
  }

  public PlumbingFactory<Sink> getSinkFactory() {
    return sinkFactory;
  }

  public void setStatus(int status) {
    this.status.set(status);
  }

  @Override
  public int getStatus() {
    return status.get();
  }

  public String getStatusString() {
    return STATUS_MAP.get(getStatus());
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
