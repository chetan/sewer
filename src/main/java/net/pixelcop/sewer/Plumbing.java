package net.pixelcop.sewer;

import java.io.Closeable;
import java.io.IOException;
import java.net.ServerSocket;
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
   * Initialize Source/Sink. This is where any ServerSockets should be created
   * but <strong>not</strong> started, e.g. {@link ServerSocket#accept()}, which
   * must happen during {@link #open()}.
   *
   * @throws IOException
   */
  public void init() throws IOException {
  }

  /**
   * Open the Source or Sink (file, network, etc). Sources are also responsible
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

  /**
   * Helper method for reading strings from arguments, with default value handling
   *
   * @param args
   * @param pos
   * @param defaultValue
   * @return
   */
  protected String getString(String[] args, int pos, String defaultValue) {

    if (args == null || args.length < pos + 1) {
      return defaultValue;
    }

    String arg = args[pos];
    if (arg == null || arg.isEmpty()) {
      return defaultValue;
    }

    return arg;
  }

  /**
   * Helper method for reading long values from arguments
   *
   * @param args String[]
   * @param pos int position in args array
   * @param defaultValue to return if arg is null or invalid
   * @return long
   */
  protected long getLong(String[] args, int pos, long defaultValue) {

    if (args == null || args.length < pos + 1) {
      return defaultValue;
    }

    String arg = args[pos];
    if (arg == null || arg.isEmpty()) {
      return defaultValue;
    }

    try {
      return Long.parseLong(arg);
    } catch (Throwable t) {
    }

    return defaultValue;
  }

  /**
   * Helper method for reading int values from arguments
   *
   * @param args String[]
   * @param pos int position in args array
   * @param defaultValue to return if arg is null or invalid
   * @return int
   */
  protected int getInt(String[] args, int pos, int defaultValue) {
    return (int) getLong(args, pos, defaultValue);
  }

}
