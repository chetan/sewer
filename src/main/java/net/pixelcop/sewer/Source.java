package net.pixelcop.sewer;

import java.io.Closeable;
import java.io.IOException;

public abstract class Source implements Closeable {

  private SourceSinkFactory<Sink> sinkFactory;;

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

  public Sink createSink() throws IOException {
    Sink sink = this.sinkFactory.build();
    sink.open();
    return sink;
  }

}
