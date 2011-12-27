package net.pixelcop.sewer;

import java.io.Closeable;
import java.io.IOException;

public abstract class Source implements Closeable {

  private Sink sink;

  public abstract void open() throws IOException;

  public void setSink(Sink sink) {
    this.sink = sink;
  }

  public Sink getSink() {
    return sink;
  }

}
