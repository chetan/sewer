package net.pixelcop.sewer.sink;

import java.io.IOException;

import net.pixelcop.sewer.Event;
import net.pixelcop.sewer.Sink;

public class NullSink extends Sink {

  @Override
  public void close() throws IOException {
    setStatus(CLOSED);
  }

  @Override
  public void open() throws IOException {
    setStatus(FLOWING);
  }

  @Override
  public void append(Event event) throws IOException {
  }

}
