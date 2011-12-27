package net.pixelcop.sewer.sink;

import java.io.IOException;

import net.pixelcop.sewer.Event;
import net.pixelcop.sewer.Sink;

public class NullSink implements Sink {

  @Override
  public void close() throws IOException {
  }

  @Override
  public void open() throws IOException {
  }

  @Override
  public void append(Event event) throws IOException {
  }

}
