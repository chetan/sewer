package net.pixelcop.sewer.sink.debug;

import java.io.IOException;

import net.pixelcop.sewer.Event;
import net.pixelcop.sewer.Sink;

public class ConsoleSink extends Sink {

  public ConsoleSink(String[] args) {
  }

  @Override
  public void close() throws IOException {
    setStatus(CLOSED);
  }

  @Override
  public void open() throws IOException {
    setStatus(FLOWING)
    ;
  }

  @Override
  public void append(Event event) throws IOException {
    System.out.println(event.toString());
  }

}
