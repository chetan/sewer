package net.pixelcop.sewer;

import java.io.Closeable;
import java.io.IOException;

public interface Sink extends Closeable {

  public void open() throws IOException;

  public void append(Event event) throws IOException;

}
