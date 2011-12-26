package net.pixelcop.sewer;

import java.io.Closeable;
import java.io.IOException;

public abstract interface Source extends Closeable {

  public void open() throws IOException;

}
