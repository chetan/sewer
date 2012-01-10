package net.pixelcop.sewer.source.debug;

import java.io.IOException;

public class FailOpenSource extends NullSource {

  public FailOpenSource(String[] args) {
    super(args);
  }

  @Override
  public void open() throws IOException {
    throw new IOException("fail!");
  }
}
