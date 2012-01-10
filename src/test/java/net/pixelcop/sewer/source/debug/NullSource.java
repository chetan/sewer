package net.pixelcop.sewer.source.debug;

import java.io.IOException;

import net.pixelcop.sewer.Source;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class NullSource extends Source {

  private static final Logger LOG = LoggerFactory.getLogger(NullSource.class);

  public NullSource(String[] args) {
  }

  @Override
  public void close() throws IOException {
    LOG.debug("close");
    setStatus(CLOSED);
  }

  @Override
  public Class<?> getEventClass() {
    return null;
  }

  @Override
  public void open() throws IOException {
    LOG.debug("open");
    setStatus(FLOWING);
  }

}
