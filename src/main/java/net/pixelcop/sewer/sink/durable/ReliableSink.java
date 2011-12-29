package net.pixelcop.sewer.sink.durable;

import java.io.IOException;

import net.pixelcop.sewer.Event;
import net.pixelcop.sewer.Sink;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Creates a new Transaction on open(). All appended Events until close() are then made durable
 * by persisting to disk until a proper close(). If close() fails, the batch is retried in another
 * thread.
 *
 * @author chetan
 *
 */
public class ReliableSink extends Sink {

  private static final Logger LOG = LoggerFactory.getLogger(ReliableSink.class);

  public ReliableSink(String[] args) {
  }

  @Override
  public void close() throws IOException {
    LOG.debug("closing");
    subSink.close();
    subSink = null;
  }

  @Override
  public void open() throws IOException {
    LOG.debug("open(): creating new TransactionSink");
    subSink = new TransactionSink();
    subSink.setSinkFactory(sinkFactory);
    subSink.open();

    setStatus(FLOWING);
  }

  @Override
  public void append(Event event) throws IOException {
    subSink.append(event);
  }

}
