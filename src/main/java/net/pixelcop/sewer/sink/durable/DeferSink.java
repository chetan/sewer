package net.pixelcop.sewer.sink.durable;

import java.io.IOException;

import net.pixelcop.sewer.Event;
import net.pixelcop.sewer.Sink;
import net.pixelcop.sewer.sink.BucketedSink;
import net.pixelcop.sewer.sink.SequenceFileSink;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Very similar to the {@link ReliableSink} except that it does not append events to the subsink.
 * Instead, it will mark all transactions as failed so that they can be pushed to the downstream
 * sink in a background thread.
 *
 * @author chetan
 *
 */
public class DeferSink extends Sink {

  private static final Logger LOG = LoggerFactory.getLogger(ReliableSink.class);

  private Transaction tx;
  private SequenceFileSink durableSink;

  public DeferSink(String[] args) {
    TransactionManager.getInstance().setSilentRollback(true);
  }

  @Override
  public void close() throws IOException {
    LOG.debug("closing");
    setStatus(CLOSING);

    try {
      durableSink.close();
    } catch (IOException e) {
      LOG.warn("Failed to close durable sink", e);
      // will proceed with rollback anyway
    }

    tx.rollback(); // always rollback!

    setStatus(CLOSED);
    LOG.debug("closed");
  }

  @Override
  public void open() throws IOException {
    LOG.debug("opening");
    setStatus(OPENING);

    createSubSink(); // create but don't open

    String nextBucket = null;
    if (subSink instanceof BucketedSink) {
      nextBucket = ((BucketedSink) subSink).generateNextBucket();
    }
    this.tx = TransactionManager.getInstance().startTx(nextBucket);
    String durablePath = tx.createTxPath(false);
    this.durableSink = new SequenceFileSink(new String[] { durablePath });

    try {
      this.durableSink.open();
    } catch (IOException e) {
      LOG.error("Error opening durable sink at path " + durablePath, e);
      throw e;
    }

    setStatus(FLOWING);
    LOG.debug("flowing");
  }

  /**
   * Append only to the local disk buffer
   */
  @Override
  public void append(Event event) throws IOException {
    durableSink.append(event);
  }

}
