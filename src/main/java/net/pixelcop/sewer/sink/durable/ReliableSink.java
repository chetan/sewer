package net.pixelcop.sewer.sink.durable;

import java.io.IOException;

import net.pixelcop.sewer.Event;
import net.pixelcop.sewer.Sink;
import net.pixelcop.sewer.sink.BucketedSink;
import net.pixelcop.sewer.sink.SequenceFileSink;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Creates a new {@link Transaction} on open(). All appended Events until close() are then made durable
 * by persisting to disk until a proper close(). If close() fails, the batch is retried in another
 * thread (via {@link TransactionManager}.
 *
 * @author chetan
 *
 */
public class ReliableSink extends Sink {

  private static final Logger LOG = LoggerFactory.getLogger(ReliableSink.class);

  private Transaction tx;
  private SequenceFileSink durableSink;

  private boolean subSinkError;

  public ReliableSink(String[] args) {
  }

  @Override
  public void close() throws IOException {
    LOG.debug("closing");
    setStatus(CLOSING);

    boolean error = false;

    try {
      durableSink.close();
    } catch (IOException e) {
      LOG.warn("Failed to close durable sink", e);
      error = true;
    }

    if (subSink == null || subSinkError
        || subSink.getStatus() == OPENING || subSink.getStatus() == ERROR) {

      // never opened or some other error, rollback!
      error = true;
    }

    // try to close subsink. it succeeds w/o error, then the tx is completed.
    try {
      subSink.close();

    } catch (IOException e) {
      LOG.error("subsink failed to close for txid " + tx.getId());
      error = true;
    }

    if (error) {
      tx.rollback();
    } else {
      tx.commit();
    }

    setStatus(CLOSED);
    LOG.debug("closed");
  }

  @Override
  public void open() throws IOException {
    LOG.debug("opening");
    setStatus(OPENING);

    subSinkError = false;
    createSubSink();
    try {
      subSink.open();
    } catch (Throwable t) {
      LOG.warn("Error opening subsink, continuing with local buffer", t);
      subSinkError = true;
    }

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
   * This appends the event to two separate sinks: the subsink & and our local disk buffer.
   * Writing to the disk buffer happens asynchronously in the background to allow execution
   * to continue on this thread.
   */
  @Override
  public void append(Event event) throws IOException {

    durableSink.append(event);

    if (subSinkError) {
      return;
    }
    try {
      subSink.append(event);
    } catch (Throwable t) {
      LOG.warn("Caught error while appending to subsink", t);
      subSinkError = true;
    }

  }

}
