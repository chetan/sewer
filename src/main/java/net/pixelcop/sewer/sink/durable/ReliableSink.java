package net.pixelcop.sewer.sink.durable;

import java.io.IOException;

import net.pixelcop.sewer.Event;
import net.pixelcop.sewer.Sink;
import net.pixelcop.sewer.sink.BucketedSink;
import net.pixelcop.sewer.sink.SequenceFileSink;

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
public class ReliableSink extends Sink implements SubSinkOpenerEvents {

  private static final Logger LOG = LoggerFactory.getLogger(ReliableSink.class);

  private String durableDirPath;
  private String durablePath;

  private String txId;
  private SequenceFileSink durableSink;

  private SubSinkOpenerThread opener;
  private AsyncBufferSink persister;
  private AsyncBufferSink delayedSink;

  public ReliableSink(String[] args) {
    this.durableDirPath = TransactionManager.getInstance().getWALPath();
  }

  @Override
  public void close() throws IOException {

    setStatus(CLOSED); // signal our threads to wrap up

    if (subSink == null) {
      // never opened??
      return;
    }

    // cleanup threads first, then close subsink and commit tx
    persister.close();
    delayedSink.close();
    if (opener != null) {
      opener.cancel();
    }

    try {
      durableSink.close();
    } catch (IOException e) {
      LOG.warn("Failed to close durable sink: " + e.getMessage(), e);
      // we can continue safely assuming that the subsink closes cleanly
      // in which case, the tx will be committed anyway
    }

    if (subSink.getStatus() == OPENING) {
      // never opened, rollback!
      TransactionManager.getInstance().releaseTx(txId);
      return;
    }

    // try to close subsink. it succeeds w/o error, then the tx is completed.
    try {
      subSink.close();

    } catch (IOException e) {
      // release tx
      LOG.error("subsink failed to close for txid " + txId);
      TransactionManager.getInstance().releaseTx(txId);
      return;

    }

    // closed cleanly, commit tx
    TransactionManager.getInstance().commitTx(txId);
  }

  @Override
  public void open() throws IOException {

    setStatus(OPENING);

    createSubSink();

    String nextBucket = null;
    if (subSink instanceof BucketedSink) {
      nextBucket = ((BucketedSink) subSink).generateNextBucket();
    }
    this.txId = TransactionManager.getInstance().startTx(nextBucket);
    this.durablePath = "file://" + durableDirPath + "/" + txId;
    this.durableSink = new SequenceFileSink(new String[] { durablePath });

    try {
      this.durableSink.open();
    } catch (IOException e) {
      LOG.error("Error opening durable sink at path " + durablePath, e);
      throw e;
    }

    setStatus(FLOWING);

    opener = new SubSinkOpenerThread(Thread.currentThread().getId(), subSink, this);
    opener.start();

    persister = new AsyncBufferSink("persister");
    persister.setSubSink(durableSink);
    persister.open();

    delayedSink = new AsyncBufferSink("delayed appender");
    delayedSink.setSubSink(this.subSink);
    delayedSink.open();

  }

  /**
   * This appends the event to two separate sinks: the subsink & and our local disk buffer.
   * Writing to the disk buffer happens asynchronously in the background to allow execution
   * to continue on this thread.
   */
  @Override
  public void append(Event event) throws IOException {

    if (subSink.getStatus() == FLOWING) {
      try {
        subSink.append(event);
      } catch (IOException e) {
        // This is 'OK' in that we still persist the message to our durable sink
        // for later transfer
        LOG.warn("Caught error while appending to subsink: " + e.getMessage(), e);
      }

    } else {
      // write message to delayed queue
      delayedSink.append(event);
    }

    persister.append(event);
  }

  @Override
  public void onSubSinkOpen() {
    opener = null;
  }

}
