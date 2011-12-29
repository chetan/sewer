package net.pixelcop.sewer.sink.durable;

import java.io.IOException;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import net.pixelcop.sewer.Event;
import net.pixelcop.sewer.Sink;
import net.pixelcop.sewer.sink.BucketedSink;
import net.pixelcop.sewer.sink.SequenceFileSink;
import net.pixelcop.sewer.util.BackoffHelper;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TransactionSink extends Sink {

  class OpenerThread extends Thread {

    public OpenerThread() {
      setName("Subsink Opener " + getId());
    }

    @Override
    public void run() {

      BackoffHelper backoff = new BackoffHelper();
      String subSinkName = getSubSink().getClass().getSimpleName();
      if (LOG.isDebugEnabled()) {
        LOG.debug("Opening SubSink: " + subSinkName);
      }

      while (getSubSink().getStatus() != FLOWING) {

        try {
          getSubSink().open();
          backoff.resolve(LOG, "SubSink opened");
          onSubSinkOpen(); // notify parent
          return;

        } catch (Exception e) {
          backoff.handleFailure(e, LOG, "Error opening subsink (" + subSinkName + ")");

        }

      }

    }

  }

  class PersistentThread extends Thread {

    public PersistentThread() {
      setName("Persister " + getId());
    }

    @Override
    public void run() {
      while (status == FLOWING || !eventQueue.isEmpty()) {
        // run until the sink closes and no events are left

        try {
          Event e = eventQueue.take();
          durableSink.append(e);

        } catch (InterruptedException e) {
          LOG.warn("PersistentThread was interrupted with " + eventQueue.size()
              + " events left in the queue");

        } catch (IOException e) {
          LOG.error("Caught an error while appending to the durable sink at " + durablePath + ": "
              + e.getMessage(), e);

          // TODO shutdown the reliable sink and maybe the source too

        }

      }
      try {
        durableSink.close();
      } catch (IOException e) {
        LOG.error("Error closing durable sink", e);
      }
      LOG.debug("persister run finished");
    }
  }

  class DelayedAppenderThread extends Thread {

    private final long NANO_WAIT = TimeUnit.SECONDS.toNanos(3);

    private String myTxId;
    private Sink mySink;

    public DelayedAppenderThread(String txId, Sink sink) {
      myTxId = txId;
      mySink = sink;
      setName("Delayed Appender " + getId());
    }

    @Override
    public void run() {
      while (status == FLOWING || !delayedEventQueue.isEmpty()) {

        if (subSink.getStatus() != FLOWING) {
          // TODO sleep a sec here? w/ backoff policy? await()?
          continue;
        }

        try {
          Event e = delayedEventQueue.poll(NANO_WAIT, TimeUnit.NANOSECONDS);
          if (e != null) {
            mySink.append(e);
          }

        } catch (InterruptedException e) {
          LOG.warn("DelayedAppenderThread was interrupted with " + delayedEventQueue.size()
              + " events left in the queue");

        } catch (IOException e) {
          LOG.error("Caught an error while appending to the subsink at: " + e.getMessage(), e);

          // TODO shutdown the reliable sink and maybe the source too?

        }

      }
      LOG.debug("delayed appender run finished");
    }
  }

  private static final Logger LOG = LoggerFactory.getLogger(TransactionSink.class);

  private String durableDirPath;
  private String durablePath;

  private String txId;
  private SequenceFileSink durableSink;

  private OpenerThread opener;
  private PersistentThread persister;
  private DelayedAppenderThread delayedAppender;

  private LinkedBlockingQueue<Event> eventQueue = new LinkedBlockingQueue<Event>(100000);
  private LinkedBlockingQueue<Event> delayedEventQueue = new LinkedBlockingQueue<Event>(100000);

  public TransactionSink() {
    this.durableDirPath = TransactionManager.getInstance().getWALPath();
  }

  @Override
  public void close() throws IOException {

    status = CLOSED; // signal our threads to wrap up

    if (subSink == null) {
      // never opened??
      return;
    }

    // try to close subsink. it succeeds w/o error, then the tx is completed.
    // TODO check this over
    try {
      subSink.close();

    } catch (IOException e) {
      LOG.error("subsink failed to close for txid " + txId);
      cleanupThreads();
      TransactionManager.getInstance().release(txId);
      return;

    }

    cleanupThreads();
    TransactionManager.getInstance().commitTx(txId);

  }

  private void cleanupThreads() {

    if (persister.isAlive()) {
      persister.interrupt();
    }

    if (delayedAppender.isAlive()) {
      delayedAppender.interrupt();
    }

    try {
      persister.join();
      delayedAppender.join();

    } catch (InterruptedException e) {
      LOG.debug("Interrupted while waiting for persister & delayed appender to join", e);
    }
  }

  @Override
  public void open() throws IOException {

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

    persister = new PersistentThread();
    persister.start();

    delayedAppender = new DelayedAppenderThread(txId, subSink);
    delayedAppender.start();

    opener = new OpenerThread();
    opener.start();

  }

  @Override
  public void append(Event event) throws IOException {

    if (subSink.getStatus() == FLOWING) {
      try {
        //LOG.debug("subsink is open, appending directly to it");
        subSink.append(event);
      } catch (IOException e) {
        // This is 'OK' in that we still persist the message to our durable sink
        // for later transfer
        LOG.warn("Caught error while appending to subsink: " + e.getMessage(), e);
      }

    } else {
      // write message to delay queue
      LOG.debug("subsink not open, writing event to delayed queue");
      try {
        delayedEventQueue.put(event);
      } catch (InterruptedException e) {
        LOG.error("interrupted while putting event in delayed queue");
      }
    }

    try {
      //LOG.debug("putting event in persistent queue as well...");
      eventQueue.put(event);
    } catch (InterruptedException e) {
      LOG.error("interrupted while putting event in persist queue");
    }

  }

  // Called from various threads
  public void onSubSinkOpen() {
    opener = null;
  }

}
