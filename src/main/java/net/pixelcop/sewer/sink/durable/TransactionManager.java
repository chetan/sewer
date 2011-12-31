package net.pixelcop.sewer.sink.durable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import net.pixelcop.sewer.Sink;
import net.pixelcop.sewer.SourceSinkFactory;
import net.pixelcop.sewer.SourceSinkFactory.SourceSinkBuilder;
import net.pixelcop.sewer.node.Node;
import net.pixelcop.sewer.sink.SequenceFileSink;
import net.pixelcop.sewer.source.TransactionSource;
import net.pixelcop.sewer.util.BackoffHelper;
import net.pixelcop.sewer.util.HdfsUtil;

import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TransactionManager extends Thread {

  private static final Logger LOG = LoggerFactory.getLogger(TransactionManager.class);

  private static final long NANO_WAIT = TimeUnit.SECONDS.toNanos(3);

  private static final TransactionManager instance = new TransactionManager();
  static {
    // TODO load old transactions from storage
  }

  private final Map<String, Transaction> transactions = new HashMap<String, Transaction>();

  private final LinkedBlockingQueue<Transaction> lostTransactions = new LinkedBlockingQueue<Transaction>();

  private final String txFileExt = new SequenceFileSink(new String[]{""}).getFileExt();

  private SourceSinkFactory<Sink> unreliableSinkFactory;
  private Transaction drainingTx;

  private TransactionManager() {
    this.unreliableSinkFactory = createUnreliableSinkFactory();
    this.setName("TxMan");
    this.start();
  }

  public static TransactionManager getInstance() {
    return instance;
  }

  public String getWALPath() {
    // TODO retrieve from configuration
    return "/opt/sewer/wal";
  }

  /**
   * Begin a new transaction
   *
   * @param bucket subsink bucket
   * @return Transaction ID
   */
  public String startTx(String bucket) {

    Transaction tx = new Transaction(bucket);
    transactions.put(tx.getId(), tx);

    return tx.getId();
  }

  /**
   * Marks the given transaction as completed and cleans up any related files
   *
   * @param id Transaction ID
   */
  public void commitTx(String id) {

    if (!exists(id)) {
      return;
    }

    transactions.remove(id);
    deleteTxFiles(id);

  }

  /**
   * Delete the files belonging to the given transaction id
   * @param id
   */
  private void deleteTxFiles(String id) {

    if (LOG.isDebugEnabled()) {
      LOG.debug("Deleting files belonging to tx " + id);
    }

    Path path = createTxPath(id);
    try {
      HdfsUtil.deletePath(path);

    } catch (IOException e) {
      LOG.warn("Error deleting tx file: " + e.getMessage() + "\n    path:" + path.toString(), e);
    }

  }

  private Path createTxPath(String id) {
    return new Path("file://" + getWALPath() + "/" + id + txFileExt);
  }

  /**
   * Marks the given transaction as no longer being delivered, usually because the Sink
   * holding it was closed down mid-transaction. Released transactions will be retried
   * in a separate thread launched by this manager.
   *
   * @param id Transaction ID
   */
  public void releaseTx(String id) {

    if (!exists(id)) {
      return;
    }

    try {
      LOG.debug("tx released: " + id);
      lostTransactions.put(transactions.get(id));

    } catch (InterruptedException e) {
      LOG.warn("Failed to release transaction into queue", e);
    }

    // TODO start thread ??

  }

  private boolean exists(String id) {
    return transactions.containsKey(id);
  }

  /**
   * Monitor lost transactions and retry, one at a time
   */
  @Override
  public void run() {

    while (true) {

      drainingTx = null;
      try {
        drainingTx = lostTransactions.poll(NANO_WAIT, TimeUnit.NANOSECONDS);
      } catch (InterruptedException e) {
        // means we're shutting down
        // TODO save lostTransactions queue and exit
      }

      if (drainingTx == null) {
        continue;
      }

      drainTx();
    }

  }

  /**
   * Drains the currently selected Transaction. Returns on completion or if interrupted
   */
  private void drainTx() {

    // drain this tx to sink
    LOG.debug("Draining tx " + drainingTx.getId());
    BackoffHelper backoff = new BackoffHelper();
    while (true) {
      TransactionSource txSource = new TransactionSource(
          createTxPath(drainingTx.getId()), drainingTx.getBucket(), txFileExt);

      txSource.setSinkFactory(this.unreliableSinkFactory);

      try {
        // returns only when it has been drained completely or throws an error due to
        // drain failure
        txSource.open();
        commitTx(drainingTx.getId());
        break;

      } catch (IOException e) {
        try {
          backoff.handleFailure(e, LOG, "Error draining tx", false); // TODO cancel flag?
        } catch (InterruptedException e1) {
          LOG.debug("Interrupted while draining, must be shutting down?");
        }


      } finally {
        try {
          LOG.debug("closing txSource");
          txSource.close();
        } catch (IOException e) {
          LOG.debug("exception closing txSource after error: " + e.getMessage());
        }
      }
    }

  }

  /**
   * Returns the configured sink without any extra reliability mechanisms
   *
   * @return Sink
   */
  @SuppressWarnings({ "rawtypes", "unchecked" })
  private SourceSinkFactory<Sink> createUnreliableSinkFactory() {

    List classes = Node.getInstance().getSinkFactory().getClasses();
    List rawSinkClasses = new ArrayList();

    for (Iterator iter = classes.iterator(); iter.hasNext();) {
      SourceSinkBuilder builder = (SourceSinkBuilder) iter.next();
      if (builder.getClazz() == ReliableSink.class || builder.getClazz() == RollSink.class) {
        continue;
      }
      rawSinkClasses.add(builder);
    }

    return new SourceSinkFactory<Sink>(rawSinkClasses);
  }

}
