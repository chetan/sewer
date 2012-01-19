package net.pixelcop.sewer.sink.durable;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import net.pixelcop.sewer.PlumbingBuilder;
import net.pixelcop.sewer.PlumbingFactory;
import net.pixelcop.sewer.Sink;
import net.pixelcop.sewer.node.ExitCodes;
import net.pixelcop.sewer.node.Node;
import net.pixelcop.sewer.node.NodeConfig;
import net.pixelcop.sewer.sink.SequenceFileSink;
import net.pixelcop.sewer.source.TransactionSource;
import net.pixelcop.sewer.util.BackoffHelper;

import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.type.TypeReference;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TransactionManager extends Thread {

  private static final Logger LOG = LoggerFactory.getLogger(TransactionManager.class);

  private static final long NANO_WAIT = TimeUnit.SECONDS.toNanos(3);

  private static final String DEFAULT_WAL_PATH = "/opt/sewer/wal";

  public static final int STOPPED = 0;
  public static final int IDLE = 1;
  public static final int DRAINING = 2;

  /**
   * Singleton instance
   */
  protected static TransactionManager instance;

  protected final Map<String, Transaction> transactions = new HashMap<String, Transaction>();

  protected final LinkedBlockingQueue<Transaction> lostTransactions = new LinkedBlockingQueue<Transaction>();

  // TODO fix this ickiness
  private static final String txFileExt = new SequenceFileSink(new String[]{""}).getFileExt();

  protected String walPath;

  protected PlumbingFactory<Sink> unreliableSinkFactory;
  protected Transaction drainingTx;

  protected AtomicInteger status;
  protected AtomicBoolean shutdown;

  protected TransactionManager(String walPath) {
    if (LOG.isDebugEnabled()) {
      LOG.debug("Initializing TransactionManager " + this.getId());
    }
    this.status = new AtomicInteger();
    this.shutdown = new AtomicBoolean(false);
    this.walPath = walPath;
    this.unreliableSinkFactory = createUnreliableSinkFactory();
    this.loadTransctionsFromDisk();
    this.setName("TxMan " + this.getId());
    this.start();
  }

  public static void init(NodeConfig conf) {
    instance = new TransactionManager(conf.get(NodeConfig.WAL_PATH, DEFAULT_WAL_PATH));
  }

  public static TransactionManager getInstance() {
    return instance;
  }

  public void shutdown() {
    this.shutdown.set(true);
    this.interrupt();
  }

  public boolean isShutdown() {
    return this.shutdown.get();
  }

  public String getWALPath() {
    return walPath;
  }

  /**
   * Begin a new transaction
   *
   * @param bucket subsink bucket
   * @return Transaction
   */
  public Transaction startTx(String bucket) {

    Transaction tx = new Transaction(
        Node.getInstance().getSource().getEventClass(), bucket, this.txFileExt);

    LOG.debug("startTx: " + tx);

    transactions.put(tx.getId(), tx);
    saveOpenTransactionsToDisk();

    return tx;
  }

  /**
   * Marks the given transaction as completed and cleans up any related files
   *
   * @param id Transaction ID
   */
  public void commitTx(String id) {

    LOG.debug("commitTx: " + id);

    if (!transactions.containsKey(id)) {
      return;
    }

    Transaction tx = transactions.remove(id);
    tx.deleteTxFiles();

    saveOpenTransactionsToDisk();
  }

  /**
   * Marks the given transaction as no longer being delivered, usually because the Sink
   * holding it was closed down mid-transaction. Released transactions will be retried
   * in a separate thread launched by this manager.
   *
   * @param id Transaction ID
   */
  public void rollbackTx(String id) {

    if (!transactions.containsKey(id)) {
      return;
    }

    try {
      LOG.debug("rollbackTx: " + id);
      lostTransactions.put(transactions.remove(id));

    } catch (InterruptedException e) {
      LOG.warn("Failed to release transaction into queue", e);
    }

    saveOpenTransactionsToDisk();
  }

  /**
   * Monitor lost transactions and retry, one at a time
   */
  @Override
  public void run() {

    setStatus(IDLE);

    while (!isShutdown()) {

      // look for tx to drain
      drainingTx = null;
      try {
        drainingTx = lostTransactions.poll(NANO_WAIT, TimeUnit.NANOSECONDS);

      } catch (InterruptedException e) {
        // Interrupted, must be shutting down TxMan
        LOG.debug("Interrupted while waiting for next tx to drain");
        if (drainingTx != null) {
          lostTransactions.add(drainingTx);
          drainingTx = null;
        }
        saveOpenTransactionsToDisk();
        return;
      }

      if (drainingTx == null) {
        continue;
      }

      setStatus(DRAINING);

      // drain it
      if (!drainTx()) {
        // drain failed (interrupted, tx man shutting down), stick tx at end of queue (front ??)
        lostTransactions.add(drainingTx);
        drainingTx = null;
        saveOpenTransactionsToDisk();
        return;
      }

      drainingTx.deleteTxFiles();
      drainingTx = null;
      saveOpenTransactionsToDisk();
      setStatus(IDLE);
    }

  }

  /**
   * Save all transactions we know about to disk
   */
  protected void saveOpenTransactionsToDisk() {

    synchronized (DEFAULT_WAL_PATH) {

      File txLog = getTxLog();

      if (LOG.isDebugEnabled()) {
        LOG.debug("Saving transaction queues to disk " + txLog.toString());
      }

      List<Transaction> txList = new ArrayList<Transaction>();

      if (drainingTx != null) {
        if (LOG.isTraceEnabled()) {
          LOG.trace("Found tx currently being drained");
        }
        txList.add(drainingTx);
      }

      if (!transactions.isEmpty()) {
        if (LOG.isTraceEnabled()) {
          LOG.trace("Found " + transactions.size() + " presently open transactions");
        }
        txList.addAll(transactions.values());
      }

      if (!lostTransactions.isEmpty()) {
        if (LOG.isTraceEnabled()) {
          LOG.trace("Found " + lostTransactions.size() + " lost transactions");
        }
        txList.addAll(lostTransactions);
      }

      try {
        new ObjectMapper().writeValue(txLog, txList);
        LOG.trace("save complete");

      } catch (IOException e) {
        LOG.error("Failed to write txn.log: " + e.getMessage(), e);

      }

    }

  }

  /**
   * Load transaction log from disk so we can restart them
   */
  protected void loadTransctionsFromDisk() {

    File txLog = getTxLog();

    if (LOG.isDebugEnabled()) {
      LOG.debug("Loading transaction queues from disk " + txLog.toString());
    }

    try {
      ArrayList<Transaction> recovered = new ObjectMapper().readValue(txLog,
          new TypeReference<ArrayList<Transaction>>() {});

      LOG.info("Recovered " + recovered.size() + " txns from disk");
      lostTransactions.addAll(recovered);

    } catch (FileNotFoundException e) {
      LOG.debug(e.getMessage());
      return; // no biggie, just doesn't exist yet

    } catch (Exception e) {
      LOG.error("Failed to load txn.log: " + e.getMessage());
      System.exit(ExitCodes.STARTUP_ERROR);
    }

  }

  protected File getTxLog() {
    return new File(getWALPath() + "/txn.log");
  }

  /**
   * Drains the currently selected Transaction. Returns on completion or if interrupted
   */
  private boolean drainTx() {

    // drain this tx to sink
    LOG.debug("Draining tx " + drainingTx);
    BackoffHelper backoff = new BackoffHelper();
    while (!isShutdown()) {

      TransactionSource txSource = new TransactionSource(drainingTx);
      txSource.setSinkFactory(this.unreliableSinkFactory);

      try {
        // returns only when it has been drained completely or throws an error due to
        // drain failure
        txSource.open();
        commitTx(drainingTx.getId());
        LOG.debug("Successfully drained tx " + drainingTx);
        return true; // if we get here, drain was successful

      } catch (Throwable t) {
        try {
          backoff.handleFailure(t, LOG, "Error draining tx", isShutdown());
        } catch (InterruptedException e) {
          LOG.debug("Interrupted while draining, must be shutting down?");
          return false;
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

    return false;
  }

  /**
   * Returns the configured sink without any extra reliability mechanisms
   *
   * @return Sink
   */
  @SuppressWarnings({ "rawtypes", "unchecked" })
  private PlumbingFactory<Sink> createUnreliableSinkFactory() {

    List classes = Node.getInstance().getSinkFactory().getClasses();
    List rawSinkClasses = new ArrayList();

    for (Iterator iter = classes.iterator(); iter.hasNext();) {
      PlumbingBuilder builder = (PlumbingBuilder) iter.next();
      if (builder.getClazz() == ReliableSink.class || builder.getClazz() == RollSink.class) {
        // skip
        continue;
      }
      if (builder.getClazz() == ReliableSequenceFileSink.class) {
        // replace with SequenceFileSink
        rawSinkClasses.add(new PlumbingBuilder<Sink>(SequenceFileSink.class, builder.getArgs()));
        continue;
      }
      rawSinkClasses.add(builder);
    }

    return new PlumbingFactory<Sink>(rawSinkClasses);
  }

  public int getStatus() {
    return status.get();
  }

  private void setStatus(int status) {
    this.status.set(status);
  }

}
