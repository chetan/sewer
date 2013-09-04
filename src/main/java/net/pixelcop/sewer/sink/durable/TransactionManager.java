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

import net.pixelcop.sewer.DrainSink;
import net.pixelcop.sewer.PlumbingBuilder;
import net.pixelcop.sewer.PlumbingFactory;
import net.pixelcop.sewer.Sink;
import net.pixelcop.sewer.node.ExitCodes;
import net.pixelcop.sewer.node.Node;
import net.pixelcop.sewer.node.NodeConfig;
import net.pixelcop.sewer.sink.SequenceFileSink;
import net.pixelcop.sewer.source.TransactionSource;
import net.pixelcop.sewer.util.BackoffHelper;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

public class TransactionManager extends Thread {

  private static final Logger LOG = LoggerFactory.getLogger(TransactionManager.class);

  private static final long NANO_WAIT = TimeUnit.SECONDS.toNanos(3);
  private static final long CLEANUP_TIME = TimeUnit.SECONDS.toSeconds(60);

  private static final String DEFAULT_WAL_PATH = "/opt/sewer/wal";

  public static final int STOPPED = 0;
  public static final int IDLE = 1;
  public static final int DRAINING = 2;

  /**
   * Singleton instance
   */
  protected static TransactionManager instance;

  /**
   * A map of open transactions currently being processed by a running Sink
   */
  protected final Map<String, Transaction> transactions = new HashMap<String, Transaction>();

  /**
   * A queue containing failed transactions (rollback was called)
   */
  protected final LinkedBlockingQueue<Transaction> failedTransactions = new LinkedBlockingQueue<Transaction>();

  /**
   * Extension used by transactional data
   */
  private final String txFileExt;

  /**
   * Location where Write Ahead Logs will be stored
   */
  protected String walPath;

  /**
   * The configured Sink without any durability mechanisms (i.e., only the final destination)
   */
  protected PlumbingFactory<Sink> unreliableSinkFactory;

  /**
   * The currently draining transaction, if any.
   */
  protected Transaction drainingTx;

  /**
   * Tracks the status of the {@link TransactionManager}: STOPPED, IDLE, or DRAINING
   */
  protected AtomicInteger status;
  protected AtomicBoolean shutdown;

  /**
   * Cheap hack to move rollback logging to debug level
   */
  protected boolean silentRollback;

  protected TransactionManager(String walPath) {
    if (LOG.isDebugEnabled()) {
      LOG.debug("Initializing TransactionManager " + this.getId());
    }
    this.silentRollback = false;
    this.status = new AtomicInteger();
    this.shutdown = new AtomicBoolean(false);
    this.walPath = walPath;
    this.txFileExt = new SequenceFileSink(new String[]{""}).getFileExt(); // TODO fix this ickiness
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

  /**
   * Shutdown the Transaction Manager immediately
   */
  public void shutdown() {
    shutdown(true);
  }

  /**
   * Shutdown the Transaction Manager
   *
   * @param immediate
   *          whether to stop immediately or wait for a transaction to drain
   */
  public void shutdown(boolean immediate) {
    if (!immediate) {
      cleanup();
    }
    this.shutdown.set(true);
    this.interrupt();
  }

  /**
   * Wait for transaction queue to empty
   */
  private void cleanup() {
    LOG.debug("cleanup");
    if (failedTransactions.size() == 0 && drainingTx == null) {
      return; // nothing to do
    }

    LOG.info("Waiting up to " + CLEANUP_TIME + " seconds for " + numRemaining()
        + " remaining transactions to drain before quit");

    int timeWaited = 0;
    while (timeWaited < CLEANUP_TIME) {
      if (numRemaining() == 0) {
        LOG.info("Transaction queue emptied");
        return;
      }
      try {
        Thread.sleep(1000);
      } catch (InterruptedException e) {
        LOG.warn("Cleanup thread interrupted!");
        return;
      }
      timeWaited++;
    }

    if (numRemaining() > 0) {
      LOG.warn("Cleanup thread finished with " + numRemaining() + " transactions left");
    }
  }

  private int numRemaining() {
    int numRemaining = failedTransactions.size();
    if (drainingTx != null) {
      numRemaining++;
    }
    return numRemaining;
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
      if (silentRollback) {
        LOG.debug("rollbackTx: " + id);
      } else {
        LOG.info("rollbackTx: " + id);
      }
      failedTransactions.put(transactions.remove(id));

    } catch (InterruptedException e) {
      LOG.error("Failed to release transaction into queue", e);
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
        drainingTx = failedTransactions.poll(NANO_WAIT, TimeUnit.NANOSECONDS);

      } catch (InterruptedException e) {
        // Interrupted, must be shutting down TxMan
        LOG.debug("Interrupted while waiting for next tx to drain");
        if (drainingTx != null) {
          failedTransactions.add(drainingTx);
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
        failedTransactions.add(drainingTx);
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

      if (!failedTransactions.isEmpty()) {
        if (LOG.isTraceEnabled()) {
          LOG.trace("Found " + failedTransactions.size() + " lost transactions");
        }
        txList.addAll(failedTransactions);
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

      LOG.info("Loaded " + recovered.size() + " txns from disk");
      failedTransactions.addAll(recovered);

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
      if (builder.getClazz() == ReliableSequenceFileSink.class) {
        // replace with SequenceFileSink
        rawSinkClasses.add(new PlumbingBuilder<Sink>(SequenceFileSink.class, builder.getArgs()));

      } else if (builder.getClazz().isAnnotationPresent(DrainSink.class)) {
        rawSinkClasses.add(builder);
      }
      // skip all other types
    }

    return new PlumbingFactory<Sink>(rawSinkClasses);
  }

  public int getStatus() {
    return status.get();
  }

  private void setStatus(int status) {
    this.status.set(status);
  }

  public boolean isSilentRollback() {
    return silentRollback;
  }

  public void setSilentRollback(boolean silentRollback) {
    this.silentRollback = silentRollback;
  }

}
