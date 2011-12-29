package net.pixelcop.sewer.sink.durable;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;

import net.pixelcop.sewer.sink.SequenceFileSink;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TransactionManager {

  private static final Logger LOG = LoggerFactory.getLogger(TransactionManager.class);

  private static final TransactionManager instance = new TransactionManager();
  static {
    // TODO load old transactions from storage
  }

  private final Map<String, Transaction> transactions = new HashMap<String, Transaction>();

  private final LinkedBlockingQueue<Transaction> lostTransactions = new LinkedBlockingQueue<Transaction>();

  private final String txFileExt = new SequenceFileSink(new String[]{""}).getFileExt();

  private TransactionManager() {
  }

  public static TransactionManager getInstance() {
    return instance;
  }

  public String getWALPath() {
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

  private void deleteTxFiles(String id) {

    LOG.debug("Deleting files belonging to tx " + id);

    Configuration conf = new Configuration();
    Path path = new Path(getWALPath() + "/" + id + txFileExt);
    LOG.debug("path: " + path.toString());
    try {
      FileSystem fs = path.getFileSystem(conf);
      fs.delete(path, false);

    } catch (IOException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }

  }

  /**
   * Marks the given transaction as no longer being delivered, usually because the Sink
   * holding it was closed down mid-transaction. Released transactions will be retried
   * in a separate thread launched by this manager.
   *
   * @param id Transaction ID
   */
  public void release(String id) {

    if (!exists(id)) {
      return;
    }

    try {
      lostTransactions.put(transactions.get(id));

    } catch (InterruptedException e) {
      LOG.warn("Failed to release transaction into queue", e);
    }

    // TODO start thread ??

  }

  private boolean exists(String id) {
    return transactions.containsKey(id);
  }

}
