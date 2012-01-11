package net.pixelcop.sewer.sink.durable;

import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Expose some members to aid testing
 *
 * @author chetan
 *
 */
public class TestableTransactionManager extends TransactionManager {

  private static final Logger LOG = LoggerFactory.getLogger(TestableTransactionManager.class);

  protected TestableTransactionManager() {
    super();
  }


  public static Map<String, Transaction> getTransactions() {
    return getInstance().transactions;
  }

  public static LinkedBlockingQueue<Transaction> getLostTransactions() {
    return getInstance().lostTransactions;
  }

  public static Transaction getDrainingTx() {
    return getInstance().drainingTx;
  }

  /**
   * Kill the existing {@link TransactionManager}
   * @throws InterruptedException
   */
  public static void shutdown() throws InterruptedException {
    LOG.debug("Shutting down TransactionManager");
    instance.interrupt();
    instance.join();
    LOG.debug("TransactionManager joined");
  }

  /**
   * Replace the existing {@link TransactionManager} with a new instance.
   * The old one should first be {@link #shutdown}!
   */
  public static void reset() {
    LOG.debug("Resetting TransactionManager");
    instance = new TransactionManager();
  }

}
