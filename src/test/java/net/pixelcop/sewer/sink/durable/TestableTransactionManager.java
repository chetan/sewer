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

  protected TestableTransactionManager(String walPath) {
    super(walPath);
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
  public static void kill() throws InterruptedException {
    LOG.debug("Shutting down TransactionManager " + getInstance().getId());
    getInstance().shutdown();
    getInstance().join();
    LOG.debug("TransactionManager joined " + getInstance().getId());
  }

}
