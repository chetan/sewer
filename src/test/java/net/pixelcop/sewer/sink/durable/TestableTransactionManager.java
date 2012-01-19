package net.pixelcop.sewer.sink.durable;

import static org.junit.Assert.*;

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

  public static boolean hasTransactions() {
    return (getTransactions().size() > 0 || getLostTransactions().size() > 0
        || getDrainingTx() != null);
  }

  public static void assertNoTransactions() {
    assertEquals(0, getTransactions().size());
    assertEquals(0, getTransactions().size());
    assertNull(getDrainingTx());
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
