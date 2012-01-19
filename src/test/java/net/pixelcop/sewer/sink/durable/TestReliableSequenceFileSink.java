package net.pixelcop.sewer.sink.durable;
import java.io.IOException;

import net.pixelcop.sewer.node.AbstractHadoopTest;
import net.pixelcop.sewer.node.TestableNode;
import net.pixelcop.sewer.source.debug.StringEvent;

import org.apache.hadoop.io.NullWritable;
import org.junit.Test;


public class TestReliableSequenceFileSink extends AbstractHadoopTest {

  private static final NullWritable NULL = NullWritable.get();

  /**
   * HDFS up and running, "all green" case test
   *
   * @throws IOException
   * @throws InterruptedException
   */
  @Test
  public void testSinkDrainsNormally() throws IOException, InterruptedException {

    setupHdfs();

    // now lets try to restart the txman and drain this thing
    // create a new node & txman using the old tmp path
    TestableNode node = createNode("gen(1000)", "reliableseq('" + getConnectionString() + "/test/data')");
    Thread.sleep(100);
    TestableTransactionManager.assertNoTransactions();


    node.start();
    node.await();
    node.cleanup();


    // wait for drain, at most 2 sec
    long stop = System.currentTimeMillis() + 2000;
    while (TestableTransactionManager.hasTransactions() && System.currentTimeMillis() < stop) {
    }
    TestableTransactionManager.kill();

    TestableTransactionManager.assertNoTransactions();
    node.getTxTestHelper().verifyRecordsInBuffers(0, 0, new StringEvent());
  }

  /**
   * Test a simulated failure (HDFS not yet started)
   *
   * @throws IOException
   * @throws InterruptedException
   */
  @Test
  public void testFailureBuffersToDisk() throws IOException, InterruptedException {

    TestableNode node = createNode("gen(1000)", "reliableseq('" + getConnectionString() + "/test/data')");

    // start node, opens source, blocks until all events sent
    node.start();
    node.await();
    node.cleanup();

    TestableTransactionManager.kill();

    // now check expected results (buffers on disk, no appends on ultimate subsink)
    assertEquals(0, TestableTransactionManager.getTransactions().size());
    assertEquals(1, TestableTransactionManager.getLostTransactions().size());

    // check count of events written to disk
    node.getTxTestHelper().verifyRecordsInBuffers(1, 1000, new StringEvent());
    node.getTxTestHelper().assertTxLogExists();
  }

  /**
   * Test a simulated failure and recovery
   *
   * @throws IOException
   * @throws InterruptedException
   */
  @Test
  public void testTxManagerDrainsFailedBatch() throws IOException, InterruptedException {

    TestableNode node = createNode("gen(1000)", "reliableseq('" + getConnectionString() + "/test/data')");

    // start node, opens source, blocks until all events sent
    node.start();
    node.await();
    node.cleanup();

    // check count of events written to disk
    node.getTxTestHelper().verifyRecordsInBuffers(1, 1000, new StringEvent());

    TestableTransactionManager.kill();
    node.getTxTestHelper().assertTxLogExists();

    // now we should still have 1 lost tx, but 0 in progress
    assertEquals(0, TestableTransactionManager.getTransactions().size());
    assertEquals(1, TestableTransactionManager.getLostTransactions().size());


    setupHdfs();

    // now lets try to restart the txman and drain this thing
    // create a new node & txman using the old tmp path
    node = createNode("gen(0)", "reliableseq('" + getConnectionString() + "/test/data')", node.getTxTestHelper().getTmpWalPath());
    Thread.sleep(100);

    // makes sure we reloaded from disk on reset()
    assertEquals(0, TestableTransactionManager.getTransactions().size());
    assertTrue("txns loaded from disk", TestableTransactionManager.getLostTransactions().size() >= 1
        || TestableTransactionManager.getDrainingTx() != null);

    TestableTransactionManager.await();
    TestableTransactionManager.kill();

    TestableTransactionManager.assertNoTransactions();
    node.getTxTestHelper().verifyRecordsInBuffers(0, 0, new StringEvent());

  }

}
