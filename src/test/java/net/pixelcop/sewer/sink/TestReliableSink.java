package net.pixelcop.sewer.sink;

import java.io.IOException;

import net.pixelcop.sewer.node.AbstractNodeTest;
import net.pixelcop.sewer.node.NodeConfig;
import net.pixelcop.sewer.node.TestableNode;
import net.pixelcop.sewer.sink.debug.CountingSink;
import net.pixelcop.sewer.sink.durable.TestableTransactionManager;
import net.pixelcop.sewer.source.debug.StringEvent;

import org.junit.Test;

public class TestReliableSink extends AbstractNodeTest {

  @Test
  public void testFailureBuffersToDisk() throws IOException, InterruptedException {

    TestableNode node = createNode("gen(1000)", "reliable > failopen > counting");
    TxTestHelper txHelper = new TxTestHelper(node);

    // start node, opens source, blocks until all events sent
    node.start();
    node.await();

    TestableTransactionManager.shutdown();

    // now check expected results (buffers on disk, no appends on ultimate subsink)
    assertEquals(0, CountingSink.getAppendCount());
    assertEquals(0, TestableTransactionManager.getTransactions().size());
    assertEquals(1, TestableTransactionManager.getLostTransactions().size());

    // check count of events written to disk
    txHelper.verifyRecordsInBuffers(1, 1000, new StringEvent());
    txHelper.assertTxLogExists();
  }

  @Test
  public void testTxManagerDrainsFailedBatch() throws IOException, InterruptedException {

    TestableNode node = createNode("gen(1000)", "reliable > failopen > counting");
    TxTestHelper txHelper = new TxTestHelper(node);

    // start node, opens source, blocks until all events sent
    node.start();
    node.await();

    // now check expected results (buffers on disk, no appends on ultimate subsink)
    assertEquals(0, CountingSink.getAppendCount());


    // check count of events written to disk
    txHelper.verifyRecordsInBuffers(1, 1000, new StringEvent());


    TestableTransactionManager.shutdown();
    txHelper.assertTxLogExists();

    // now we should still have 1 lost tx, but 0 in progress
    assertEquals(0, TestableTransactionManager.getTransactions().size());
    assertEquals(1, TestableTransactionManager.getLostTransactions().size());


    // now lets try to restart the txman and drain this thing
    node = createNode("null", "reliable > counting");
    node.getConf().set(NodeConfig.WAL_PATH, txHelper.getTmpWalPath());
    TestableTransactionManager.reset();


    // makes sure we reloaded from disk on reset()
    assertEquals(0, TestableTransactionManager.getTransactions().size());
    assertTrue("txns loaded from disk", TestableTransactionManager.getLostTransactions().size() >= 1
        || TestableTransactionManager.getDrainingTx() != null);

    // wait for drain
    while (TestableTransactionManager.getDrainingTx() != null) {
    }
    TestableTransactionManager.shutdown();

    assertEquals(0, TestableTransactionManager.getTransactions().size());
    assertEquals(0, TestableTransactionManager.getLostTransactions().size());

    assertEquals(1000, CountingSink.getAppendCount());
  }

}
