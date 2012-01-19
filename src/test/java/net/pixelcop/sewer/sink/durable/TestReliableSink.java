package net.pixelcop.sewer.sink.durable;

import java.io.IOException;

import net.pixelcop.sewer.node.AbstractNodeTest;
import net.pixelcop.sewer.node.TestableNode;
import net.pixelcop.sewer.sink.debug.CountingSink;
import net.pixelcop.sewer.source.debug.StringEvent;

import org.junit.Test;

public class TestReliableSink extends AbstractNodeTest {

  @Test
  public void testFailureBuffersToDisk() throws IOException, InterruptedException {

    TestableNode node = createNode("gen(1000)", "reliable > failopen > counting");

    // start node, opens source, blocks until all events sent
    node.start();
    node.await();
    node.cleanup();

    TestableTransactionManager.kill();

    // now check expected results (buffers on disk, no appends on ultimate subsink)
    assertEquals(0, CountingSink.getAppendCount());
    assertEquals(0, TestableTransactionManager.getTransactions().size());
    assertEquals(1, TestableTransactionManager.getLostTransactions().size());

    // check count of events written to disk
    node.getTxTestHelper().verifyRecordsInBuffers(1, 1000, new StringEvent());
    node.getTxTestHelper().assertTxLogExists();
  }

  @Test
  public void testTxManagerDrainsFailedBatch() throws IOException, InterruptedException {

    TestableNode node = createNode("gen(1000)", "reliable > failopen > counting");

    // start node, opens source, blocks until all events sent
    node.start();
    node.await();
    node.cleanup();

    // now check expected results (buffers on disk, no appends on ultimate subsink)
    assertEquals(0, CountingSink.getAppendCount());


    // check count of events written to disk
    node.getTxTestHelper().verifyRecordsInBuffers(1, 1000, new StringEvent());


    TestableTransactionManager.kill();
    node.getTxTestHelper().assertTxLogExists();

    // now we should still have 1 lost tx, but 0 in progress
    assertEquals(0, TestableTransactionManager.getTransactions().size());
    assertEquals(1, TestableTransactionManager.getLostTransactions().size());


    // now lets try to restart the txman and drain this thing
    // create a new node & txman using the old tmp path
    node = createNode("null", "reliable > counting", node.getTxTestHelper().getTmpWalPath());

    // makes sure we reloaded from disk on reset()
    assertEquals(0, TestableTransactionManager.getTransactions().size());
    assertTrue("txns loaded from disk", TestableTransactionManager.hasTransactions());

    TestableTransactionManager.await();
    TestableTransactionManager.kill();

    TestableTransactionManager.assertNoTransactions();
    assertEquals(1000, CountingSink.getAppendCount());
  }

}
