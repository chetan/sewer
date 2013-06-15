package net.pixelcop.sewer.sink.durable;

import java.io.IOException;

import net.pixelcop.sewer.node.AbstractHadoopTest;
import net.pixelcop.sewer.node.TestableNode;
import net.pixelcop.sewer.source.debug.StringEvent;

import org.junit.Test;

public class TestDeferSink extends AbstractHadoopTest {

  /**
   * Test normal operation with background delivery
   *
   * @throws IOException
   * @throws InterruptedException
   */
  @Test
  public void testTxManagerDrainsFailedBatch() throws IOException, InterruptedException {

    setupHdfs();

    TestableNode node = createNode("gen(1000)", "defer > seqfile('" + getConnectionString() + "/test/data')");
    TestableTransactionManager.kill(); // kill before starting node so we don't drain immediately

    // start node, opens source, blocks until all events sent
    node.start();
    node.await();
    node.cleanup();

    // check count of events written to disk
    node.getTxTestHelper().verifyRecordsInBuffers(1, 1000, new StringEvent());
    node.getTxTestHelper().assertTxLogExists();

    // now we should still have 1 lost tx, but 0 in progress
    assertEquals(0, TestableTransactionManager.getTransactions().size());
    assertEquals(1, TestableTransactionManager.getLostTransactions().size());


    // now lets try to restart the txman and drain this thing
    TestableTransactionManager.init(node.getConf());
    TestableTransactionManager.await();
    TestableTransactionManager.kill();

    TestableTransactionManager.assertNoTransactions();
    node.getTxTestHelper().verifyRecordsInBuffers(0, 0, new StringEvent());

    assertEquals(1000, countEventsInSequenceFile(getConnectionString() + "/test/data.seq"));
  }

}
