package net.pixelcop.sewer.sink.durable;
import java.io.IOException;

import net.pixelcop.sewer.node.AbstractHadoopTest;
import net.pixelcop.sewer.node.TestableNode;
import net.pixelcop.sewer.source.debug.StringEvent;

import org.apache.hadoop.io.NullWritable;
import org.junit.Test;


public class TestReliableSequenceFileSink extends AbstractHadoopTest {

  private static final NullWritable NULL = NullWritable.get();

  @Test
  public void testFailureBuffersToDisk() throws IOException, InterruptedException {

    TestableNode node = createNode("gen(1000)", "reliableseq('" + getConnectionString() + "/test/data')");

    // start node, opens source, blocks until all events sent
    node.start();
    node.await();

    TestableTransactionManager.kill();

    // now check expected results (buffers on disk, no appends on ultimate subsink)
    assertEquals(0, TestableTransactionManager.getTransactions().size());
    assertEquals(1, TestableTransactionManager.getLostTransactions().size());

    // check count of events written to disk
    node.getTxTestHelper().verifyRecordsInBuffers(1, 1000, new StringEvent());
    node.getTxTestHelper().assertTxLogExists();
  }

  /*
  @Test
  public void testWriter() throws IOException {

    TestableNode node = createNode("gen", "null");

    String bucket = BucketPath.escapeString(
        "hdfs://localhost:9000/test/collect/%Y-%m-%d/%H00/data-%{host}-%Y%m%d-%k%M%S", null);

    Transaction tx = new Transaction(StringEvent.class, bucket, ".seq.deflate");


    Path hdfsPath = new Path(tx.getBucket() + ".seq.deflate");

    System.err.println("creating dualout");
    DualFSDataOutputStream dualOut = new DualFSDataOutputStream(tx.createTxPath(), hdfsPath,
        node.getConf());
    System.err.println("dualout created....");

    Writer w = SequenceFile.createWriter(node.getConf(), dualOut, NullWritable.class, StringEvent.class,
        CompressionType.BLOCK, HdfsUtil.createCodec());

    //TransactionWriter w = new TransactionWriter(tx, hdfsPath, new Configuration());
    assertNotNull(w);

    w.append(NULL, new StringEvent("foobar1"));
    w.append(NULL, new StringEvent("foobar2"));
    w.append(NULL, new StringEvent("foobar3"));
    w.close();

    dualOut.flush();
    dualOut.close();


  }
  */

}
