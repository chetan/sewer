package net.pixelcop.sewer.sink.durable;

import java.io.IOException;

import net.pixelcop.sewer.node.Node;
import net.pixelcop.sewer.sink.SequenceFileSink;
import net.pixelcop.sewer.util.HdfsUtil;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.io.SequenceFile.Writer;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ReliableSequenceFileSink extends SequenceFileSink {

  private static final Logger LOG = LoggerFactory.getLogger(ReliableSequenceFileSink.class);

  protected Transaction tx;

  public ReliableSequenceFileSink(String[] args) {
    super(args);
  }

  @Override
  protected void createWriter() throws IOException {

    tx = TransactionManager.getInstance().startTx(nextBucket);

    Configuration conf = Node.getInstance().getConf();

    CompressionCodec codec = HdfsUtil.createCodec();
    dstPath = new Path(nextBucket + ".seq" + codec.getDefaultExtension());

    DualFSDataOutputStream dualOut = new DualFSDataOutputStream(tx.createTxPath(), dstPath, conf);

    Writer w = SequenceFile.createWriter(conf, dualOut, NullWritable.class,
        Node.getInstance().getSource().getEventClass(), CompressionType.BLOCK, codec);

    if (LOG.isInfoEnabled()) {
      LOG.info("Opened ReliableSequenceFileSink: " + dstPath.toString());
    }

    nextBucket = null;
  }

}
