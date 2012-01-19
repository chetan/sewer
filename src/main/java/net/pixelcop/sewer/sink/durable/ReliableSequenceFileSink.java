package net.pixelcop.sewer.sink.durable;

import java.io.IOException;

import net.pixelcop.sewer.Plumbing;
import net.pixelcop.sewer.node.Node;
import net.pixelcop.sewer.sink.SequenceFileSink;
import net.pixelcop.sewer.util.HdfsUtil;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ReliableSequenceFileSink extends SequenceFileSink {

  private static final Logger LOG = LoggerFactory.getLogger(ReliableSequenceFileSink.class);

  protected Transaction tx;

  private DualFSDataOutputStream reliableOut;
  private Path localPath;

  public ReliableSequenceFileSink(String[] args) {
    super(args);
  }

  /**
   * Override createWriter from super() to create a reliable instance
   */
  @Override
  protected void createWriter() throws IOException {

    tx = TransactionManager.getInstance().startTx(nextBucket);

    Configuration conf = Node.getInstance().getConf();

    CompressionCodec codec = HdfsUtil.createCodec();

    localPath = tx.createTxPath();
    dstPath = new Path(nextBucket + ".seq" + codec.getDefaultExtension());

    reliableOut = new DualFSDataOutputStream(localPath, dstPath, conf);

    this.writer = SequenceFile.createWriter(conf, reliableOut, NullWritable.class,
        Node.getInstance().getSource().getEventClass(), CompressionType.BLOCK, codec);

    nextBucket = null;

    if (LOG.isInfoEnabled()) {
      LOG.info("Opened: " + HdfsUtil.pathToString(localPath));

      if (reliableOut.isRemoteOpen()) {
        LOG.info("Opened: " + HdfsUtil.pathToString(dstPath));
      } else {
        LOG.info("Still opening: " + HdfsUtil.pathToString(dstPath));
      }
    }
  }

  @Override
  public void close() throws IOException {

    if (LOG.isDebugEnabled()) {
      LOG.debug("close() called; currently: " + getStatusString());
      LOG.debug("Closing: " + HdfsUtil.pathToString(localPath));
      LOG.debug("Closing: " + HdfsUtil.pathToString(dstPath));
    }
    setStatus(CLOSING);

    if (writer != null) {
      writer.close();
    }

    if (reliableOut != null) {
      reliableOut.close();
    }

    if (reliableOut.getStatus() == Plumbing.ERROR) {
      tx.rollback();
    }

    nextBucket = null;
    setStatus(CLOSED);

    if (LOG.isInfoEnabled()) {
      LOG.info("Closed: " + HdfsUtil.pathToString(localPath));
      LOG.info("Closed: " + HdfsUtil.pathToString(dstPath));
    }
  }

}
