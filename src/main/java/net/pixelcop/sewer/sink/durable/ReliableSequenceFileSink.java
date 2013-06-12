package net.pixelcop.sewer.sink.durable;

import java.io.IOException;

import net.pixelcop.sewer.DrainSink;
import net.pixelcop.sewer.node.Node;
import net.pixelcop.sewer.sink.SequenceFileSink;
import net.pixelcop.sewer.util.HdfsUtil;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.io.SequenceFile.Writer;
import org.apache.hadoop.io.VLongWritable;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * An extension of the {@link SequenceFileSink} which writes simultaneously to an HDFS location
 * and a local disk buffer. A transaction will be marked as failed (rolled back) if the HDFS path
 * does not open and close cleanly.
 *
 * @author chetan
 *
 */
@DrainSink
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
    dstPath = new Path(nextBucket + ".seq");

    reliableOut = new DualFSDataOutputStream(localPath, dstPath, conf);

    this.writer = SequenceFile.createWriter(
        conf,
        Writer.stream(reliableOut),
        Writer.keyClass(Node.getInstance().getSource().getEventClass()),
        Writer.valueClass(VLongWritable.class),
        Writer.compression(CompressionType.BLOCK, codec)
        );

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

    try {
      if (writer != null) {
        writer.close();
      }
    } catch (IOException e) {
      LOG.error("sequence file writer failed to close for txid " + tx.getId(), e);
      tx.rollback();
    }

    try {
      if (reliableOut != null) {
        reliableOut.close();
      }
    } catch (IOException e) {
      LOG.error("reliable outpustream failed to close for txid " + tx.getId());
      if (tx.isOpen()) {
        tx.rollback();
      }
    }

    if (tx.isOpen()) {
      if (reliableOut.getStatus() == ERROR) {
        LOG.warn("reliable output stream was in error state for txid " + tx.getId());
        tx.rollback();

      } else {
        // closed cleanly, commit tx
        tx.commit();
      }
    }

    nextBucket = null;
    setStatus(CLOSED);

    if (LOG.isInfoEnabled()) {
      LOG.info("Closed: " + HdfsUtil.pathToString(localPath));
      LOG.info("Closed: " + HdfsUtil.pathToString(dstPath));
    }
  }

}
