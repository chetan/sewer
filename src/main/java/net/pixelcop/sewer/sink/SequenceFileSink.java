package net.pixelcop.sewer.sink;

import java.io.IOException;

import net.pixelcop.sewer.ByteArrayEvent;
import net.pixelcop.sewer.Event;
import net.pixelcop.sewer.node.Node;
import net.pixelcop.sewer.util.HdfsUtil;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.io.SequenceFile.Writer;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Simple wrapper around {@link FSDataOutputStream} with compression
 *
 * @author chetan
 *
 */
public class SequenceFileSink extends BucketedSink {

  private static final Logger LOG = LoggerFactory.getLogger(SequenceFileSink.class);

  private static final NullWritable NULL = NullWritable.get();

  /**
   * Configured DFS path to write to
   */
  private String configPath;

  /**
   * Reference to DFS Path object
   */
  private Path dstPath;

  private Writer writer;

  public SequenceFileSink(String[] args) {
    this.configPath = args[0];
  }

  @Override
  public void close() throws IOException {
    if (LOG.isDebugEnabled()) {
      LOG.debug("Closing SequenceFileSink: " + dstPath.toString());
    }

    if (writer != null) {
      writer.close();
    }
    nextBucket = null;
    setStatus(CLOSED);

    if (LOG.isInfoEnabled()) {
      LOG.info("Closed SequenceFileSink: " + dstPath.toString());
    }
  }

  @Override
  public void open() throws IOException {
    setStatus(OPENING);
    if (nextBucket == null) {
      generateNextBucket();
    }
    createWriter();
    setStatus(FLOWING);
  }

  private void createWriter() throws IOException {

    Configuration conf = Node.getInstance().getConf();

    CompressionCodec codec = HdfsUtil.createCodec();
    dstPath = new Path(nextBucket + ".seq" + codec.getDefaultExtension());
    FileSystem hdfs = dstPath.getFileSystem(conf);

    writer = SequenceFile.createWriter(
        hdfs, conf, dstPath, NullWritable.class, ByteArrayEvent.class, CompressionType.BLOCK, codec);

    if (LOG.isInfoEnabled()) {
      LOG.info("Opened SequenceFileSink: " + dstPath.toString());
    }

    nextBucket = null;
  }

  @Override
  public String getFileExt() {
    return ".seq" + HdfsUtil.createCodec().getDefaultExtension();
  }

  @Override
  public String generateNextBucket() {
    nextBucket = BucketPath.escapeString(configPath, null);
    return nextBucket;
  }

  @Override
  public void append(Event event) throws IOException {
    writer.append(NULL, event);
  }

}
