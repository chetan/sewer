package net.pixelcop.sewer.sink;

import java.io.DataOutputStream;
import java.io.IOException;

import net.pixelcop.sewer.DrainSink;
import net.pixelcop.sewer.Event;
import net.pixelcop.sewer.Sink;
import net.pixelcop.sewer.node.Node;
import net.pixelcop.sewer.util.HdfsUtil;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.Compressor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Simple wrapper around {@link FSDataOutputStream} with compression
 *
 * @author chetan
 *
 */
@DrainSink
public class DfsSink extends Sink {

  private static final Logger LOG = LoggerFactory.getLogger(DfsSink.class);

  /**
   * Configured DFS path to write to
   */
  private String configPath;

  /**
   * Reference to DFS Path object
   */
  private Path dstPath;

  private DataOutputStream writer;

  public DfsSink(String[] args) {
    this.configPath = args[0];
  }

  @Override
  public void close() throws IOException {
    if (writer != null) {
      writer.close();
    }
  }

  @Override
  public void open() throws IOException {
    String fullPath = BucketPath.escapeString(configPath);
    createWriter(fullPath);
  }

  private void createWriter(String path) throws IOException {

    CompressionCodec codec = HdfsUtil.createCodec();

    Compressor cmp = codec.createCompressor();
    dstPath = new Path(path + ".dat" + codec.getDefaultExtension());
    FileSystem hdfs = dstPath.getFileSystem(Node.getInstance().getConf());

    writer = new DataOutputStream(codec.createOutputStream(hdfs.create(dstPath), cmp));

    if (LOG.isInfoEnabled()) {
      LOG.info("Creating " + codec.getClass().getSimpleName() + " compressed HDFS file: " + dstPath.toString());
    }
  }

  @Override
  public void append(Event event) throws IOException {
    event.write(writer);
  }

}
