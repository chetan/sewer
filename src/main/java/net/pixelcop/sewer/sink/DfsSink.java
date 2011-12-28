package net.pixelcop.sewer.sink;

import java.io.DataOutputStream;
import java.io.IOException;

import net.pixelcop.sewer.Event;
import net.pixelcop.sewer.Sink;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.Compressor;
import org.apache.hadoop.io.compress.GzipCodec;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Simple wrapper around {@link FSDataOutputStream} with compression
 *
 * @author chetan
 *
 */
public class DfsSink implements Sink {

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

  public DfsSink(String path) {
    this.configPath = path;
  }

  @Override
  public void close() throws IOException {
    writer.close();
  }

  @Override
  public void open() throws IOException {

    String fullPath = BucketPath.escapeString(configPath, null);
    createWriter(fullPath);

  }

  private void createWriter(String path) throws IOException {

    Configuration conf = new Configuration();
    FileSystem hdfs;

    // String codecName = conf.getCollectorDfsCompressCodec();
    // CompressionCodec codec = getCodec(conf, codecName);
    CompressionCodec codec = new GzipCodec(); // TODO (handle pluggable
                                              // compression with HDFS)

    // if (codec == null) {
    // dstPath = new Path(path);
    // hdfs = dstPath.getFileSystem(conf);
    // writer = hdfs.create(dstPath);
    // LOG.info("Creating HDFS file: " + dstPath.toString());
    // return;
    // }

    Compressor cmp = codec.createCompressor();
    dstPath = new Path(path + codec.getDefaultExtension());
    hdfs = dstPath.getFileSystem(conf);
    writer = new DataOutputStream(codec.createOutputStream(hdfs.create(dstPath), cmp));

    if (LOG.isInfoEnabled()) {
      LOG.info("Creating " + codec + " compressed HDFS file: " + dstPath.toString());
    }
  }

  @Override
  public void append(Event event) throws IOException {
    event.write(writer);
  }

}
