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
   * Reference to DFS Path object
   */
  private Path dstPath;

  /**
   * String input of DFS path to write to
   */
  private String path;

  private DataOutputStream writer;

  @Override
  public void close() throws IOException {
    writer.close();
  }

  @Override
  public void open() throws IOException {

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
    writer = hdfs.create(dstPath);
    try {
      writer = new DataOutputStream(codec.createOutputStream(writer, cmp));

    } catch (NullPointerException npe) {
      // tries to find "native" version of codec, if that fails, then tries to
      // find java version. If there is no java version, the createOutputStream
      // exits via NPE. We capture this and convert it into a IOE with a more
      // useful error message.
      LOG.error("Unable to load compression codec " + codec);
      throw new IOException("Unable to load compression codec " + codec);
    }

    if (LOG.isInfoEnabled()) {
      LOG.info("Creating " + codec + " compressed HDFS file: " + dstPath.toString());
    }

  }

  @Override
  public void append(Event event) throws IOException {
    event.write(writer);
  }

}
