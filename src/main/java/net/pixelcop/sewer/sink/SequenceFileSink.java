package net.pixelcop.sewer.sink;

import java.io.IOException;

import net.pixelcop.sewer.ByteArrayEvent;
import net.pixelcop.sewer.Event;
import net.pixelcop.sewer.Sink;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.io.SequenceFile.Writer;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.DeflateCodec;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.util.NativeCodeLoader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Simple wrapper around {@link FSDataOutputStream} with compression
 *
 * @author chetan
 *
 */
public class SequenceFileSink extends Sink {

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

  public SequenceFileSink(String path) {
    this.configPath = path;
  }

  @Override
  public void close() throws IOException {
    if (writer != null) {
      writer.close();
    }
  }

  @Override
  public void open() throws IOException {
    String fullPath = BucketPath.escapeString(configPath, null);
    createWriter(fullPath);
  }

  private void createWriter(String path) throws IOException {

    Configuration conf = new Configuration();
    conf.setInt("io.file.buffer.size", 16384*4); // temp workaround until we fix Config

    // TODO handle pluggable compression codec
    CompressionCodec codec;
    if (NativeCodeLoader.isNativeCodeLoaded()) {
      codec = new GzipCodec();
    } else {
      codec = new DeflateCodec();
    }

    dstPath = new Path(path + ".seq" + codec.getDefaultExtension());
    FileSystem hdfs = dstPath.getFileSystem(conf);

    writer = SequenceFile.createWriter(
        hdfs, conf, dstPath, NullWritable.class, ByteArrayEvent.class, CompressionType.BLOCK, codec);

    if (LOG.isInfoEnabled()) {
      LOG.info("Creating " + codec.getClass().getSimpleName() + " compressed HDFS file: " + dstPath.toString());
    }
  }

  @Override
  public void append(Event event) throws IOException {
    writer.append(NULL, event);
  }

}
