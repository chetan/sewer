package net.pixelcop.sewer.source;

import java.io.IOException;

import net.pixelcop.sewer.ByteArrayEvent;
import net.pixelcop.sewer.Sink;
import net.pixelcop.sewer.Source;
import net.pixelcop.sewer.sink.BucketedSink;
import net.pixelcop.sewer.sink.durable.TransactionManager;
import net.pixelcop.sewer.util.HdfsUtil;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.Reader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TransactionSource extends Source {

  private static final Logger LOG = LoggerFactory.getLogger(TransactionManager.class);

  private Path path;
  private String bucket;
  private String ext;
  private Sink sink;

  public TransactionSource(Path path, String bucket, String ext) {
    this.path = path;
    this.bucket = bucket;
    this.ext = ext;
  }

  @Override
  public void close() throws IOException {
    if (sink != null) {
      sink.close();
    }
  }

  /**
   * Opens the SequenceFile and drains its contents to the configured Sink
   */
  @Override
  public void open() throws IOException {

    LOG.debug("Going to drain " + path.toString() + " to bucket " + bucket);

    // before we do anything, let's delete the existing destination file so
    // we don't have any problems
    HdfsUtil.deletePath(new Path(bucket + ext));

    sink = getSinkFactory().build();
    if (sink instanceof BucketedSink && bucket != null) {
      ((BucketedSink) sink).setNextBucket(bucket);
    }
    sink.open();

    Reader reader = null;
    try {
      reader = createReader();

      NullWritable nil = NullWritable.get();
      ByteArrayEvent event = new ByteArrayEvent();

      while (reader.next(nil, event)) {
        sink.append(event);
      }

    } finally {
      if (reader != null) {
        try {
          reader.close();
        } catch (IOException e) {
          // safe to ignore (hopefully :)
        }
      }
    }

  }

  public Reader createReader() throws IOException {

    Configuration conf = new Configuration();
    conf.setInt("io.file.buffer.size", 16384*4); // TODO temp workaround until we fix Config
    FileSystem hdfs = path.getFileSystem(conf);

    return new SequenceFile.Reader(hdfs, path, conf);
  }

}
