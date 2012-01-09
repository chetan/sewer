package net.pixelcop.sewer.source;

import java.io.IOException;

import net.pixelcop.sewer.ByteArrayEvent;
import net.pixelcop.sewer.Event;
import net.pixelcop.sewer.Sink;
import net.pixelcop.sewer.Source;
import net.pixelcop.sewer.node.Node;
import net.pixelcop.sewer.sink.BucketedSink;
import net.pixelcop.sewer.sink.durable.Transaction;
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

  private Transaction tx;

  private Path path;
  private String bucket;
  private String ext;
  private Sink sink;

  public TransactionSource(Transaction tx) {

    this.tx = tx;

    this.path = tx.createTxPath();
    this.bucket = tx.getBucket();
    this.ext = tx.getFileExt();
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
      Event event = null;
      try {
        event = tx.newEvent();
      } catch (Exception e) {
        // Should relaly never happen, since the Event class should always be available
        throw new IOException("Failed to create Event class", e);
      }

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

    Configuration conf = Node.getInstance().getConf();
    FileSystem hdfs = path.getFileSystem(conf);

    return new SequenceFile.Reader(hdfs, path, conf);
  }

  @Override
  public Class<?> getEventClass() {
    return ByteArrayEvent.class;
  }

}
