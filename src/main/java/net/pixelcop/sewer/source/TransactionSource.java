package net.pixelcop.sewer.source;

import java.io.EOFException;
import java.io.IOException;
import java.util.concurrent.TimeUnit;

import net.pixelcop.sewer.ByteArrayEvent;
import net.pixelcop.sewer.Event;
import net.pixelcop.sewer.Sink;
import net.pixelcop.sewer.Source;
import net.pixelcop.sewer.node.Node;
import net.pixelcop.sewer.sink.BucketedSink;
import net.pixelcop.sewer.sink.DfsSink;
import net.pixelcop.sewer.sink.SequenceFileSink;
import net.pixelcop.sewer.sink.durable.Transaction;
import net.pixelcop.sewer.util.HdfsUtil;
import net.pixelcop.sewer.util.TimeoutThread;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.ChecksumException;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.Reader;
import org.apache.hadoop.io.VLongWritable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class TransactionSource extends Source {

  private static final Logger LOG = LoggerFactory.getLogger(TransactionSource.class);

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
    if (sink != null && sink.getStatus() != CLOSED) {
      sink.close();
    }
  }

  /**
   * Opens the SequenceFile and drains its contents to the configured Sink
   */
  @Override
  public void open() throws IOException {
    LOG.debug("Going to drain " + path.toString() + " to bucket " + bucket);
    setStatus(FLOWING);

    sink = getSinkFactory().build();
    if (sink instanceof SequenceFileSink || sink instanceof DfsSink) {

      // before we do anything, let's delete the existing destination file so
      // we don't have any problems
      final Path destFile = new Path(bucket + ext);
      deleteExistingFile(destFile);

      copySequenceFileToDfs(path, destFile);

    } else {
      copySequenceFileToSink();
    }

    setStatus(CLOSED);
  }

  /**
   * Copy a file directly to output, byte by byte
   *
   * @param input
   * @param output
   * @throws IOException
   */
  private void copySequenceFileToDfs(Path input, Path output) throws IOException {
    Configuration conf = new Configuration();
    output.getFileSystem(conf).copyFromLocalFile(input, output);
  }

  /**
   * Read a sequence file and copy it to the subsink
   *
   * @throws IOException
   */
  private void copySequenceFileToSink() throws IOException {

    if (sink instanceof BucketedSink && bucket != null) {
      ((BucketedSink) sink).setNextBucket(bucket);
    }
    sink.open();

    Reader reader = null;
    try {
      try {
        reader = createReader();
      } catch (IOException e) {
        // May occur if the file wasn't found or is zero bytes (never received any data)
        // This generally happens if the server was stopped improperly (kill -9, crash, reboot)
        LOG.warn("Failed to open tx " + tx + " at " + tx.createTxPath().toString()
            + "; this usually means the file is 0 bytes or the header is corrupted/incomplete", e);
        return;
      }
      setStatus(FLOWING);

      Event event = null;
      VLongWritable lng = new VLongWritable();
      try {
        event = tx.newEvent();
      } catch (Exception e) {
        // Should really never happen, since the Event class should always be available
        throw new IOException("Failed to create Event class", e);
      }


      while (true) {

        try {
          if (!reader.next(event, lng)) {
            break;
          }
        } catch (IOException e) {
          if (e instanceof EOFException) {
            LOG.warn("Caught EOF reading from buffer; skipping to close");
            break;
          } else if (e instanceof ChecksumException) {
            LOG.warn("Caught ChecksumException reading from buffer; skipping to close");
            break;
          }
          throw e;
        }

        sink.append(event);
      }


    } finally {
      if (reader != null) {
        try {
          reader.close();
        } catch (IOException e) {
          // safe to ignore (hopefully :)
          // because at this point, we only care about errors when closing the sink
        }
      }
    }


  }

  /**
   * Try to delete the existing file. Throws exception if it takes more than 10 seconds
   * @throws IOException
   */
  private void deleteExistingFile(final Path file) throws IOException {

    TimeoutThread t = new TimeoutThread() {
      @Override
      public void work() throws Exception {
        try {
          HdfsUtil.deletePath(file);
        } catch (InterruptedException e) {
          throw new IOException("Interrupted trying to delete " + file, e);
        }
      }
    };
    if (!t.await(10, TimeUnit.SECONDS)) {
      setStatus(ERROR);
      throw new IOException("Error trying to delete " + file, t.getError());
    }
  }

  private Reader createReader() throws IOException {
    Configuration conf = Node.getInstance().getConf();
    return new SequenceFile.Reader(conf, Reader.file(path));
  }

  @Override
  public Class<?> getEventClass() {
    return ByteArrayEvent.class;
  }

}
