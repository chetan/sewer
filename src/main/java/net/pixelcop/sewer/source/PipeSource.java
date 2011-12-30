package net.pixelcop.sewer.source;

import java.io.EOFException;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;

import net.pixelcop.sewer.Event;
import net.pixelcop.sewer.Sink;
import net.pixelcop.sewer.Source;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Single-threaded source which reads input from a Unix Socket / pipe. Extremely fast for local
 * events.
 *
 * @author chetan
 *
 */
public class PipeSource extends Source implements Runnable {

  private static final Logger LOG = LoggerFactory.getLogger(PipeSource.class);

  private File pipe;
  private Thread thread;

  private Sink sink;

  private SyslogWireExtractor extractor;

  public PipeSource(String[] args) {
    pipe = new File(args[0]);
  }

  @Override
  public void close() throws IOException {
    if (thread != null && thread.isAlive()) {
      thread.interrupt();
    }
    if (extractor != null) {
      extractor.close();
    }
  }

  @Override
  public void open() throws IOException {

   if (!pipe.exists()) {
     Runtime.getRuntime().exec("mkfifo " + pipe.getAbsolutePath());
   }

   this.sink = createSink();

   reopenPipe();

   thread = new Thread(this);
   thread.setName("PipeSource Reader");
   thread.start();
  }

  private void reopenPipe() throws FileNotFoundException {
    if (extractor != null) {
      try {
        extractor.close();
      } catch (IOException e) {
      }
      extractor = null;
    }
    extractor = new SyslogWireExtractor(new FileInputStream(pipe));
  }

  @Override
  public void run() {

    while (true) {

      try {

        Event e = extractor.extractEvent();
        sink.append(e);

      } catch (EOFException e) {
        try {
          reopenPipe();
        } catch (FileNotFoundException e1) {
          LOG.error("Failed to reopen pipe: " + e.getMessage(), e);
          return;
        }

      } catch (IOException e) {
        LOG.error("Error extracting event: " + e.getMessage(), e);
      }


    }

  }

}
