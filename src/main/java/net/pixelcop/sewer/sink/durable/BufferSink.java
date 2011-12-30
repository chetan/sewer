package net.pixelcop.sewer.sink.durable;

import java.io.IOException;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import net.pixelcop.sewer.Event;
import net.pixelcop.sewer.Sink;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BufferSink extends Sink implements Runnable {

  private static final Logger LOG = LoggerFactory.getLogger(BufferSink.class);

  private static final long NANO_WAIT = TimeUnit.SECONDS.toNanos(3);

  private LinkedBlockingQueue<Event> buffer;
  private final String name;
  private final Sink parentSink;

  private final Thread thread;

  /**
   * Buffering sink which will close when the parent sink closes and the queue is empty
   *
   * @param name Thread name
   * @param parentSink Parent sink which opened this buffer
   */
  public BufferSink(String name, Sink parentSink) {
    this.name = "BufferSink::" + name + " " + Thread.currentThread().getId();
    this.parentSink = parentSink;

    this.thread = new Thread(this);
    this.thread.setName(this.name);
  }

  @Override
  public void close() throws IOException {

    thread.interrupt();

    try {
      thread.join();
    } catch (InterruptedException e) {
      LOG.error("Interrupted while waiting for " + name + " thread to join");
    }

  }

  @Override
  public void open() throws IOException {
    setStatus(OPENING);
    buffer = new LinkedBlockingQueue<Event>(100000);
    thread.start();
    setStatus(FLOWING);
  }

  @Override
  public void append(Event event) throws IOException {
    try {
      this.buffer.put(event);
    } catch (InterruptedException e) {
      LOG.error("interrupted while putting event in persist queue");
    }
  }

  @Override
  public void run() {

    while (parentSink.getStatus() == FLOWING && subSink.getStatus() != FLOWING)  {
      // wait for the subsink to open as long as our parent sink is open/flowing
      // TODO may need to do this multiple times in the case of multiple failures/recoveries
      // TODO some other way to wait for subsink to open? await/notify?
    }

    if (LOG.isDebugEnabled()) {
      LOG.debug(name + " SubSink opened");
    }

    while (parentSink.getStatus() == FLOWING || !buffer.isEmpty()) {
      // run until the sink closes and no events are left

      try {
        Event e = buffer.poll(NANO_WAIT, TimeUnit.NANOSECONDS);
        if (e != null) {
          subSink.append(e);
        }

      } catch (InterruptedException e) {
        if (LOG.isWarnEnabled() && buffer.size() > 0) {
          LOG.warn(name + " was interrupted with " + buffer.size() + " events left in the buffer");
        } else if (LOG.isDebugEnabled()) {
          LOG.debug(name + " was interrupted with an empty event buffer");
        }

      } catch (IOException e) {
        LOG.error("Caught an error while appending to the subsink ("
            + subSink.getClass().getSimpleName() + ": " + e.getMessage(), e);

        // TODO shutdown the reliable sink and maybe the source too

      }

    }

//    try {
//      subSink.close();
//    } catch (IOException e) {
//      LOG.error("Error closing durable sink", e);
//    }

    if (LOG.isDebugEnabled()) {
      LOG.debug(name + " - run finished");
    }
  }

}
