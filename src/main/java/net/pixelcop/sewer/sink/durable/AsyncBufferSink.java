package net.pixelcop.sewer.sink.durable;

import java.io.IOException;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import net.pixelcop.sewer.Event;
import net.pixelcop.sewer.Sink;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AsyncBufferSink extends Sink implements Runnable {

  private static final Logger LOG = LoggerFactory.getLogger(AsyncBufferSink.class);

  private static final long NANO_WAIT = TimeUnit.SECONDS.toNanos(3);

  private LinkedBlockingQueue<Event> buffer;
  private final String name;

  private final Thread thread;

  /**
   * Buffering sink which will close when the parent sink closes and the queue is empty
   *
   * @param name Thread name
   */
  public AsyncBufferSink(String name) {
    this.name = "BufferSink::" + name + " " + Thread.currentThread().getId();

    this.thread = new Thread(this);
    this.thread.setName(this.name);
  }

  @Override
  public void close() throws IOException {

    if (LOG.isDebugEnabled()) {
      LOG.debug(name + " closing");
    }
    setStatus(CLOSING);

    thread.interrupt();

    try {
      thread.join();
    } catch (InterruptedException e) {
      LOG.error("Interrupted while waiting for " + name + " thread to join");
    }

    if (LOG.isDebugEnabled()) {
      LOG.debug(name + " closed with " + buffer.size() + " events left in buffer");
    }
    setStatus(CLOSED);
  }

  @Override
  public void open() throws IOException {
    setStatus(OPENING);
    buffer = new LinkedBlockingQueue<Event>(100000);
    setStatus(FLOWING);
    thread.start();
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

    while (getStatus() == FLOWING && subSink.getStatus() != FLOWING)  {
      // wait for the subsink to open as long as our parent sink is open/flowing
      // TODO may need to do this multiple times in the case of multiple failures/recoveries
      // TODO some other way to wait for subsink to open? await/notify?
    }

    if (getStatus() != FLOWING) {
      // parent must be closing down and the sink isn't open yet, quit trying to drain
      // the buffer. let the parent worry about it

      if (LOG.isWarnEnabled() && buffer.size() > 0) {
        LOG.warn("looks like parent sink closed with " + buffer.size() + " events left in the buffer. events lost?!");
      } else if (LOG.isDebugEnabled()) {
        LOG.debug("looks like parent sink closed with an empty event buffer");
      }
      return;
    }

    if (LOG.isDebugEnabled() && subSink.getStatus() == FLOWING) {
      LOG.debug(name + " SubSink opened");
    }

    while (getStatus() == FLOWING || !buffer.isEmpty()) {
      // run until the sink closes and no events are left

      try {
        Event e = buffer.poll(NANO_WAIT, TimeUnit.NANOSECONDS);
        if (e != null) {
          subSink.append(e);
        }

      } catch (InterruptedException e) {
        if (LOG.isWarnEnabled() && buffer.size() > 0) {
          LOG.warn("interrupted with " + buffer.size() + " events left in the buffer, going to run until empty");
        } else if (LOG.isDebugEnabled()) {
          LOG.debug("interrupted with an empty event buffer");
        }

      } catch (IOException e) {
        LOG.error("Caught an error while appending to the subsink ("
            + subSink.getClass().getSimpleName() + ": " + e.getMessage(), e);

        // nothing else we can do, bail out
        // it's up to whoever started this thread to retry
        setStatus(ERROR);
        return;
      }

    }

    if (LOG.isDebugEnabled()) {
      LOG.debug(name + " - run finished");
    }
  }

}
