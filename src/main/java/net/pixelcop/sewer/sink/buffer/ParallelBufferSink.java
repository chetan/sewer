package net.pixelcop.sewer.sink.buffer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import net.pixelcop.sewer.Event;
import net.pixelcop.sewer.Sink;
import net.pixelcop.sewer.node.Node;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.codahale.metrics.Gauge;

public class ParallelBufferSink extends Sink {

  class DrainThread extends Thread {

    private Sink drainSink;

    public DrainThread() throws IOException {
      drainSink = getSinkFactory().build();
      drainSink.open();
      setName("DrainThread-" + getId());
    }

    @Override
    public void run() {

      while (getStatus() == FLOWING || !buffer.isEmpty()) {
        // run until the sink closes and no events are left

        try {
          Event e = buffer.poll(NANO_WAIT, TimeUnit.NANOSECONDS);
          if (e != null) {
            drainSink.append(e);
          }

        } catch (InterruptedException e) {
          if (LOG.isDebugEnabled()) {
            if (buffer.size() > 0) {
              LOG.debug("interrupted with " + buffer.size() + " events left in the buffer, going to run until empty");
            } else {
              LOG.debug("interrupted with an empty event buffer");
            }
          }

        } catch (IOException e) {
          LOG.error("Caught an error while appending to the subsink ("
              + drainSink.getClass().getSimpleName() + ": " + e.getMessage(), e);

          // nothing else we can do, bail out
          // it's up to whoever started this thread to retry
          setStatus(ERROR);
          return;
        }

      }

      if (LOG.isDebugEnabled()) {
        LOG.debug(name + " - run finished with " + buffer.size() + " events left");
      }
    }

    public void closeSink() throws IOException {
      drainSink.close();
    }
  }

  private static final Logger LOG = LoggerFactory.getLogger(ParallelBufferSink.class);

  private static final long NANO_WAIT = TimeUnit.SECONDS.toNanos(3);

  private static final String DEFAULT_THREAD_NAME = "DrainThread";

  private LinkedBlockingQueue<Event> buffer;

  private final int bufferSize;
  private final int numThreads;
  private final String name;

  private final List<DrainThread> threads;

  /**
   * Buffering sink which will close when the parent sink closes and the queue is empty
   *
   * @param args
   */
  public ParallelBufferSink(String[] args) {
    this.bufferSize = getInt(args, 0, 100000);
    this.numThreads = getInt(args, 1, 1);
    this.name = getString(args, 2, DEFAULT_THREAD_NAME);

    this.threads = new ArrayList<DrainThread>();
  }

  @Override
  public void close() throws IOException {

    if (LOG.isDebugEnabled()) {
      LOG.debug(name + " closing");
    }
    setStatus(CLOSING);

    for (DrainThread thread : threads) {
      thread.interrupt();
    }

    try {
      for (DrainThread thread : threads) {
        thread.join();
        thread.closeSink();
      }
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
    LOG.debug("opening");
    setStatus(OPENING);

    buffer = new LinkedBlockingQueue<Event>(bufferSize);
    LOG.debug("created buffer of size " + bufferSize);

    if (!Node.getInstance().getMetricReporters().isEmpty()) {
      // create gauge on queue size if any reporters are registered (metrics enabled)
      String metricName = "buffer_queue_size";
      if (name != DEFAULT_THREAD_NAME) {
        metricName = name + "." + metricName;
      }
      Node.getInstance().getMetricRegistry().register(metricName, new Gauge<Integer>() {
        @Override
        public Integer getValue() {
          return buffer.size();
        }
      });
    }

    for (int i = 0; i < numThreads; i++) {
      LOG.debug("opening thread " + i);
      DrainThread t = new DrainThread();
      threads.add(t);
    }

    LOG.debug("launched " + threads.size() + " drain threads");
    setStatus(FLOWING);

    for (Thread t : threads) {
      t.start();
    }
  }

  @Override
  public void append(Event event) throws IOException {
    try {
      this.buffer.put(event);
    } catch (InterruptedException e) {
      LOG.error("interrupted while putting event in buffer queue");
    }
  }



}
