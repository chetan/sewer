package net.pixelcop.sewer.sink.durable;

import java.io.IOException;

import net.pixelcop.sewer.Sink;
import net.pixelcop.sewer.util.BackoffHelper;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Asynchronously open the given Sink, retrying until it actually opens
 *
 * @author chetan
 *
 */
public class SinkOpenerThread extends Thread {

  private static final Logger LOG = LoggerFactory.getLogger(SinkOpenerThread.class);

  private static final int RUNNING  = 1;
  private static final int CANCELED = -1;

  private int status = RUNNING;
  private Sink sink;
  private SinkOpenerEvents callback;

  public SinkOpenerThread(long id, Sink sink, SinkOpenerEvents callback) {
    setName("SubSinkOpener " + id);
    this.sink = sink;
    this.callback = callback;
  }

  @Override
  public void run() {

    BackoffHelper backoff = new BackoffHelper();

    String subSinkName = sink.getClass().getSimpleName();
    if (LOG.isDebugEnabled()) {
      LOG.debug("Opening SubSink: " + subSinkName);
    }

    while (status != CANCELED && sink.getStatus() != Sink.FLOWING) {

      try {
        sink.open();
        backoff.resolve(LOG, "SubSink opened");
        if (this.callback != null) {
          this.callback.onSubSinkOpen(); // notify parent
        }
        return;

      } catch (Throwable t) {
        // We want to catch all exceptions while trying to open
        try {
          backoff.handleFailure(t, LOG,
              "Error opening subsink (" + subSinkName + ")",
              status == CANCELED);
        } catch (InterruptedException e1) {
          LOG.debug("opener was interrupted; bailing");
          return;
        }

      }

    }

    if (status == CANCELED) {
      LOG.debug("sink opener canceled");
      if (sink.getStatus() == Sink.FLOWING) {
        // opened on the last try, lets shut it down
        try {
          sink.close();
        } catch (IOException e) {
          LOG.debug("sink failed to close while canceling SubSinkOpener");
        }
      }
    }

  }

  /**
   * Signal the Opener to cancel ASAP
   */
  public void cancel() {
    this.interrupt();
    this.status = CANCELED;
  }

}