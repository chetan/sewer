package net.pixelcop.sewer.sink.durable;

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
class SubSinkOpenerThread extends Thread {

  private static final Logger LOG = LoggerFactory.getLogger(SubSinkOpenerThread.class);

  private Sink sink;
  private SubSinkOpenerEvents callback;

  public SubSinkOpenerThread(long id, Sink sink, SubSinkOpenerEvents callback) {
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

    while (sink.getStatus() != Sink.FLOWING) {

      try {
        sink.open();
        backoff.resolve(LOG, "SubSink opened");
        if (this.callback != null) {
          this.callback.onSubSinkOpen(); // notify parent
        }
        return;

      } catch (Exception e) {
        backoff.handleFailure(e, LOG, "Error opening subsink (" + subSinkName + ")");

      }

    }

  }

}