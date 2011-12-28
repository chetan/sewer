package net.pixelcop.sewer.util;

import org.slf4j.Logger;

import com.google.common.base.Stopwatch;

public class BackoffHelper {

  private int failures;
  private Stopwatch stopwatch;

  public BackoffHelper() {
    failures = 0;
    stopwatch = new Stopwatch();
    stopwatch.start();
  }

  /**
   * Increments failure count, prints a diagnostic message and sleeps for some amount of time
   * based on the number of failures that have occurred. Max sleep time is 30 seconds.
   *
   * @param t
   * @param log
   * @param msg
   */
  public void handleFailure(Throwable t, Logger log, String msg) {

    failures++;
    if (failures == 1 && log.isWarnEnabled()) {
      log.warn(msg + ", failures = " + failures + " (" + t.getMessage() + ")");

    } else if (log.isDebugEnabled()) {
      log.debug(msg + ", failures = " + failures + " (" + t.getMessage() + ")");
    }

    int backoff = 5000;
    if (failures > 30) {
      backoff = 10000;

    } else if (failures > 100) {
      backoff = 30000;
    }

    try {
      Thread.sleep(backoff);
    } catch (InterruptedException e1) {
      return;
    }

  }

  /**
   * After successfully completing the action, print a diagnostic message if necessary
   *
   * @param log
   * @param msg
   */
  public void resolve(Logger log, String msg) {

    if (!log.isDebugEnabled() && (failures == 0 || !log.isInfoEnabled())) {
      return;
    }

    // will always be shown if debug is enabled
    log.info(msg + " after " + failures + " failures (" + getElapsedTime() + ")");
  }

  public int getNumFailures() {
    return failures;
  }

  public String getElapsedTime() {
    return stopwatch.toString();
  }

}
