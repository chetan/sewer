package net.pixelcop.sewer.sink;

import net.pixelcop.sewer.Sink;

/**
 * Sink that supports "output bucketing", generally in a path name where data will be
 * written to when this Sink is open. The bucket changes every time the Sink is opened
 * and only when it is opened.
 *
 * @author chetan
 *
 */
public abstract class BucketedSink extends Sink {

  protected String nextBucket;

  public abstract String getFileExt();

  public abstract String generateNextBucket();

  public void setNextBucket(String nextBucket) {
    this.nextBucket = nextBucket;
  }

  public String getNextBucket() {
    return nextBucket;
  }

}
