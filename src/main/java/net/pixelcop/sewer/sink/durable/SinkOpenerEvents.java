package net.pixelcop.sewer.sink.durable;

/**
 * Implement this interface to receive events from a {@link SinkOpenerThread}
 *
 * @author chetan
 *
 */
public interface SinkOpenerEvents {

  /**
   * Fired when the SubSink is opened
   */
  public void onSubSinkOpen();

}
