package net.pixelcop.sewer.sink.durable;

/**
 * Implement this interface to receive events from a {@link SubSinkOpenerThread}
 *
 * @author chetan
 *
 */
public interface SubSinkOpenerEvents {

  /**
   * Fired when the SubSink is opened
   */
  public void onSubSinkOpen();

}
