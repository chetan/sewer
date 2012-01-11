package net.pixelcop.sewer;


public interface StatusProvider {

  /**
   * Channel is not open (default status)
   */
  public static final int CLOSED    = 0;

  /**
   * Channel is in the process of closing down
   */
  public static final int CLOSING   = 1;

  /**
   * Channel is in the process of opening
   */
  public static final int OPENING   = 2;

  /**
   * Channel is active and Events are flowing through it
   */
  public static final int FLOWING   = 3;

  /**
   * Channel is in an error state and Events are not flowing through it. This state could be set
   * during any operation: open, close, append, etc.
   */
  public static final int ERROR     = -1;

  public int getStatus();

}
