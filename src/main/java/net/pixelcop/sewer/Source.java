package net.pixelcop.sewer;


public abstract class Source extends Plumbing {

  /**
   * The class which this source uses for new Events
   *
   * @return
   */
  public abstract Class<?> getEventClass();

}
