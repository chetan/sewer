package net.pixelcop.sewer;

/**
 * Exposes a method used for dynamically loading plugins
 *
 * @author chetan
 *
 */
public interface PlumbingProvider {

  /**
   * Register the class with the appropriate Registry
   * ({@link SourceRegistry} or {@link SinkRegistry})
   */
  public abstract void register();

}
