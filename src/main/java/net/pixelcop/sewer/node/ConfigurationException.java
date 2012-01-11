package net.pixelcop.sewer.node;

/**
 * Indicates a configuration problem, thrown during Node startup
 *
 * @author chetan
 *
 */
public class ConfigurationException extends Exception {

  private static final long serialVersionUID = -2709873700603906877L;

  public ConfigurationException() {
    super();
  }

  public ConfigurationException(String message, Throwable cause) {
    super(message, cause);
  }

  public ConfigurationException(String message) {
    super(message);
  }

  public ConfigurationException(Throwable cause) {
    super(cause);
  }

}
