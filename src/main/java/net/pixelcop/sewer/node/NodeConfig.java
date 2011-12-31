package net.pixelcop.sewer.node;

import java.util.Enumeration;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;

public class NodeConfig extends Configuration {

  public static final String SOURCE = "sewer.source";
  public static final String SINK = "sewer.sink";

  public NodeConfig() {
    super();
  }

  public NodeConfig(boolean loadDefaults) {
    super(loadDefaults);
  }

  public NodeConfig(Configuration other) {
    super(other);
  }

  /**
   * Add the properties contained in file into this configuration
   * @param props
   */
  public void addResource(Properties props) {

    for (Enumeration<?> e = props.propertyNames(); e.hasMoreElements();) {
      String name = (String) e.nextElement();
      set(name, props.getProperty(name));
    }

  }

}