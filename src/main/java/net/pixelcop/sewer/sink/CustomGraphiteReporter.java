package net.pixelcop.sewer.sink;

import java.io.IOException;

import net.pixelcop.sewer.util.NetworkUtil;

import com.yammer.metrics.core.MetricName;
import com.yammer.metrics.reporting.GraphiteReporter;


public class CustomGraphiteReporter extends GraphiteReporter {

  public static CustomGraphiteReporter create(String host, int port, String prefix) throws IOException {

    // add local hostname to prefix
    if (prefix == null) {
      prefix = "";
    }
    if (prefix.charAt(prefix.length()-1) != '.') {
      prefix = prefix + ".";
    }
    prefix = prefix + NetworkUtil.getLocalhost().replace('.', '_') + ".";

    CustomGraphiteReporter reporter = new CustomGraphiteReporter(host, port, prefix);
    reporter.printVMMetrics = false;

    return reporter;
  }

  public CustomGraphiteReporter(String host, int port, String prefix) throws IOException {
    super(host, port, prefix);
  }

  @Override
  protected String sanitizeName(MetricName name) {
    return name.getName();
  }

}
