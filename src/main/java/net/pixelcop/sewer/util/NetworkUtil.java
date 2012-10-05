package net.pixelcop.sewer.util;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class NetworkUtil {

  public static final Logger LOG = LoggerFactory.getLogger(NetworkUtil.class);

  private static final String localhost;

  // try to determine local hostname
  static {
    String host = null;
    try {
      // host = InetAddress.getLocalHost().getCanonicalHostName();
      host = InetAddress.getLocalHost().getHostName();
    } catch (UnknownHostException e) {
      LOG.error("Unable to get canonical host name: " + e.getMessage(), e);
      // fallback method
      File hostfile = new File("/etc/hostname");
      if (hostfile.exists()) {
        try {
          host = new BufferedReader(new FileReader(hostfile)).readLine().trim();
        } catch (IOException e1) {
          host = "localhost";
        }
      }
    }
    localhost = host;
  }

  /**
   * Returns the hostname of the the local machine
   * @return String hostname
   */
  public static final String getLocalhost() {
    return localhost;
  }

}
