package org.eclipse.jetty.io.nio;

import org.eclipse.jetty.io.EndPoint;
import org.eclipse.jetty.server.AsyncHttpConnection;
import org.eclipse.jetty.server.Connector;
import org.eclipse.jetty.server.Server;

public class ClampConnection extends AsyncHttpConnection {

  protected boolean close;

  public ClampConnection(Connector connector, EndPoint endpoint, Server server) {
    super(connector, endpoint, server);
  }

  public void setClose(boolean close) {
    this.close = close;
  }

  /**
   * If true, the "Connection: close" header should be sent
   * @return boolean
   */
  public boolean getClose() {
    return close;
  }

}
