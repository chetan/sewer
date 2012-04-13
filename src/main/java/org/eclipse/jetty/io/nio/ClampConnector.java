package org.eclipse.jetty.io.nio;

import java.io.IOException;
import java.nio.channels.SelectionKey;
import java.nio.channels.SocketChannel;

import org.eclipse.jetty.io.AsyncEndPoint;
import org.eclipse.jetty.io.nio.SelectorManager.SelectSet;
import org.eclipse.jetty.server.Connector;
import org.eclipse.jetty.server.nio.SelectChannelConnector;

/**
 * Custom {@link Connector} which allows setting a threshold after which keep-alive
 * connections will be disabled in order to maintain server resources.
 *
 * @author chetan
 *
 */
public class ClampConnector extends SelectChannelConnector {

  private int disableKeepaliveThreshold;

  public ClampConnector() {
    super();
  }

  @Override
  protected SelectChannelEndPoint newEndPoint(SocketChannel channel, SelectSet selectSet,
      SelectionKey key) throws IOException {

    ClampEndpoint endp = new ClampEndpoint(channel, selectSet, key, this._maxIdleTime);
    endp.setConnection(selectSet.getManager().newConnection(channel, endp, key.attachment()));
    endp.setDisableKeepaliveThreshold(this.disableKeepaliveThreshold);
    return endp;
  }

  @Override
  protected AsyncConnection newConnection(SocketChannel channel, AsyncEndPoint endpoint) {
    return new ClampConnection(ClampConnector.this, endpoint, getServer());
  }

  public void setDisableKeepaliveThreshold(int disableKeepaliveThreshold) {
    this.disableKeepaliveThreshold = disableKeepaliveThreshold;
  }

  public int getDisableKeepaliveThreshold() {
    return disableKeepaliveThreshold;
  }

}
