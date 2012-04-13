package org.eclipse.jetty.io.nio;

import java.io.IOException;
import java.nio.channels.SelectionKey;
import java.nio.channels.SocketChannel;
import java.util.concurrent.atomic.AtomicBoolean;

import org.eclipse.jetty.io.nio.SelectorManager.SelectSet;

public class ClampEndpoint extends SelectChannelEndPoint {

  private final AtomicBoolean firstReq;
  private int disableKeepaliveThreshold;

  public ClampEndpoint(SocketChannel channel, SelectSet selectSet, SelectionKey key, int maxIdleTime)
      throws IOException {
    super(channel, selectSet, key, maxIdleTime);
    firstReq = new AtomicBoolean(true);
  }

  @Override
  protected void handle() {

    if (disableKeepaliveThreshold > 0 &&
        firstReq.get() && getSelectSet().getSelector().keys().size() > disableKeepaliveThreshold) {

      ((ClampConnection) getConnection()).setClose(true);
    }

    if (disableKeepaliveThreshold > 0 && firstReq.get()) {
      firstReq.set(false);
    }

    super.handle();
  }

  public void setDisableKeepaliveThreshold(int disableKeepaliveThreshold) {
    this.disableKeepaliveThreshold = disableKeepaliveThreshold;
  }

  public int getDisableKeepaliveThreshold() {
    return disableKeepaliveThreshold;
  }

}
