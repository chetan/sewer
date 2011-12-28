package net.pixelcop.sewer.master;

import net.pixelcop.sewer.rpc.MasterAPI;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MasterRPCHandler implements MasterAPI {

  private static final Logger LOG = LoggerFactory.getLogger(MasterRPCHandler.class);

  public MasterRPCHandler() {
  }

  @Override
  public boolean handshake(String nodeName) {
    if (LOG.isDebugEnabled()) {
      LOG.debug("handshake(\"" + nodeName + "\")");
    }
    // TOOD implement handshake
    return true;
  }

  @Override
  public String getConfig(String nodeName) {
    // TODO implement getConfig()
    return null;
  }

}
