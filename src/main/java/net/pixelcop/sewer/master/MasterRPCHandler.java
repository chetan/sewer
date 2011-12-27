package net.pixelcop.sewer.master;

import net.pixelcop.sewer.rpc.MasterAPI;

public class MasterRPCHandler implements MasterAPI {

  public MasterRPCHandler() {
  }

  @Override
  public boolean handshake(String nodeName) {
    System.out.println("Handshake: " + nodeName);
    return true;
  }

  @Override
  public String getConfig(String nodeName) {
    // TODO Auto-generated method stub
    return null;
  }

}
