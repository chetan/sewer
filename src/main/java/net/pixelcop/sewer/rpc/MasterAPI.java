package net.pixelcop.sewer.rpc;

public interface MasterAPI {

  public boolean handshake(String nodeName);

  public String getConfig(String nodeName);

}
