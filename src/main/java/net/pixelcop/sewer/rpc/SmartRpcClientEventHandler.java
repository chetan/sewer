package net.pixelcop.sewer.rpc;

/**
 * Events raised by RPC Client; these operations MUST be thread-safe.
 *
 * @author chetan
 *
 */
public interface SmartRpcClientEventHandler {

  public void onConnect();
  public void onDisconnect();

}
