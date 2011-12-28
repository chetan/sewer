package net.pixelcop.sewer.master;

import java.io.IOException;

import net.pixelcop.sewer.rpc.MasterAPI;
import net.pixelcop.sewer.rpc.SmartRpcServer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Master {

  private static final Logger LOG = LoggerFactory.getLogger(Master.class);

  public static void main(String[] args) {

    SmartRpcServer rpcServer = null;
    try {
      rpcServer = new SmartRpcServer();
      rpcServer.registerAPIHandler(MasterAPI.class, new MasterRPCHandler());

    } catch (IOException e) {
      e.printStackTrace();
    }
    rpcServer.start();

  }

}
