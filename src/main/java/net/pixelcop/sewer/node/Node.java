package net.pixelcop.sewer.node;

import net.pixelcop.sewer.rpc.MasterAPI;
import net.pixelcop.sewer.rpc.SmartRpcClient;

public class Node extends Thread {

  private static Node instance;

  private MasterAPI master;

  public static Node getInstance() {
    return instance;
  }

  public static void main(String[] args) {

    if (args.length == 0) {
      System.err.println("Missing node type");
      System.err.println("usage: Node.class <agent|collector>");
      System.exit(1);
    }

    instance = new Node(args[0]);
    instance.start();
  }

  public Node(String nodeType) {

    setName("Node");

    try {
      master = (MasterAPI) SmartRpcClient.createClient(MasterAPI.class);
      if (!master.handshake("foo_node")) {
        System.err.println("HANDSHAKE FAILED");
        System.exit(1);
      }

    } catch (Exception e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }

  }

  @Override
  public void run() {

    // TODO do we even need to do anything with the master in a loop? heartbeat? check configs?

  }


}


