package net.pixelcop.sewer.node;

import java.io.IOException;

import net.pixelcop.sewer.Sink;
import net.pixelcop.sewer.Source;
import net.pixelcop.sewer.rpc.MasterAPI;
import net.pixelcop.sewer.rpc.SmartRpcClient;
import net.pixelcop.sewer.rpc.SmartRpcClientEventHandler;
import net.pixelcop.sewer.rpc.SmartRpcServer;
import net.pixelcop.sewer.sink.NullSink;
import net.pixelcop.sewer.source.SyslogTcpSource;

public class Node extends Thread implements SmartRpcClientEventHandler {

  private static Node instance;

  private MasterAPI master;
  private String nodeType;

  private Source source;
  private Sink sink;

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
    try {
      instance.configure();

    } catch (IOException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
    instance.start();
  }

  /**
   * Load node configuration and start source/sink
   * @throws IOException
   */
  public void configure() throws IOException {

    // for now, lets just start using static configs based on the node type/name

    if (this.nodeType.equalsIgnoreCase("agent")) {

      this.sink = new NullSink();
      this.sink.open();

      this.source = new SyslogTcpSource(9888);
      this.source.setSink(this.sink);
      this.source.open();

      return;
    }

  }

  public Node(String nodeType) {

    setName("Node Thread " + getId());

    this.setNodeType(nodeType);

    String host = "localhost";
    int port = SmartRpcServer.DEFAULT_PORT;

    try {

      SmartRpcClient client = new SmartRpcClient(host, port);
      client.setEventHandler(this);
      client.start();

      master = (MasterAPI) SmartRpcClient.createClient(
          MasterAPI.class, client);

    } catch (Exception e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
      System.err.println("Startup failed! Bailing out");
      System.exit(1);
    }

  }

  @Override
  public void run() {

    // TODO do we even need to do anything with the master in a loop? heartbeat? check configs?

  }

  @Override
  public void onConnect() {
    if (!master.handshake(nodeType)) {
      System.err.println("HANDSHAKE FAILED");
      System.exit(1);
    }
    System.out.println("Handshake succeeded");
  }

  @Override
  public void onDisconnect() {
    // nothing for now
    System.out.println("onDisconnect() raised");
  }


  // GETTERS & SETTERS

  public void setNodeType(String nodeType) {
    this.nodeType = nodeType;
  }

  public String getNodeType() {
    return nodeType;
  }

}


