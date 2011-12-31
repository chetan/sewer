package net.pixelcop.sewer.node;

import java.io.IOException;

import net.pixelcop.sewer.Sink;
import net.pixelcop.sewer.SinkRegistry;
import net.pixelcop.sewer.Source;
import net.pixelcop.sewer.SourceRegistry;
import net.pixelcop.sewer.SourceSinkFactory;
import net.pixelcop.sewer.rpc.MasterAPI;
import net.pixelcop.sewer.rpc.SmartRpcClient;
import net.pixelcop.sewer.rpc.SmartRpcClientEventHandler;
import net.pixelcop.sewer.rpc.SmartRpcServer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Node extends Thread implements SmartRpcClientEventHandler {

  private static final Logger LOG = LoggerFactory.getLogger(Node.class);

  private static Node instance;

  private final NodeConfig conf;

  private MasterAPI master;
  private String nodeType;

  private Source source;

  private SourceSinkFactory<Source> sourceFactory;
  private SourceSinkFactory<Sink> sinkFactory;

  public static Node getInstance() {
    return instance;
  }

  public static void main(String[] args) {

    NodeConfig conf = new NodeConfigurator().configure(args);

    try {
      instance = new Node(conf);
    } catch (IOException e) {
      System.err.println("Error while starting node: " + e.getMessage());
      e.printStackTrace();
      System.exit(2);
    }

    instance.start();
  }

  public Node(NodeConfig config) throws IOException {

    setName("Node " + getId());

    this.setNodeType(null); // TODO set node type

    this.conf = config;
    configure();

    // connectToMaster();
  }

  /**
   * Load node configuration and start source/sink. In the future this could be from a Master
   * server, but for now we simply use a properties file.
   *
   * @throws IOException
   */
  public void configure() throws IOException {

    this.sourceFactory = new SourceSinkFactory<Source>(conf.get(NodeConfig.SOURCE), SourceRegistry.getRegistry());
    this.sinkFactory = new SourceSinkFactory<Sink>(conf.get(NodeConfig.SINK), SinkRegistry.getRegistry());

    this.source = sourceFactory.build();
    this.source.setSinkFactory(sinkFactory);

  }

  private void connectToMaster() {

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
      LOG.error("Startup failed! Bailing out");
      System.exit(1);
    }

  }

  @Override
  public void run() {

    try {
      this.source.open();
    } catch (IOException e) {
      LOG.error("Failed to open source: " + e.getMessage(), e);
      System.exit(1);

    }

  }

  @Override
  public void onConnect() {
    if (!master.handshake(nodeType)) {
      LOG.error("HANDSHAKE FAILED");
      System.exit(1);
    }
    LOG.debug("Handshake succeeded");
  }

  @Override
  public void onDisconnect() {
    // nothing for now
   LOG.debug("onDisconnect() raised");
  }


  // GETTERS & SETTERS

  public void setNodeType(String nodeType) {
    this.nodeType = nodeType;
  }

  public String getNodeType() {
    return nodeType;
  }

  public Source getSource() {
    return source;
  }

  public SourceSinkFactory<Sink> getSinkFactory() {
    return sinkFactory;
  }

  public NodeConfig getConf() {
    return conf;
  }

}


