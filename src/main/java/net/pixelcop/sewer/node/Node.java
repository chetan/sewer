package net.pixelcop.sewer.node;

import java.io.IOException;
import java.lang.reflect.Constructor;

import net.pixelcop.sewer.PlumbingFactory;
import net.pixelcop.sewer.PlumbingProvider;
import net.pixelcop.sewer.Sink;
import net.pixelcop.sewer.SinkRegistry;
import net.pixelcop.sewer.Source;
import net.pixelcop.sewer.SourceRegistry;
import net.pixelcop.sewer.rpc.MasterAPI;
import net.pixelcop.sewer.rpc.SmartRpcClient;
import net.pixelcop.sewer.rpc.SmartRpcClientEventHandler;
import net.pixelcop.sewer.rpc.SmartRpcServer;
import net.pixelcop.sewer.sink.durable.TransactionManager;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Node extends Thread implements SmartRpcClientEventHandler {

  static class ShutdownHook extends Thread {
    @Override
    public void run() {
      LOG.warn("Caught shutdown signal. Going to try to stop cleanly..");
      try {
        Node.getInstance().getSource().close();
      } catch (IOException e) {
        LOG.error("Source failed to close cleanly: " + e.getMessage(), e);
        return;
      }
      LOG.debug("Shutdown complete. Goodbye!");
    }
  }

  private static final Logger LOG = LoggerFactory.getLogger(Node.class);

  protected static Node instance;

  private final NodeConfig conf;

  private MasterAPI master;
  private String nodeType;

  private Source source;

  private PlumbingFactory<Source> sourceFactory;
  private PlumbingFactory<Sink> sinkFactory;

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
      LOG.error("Error while starting node: " + e.getMessage(), e);
      System.exit(ExitCodes.IO_ERROR);
    }

  }

  public Node(NodeConfig config) throws IOException {

    instance = this;

    setName("Node " + getId());
    this.setNodeType(null); // TODO set node type

    this.conf = config;
    configure();

    // Boot TransactionManager
    TransactionManager.init(conf);

    // connectToMaster();
  }

  /**
   * Load node configuration and start source/sink. In the future this could be from a Master
   * server, but for now we simply use a properties file.
   *
   * @throws IOException
   */
  protected void configure() throws IOException {

    validateConfig();

    loadPlugins();

    try {
      this.sourceFactory = new PlumbingFactory<Source>(conf.get(NodeConfig.SOURCE), SourceRegistry.getRegistry());
      this.sinkFactory = new PlumbingFactory<Sink>(conf.get(NodeConfig.SINK), SinkRegistry.getRegistry());

    } catch (ConfigurationException ex) {
      LOG.error("Node configuration failed: " + ex.getMessage(), ex);
      System.exit(ExitCodes.CONFIG_ERROR);
    }

    this.source = sourceFactory.build();
    this.source.setSinkFactory(sinkFactory);

    this.source.init();

  }

  /**
   * Load plugin classes specified in config
   */
  @SuppressWarnings("unchecked")
  protected void loadPlugins() {
    String[] plugins = conf.getStrings(NodeConfig.PLUGINS);
    if (plugins == null || plugins.length == 0) {
      return;
    }
    for (int i = 0; i < plugins.length; i++) {
      try {
        Constructor con = Class.forName(plugins[i]).getConstructor(new String[]{}.getClass());
        Object obj = con.newInstance(new Object[] { new String[]{} });
        ((PlumbingProvider) obj).register();

      } catch (Throwable t) {
        LOG.error("Failed to load plugin class: " + plugins[i], t);
        System.exit(ExitCodes.CONFIG_ERROR);
      }
    }
  }

  protected void validateConfig() {

    boolean err = false;
    if (conf.get(NodeConfig.SOURCE) == null) {
      err = true;
      LOG.error("Property " + NodeConfig.SOURCE + " cannot be null");
    }
    if (conf.get(NodeConfig.SINK) == null) {
      err = true;
      LOG.error("Property " + NodeConfig.SINK + " cannot be null");
    }

    if (err == true) {
      System.exit(ExitCodes.CONFIG_ERROR);
    }

  }

  @SuppressWarnings("unused")
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
      LOG.error("Startup failed! Bailing out");
      System.exit(ExitCodes.OTHER_ERROR);
    }

  }

  @Override
  public void run() {

    try {
      LOG.debug("Opening source");
      this.source.open();
    } catch (IOException e) {
      LOG.error("Failed to open source: " + e.getMessage(), e);
      System.exit(ExitCodes.IO_ERROR);

    }

  }

  @Override
  public void onConnect() {
    if (!master.handshake(nodeType)) {
      LOG.error("HANDSHAKE FAILED");
      System.exit(ExitCodes.OTHER_ERROR);
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

  public PlumbingFactory<Sink> getSinkFactory() {
    return sinkFactory;
  }

  public NodeConfig getConf() {
    return conf;
  }

}
