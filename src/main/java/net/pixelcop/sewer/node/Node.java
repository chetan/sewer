package net.pixelcop.sewer.node;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import net.pixelcop.sewer.PlumbingFactory;
import net.pixelcop.sewer.PlumbingProvider;
import net.pixelcop.sewer.Sink;
import net.pixelcop.sewer.SinkRegistry;
import net.pixelcop.sewer.Source;
import net.pixelcop.sewer.SourceRegistry;
import net.pixelcop.sewer.sink.debug.GraphiteConsole;
import net.pixelcop.sewer.sink.durable.TransactionManager;
import net.pixelcop.sewer.util.NetworkUtil;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.ScheduledReporter;
import com.codahale.metrics.graphite.Graphite;
import com.codahale.metrics.graphite.GraphiteReporter;

/**
 * Primary Sewer entry point. The node is responsible for configuring a source/sink pair. It
 * currently operates in a 100% standalone manner, reading its configuration from a local file.
 *
 * Future master/child support was planned but never completed.
 *
 * @author chetan
 *
 */
public class Node extends Thread {

  static class ShutdownHook extends Thread {
    @Override
    public void run() {
      LOG.warn("Caught shutdown signal. Going to try to stop cleanly..");
      Node.getInstance().shutdown();
      LOG.debug("Shutdown complete. Goodbye!");
    }
  }

  private static final Logger LOG = LoggerFactory.getLogger(Node.class);

  protected static Node instance;

  private final NodeConfig conf;

  private MetricRegistry metricRegistry;
  private List<ScheduledReporter> metricReporters;

  private Source source;

  private PlumbingFactory<Source> sourceFactory;
  private PlumbingFactory<Sink> sinkFactory;

  public static Node getInstance() {
    return instance;
  }

  /**
   * This main() method is not called directly, rather it is called via {@link NodeDaemon}.
   *
   * @param args
   */
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
    this.conf = config;
    configure();

    TransactionManager.init(conf);
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
  @SuppressWarnings({ "rawtypes" })
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

  /**
   * Setup {@link GraphiteReporter} if enabled in config
   * @throws IOException
   */
  private void configureMetrics() throws IOException {

    this.metricRegistry = new MetricRegistry();
    this.metricReporters = new ArrayList<ScheduledReporter>();

    String prefix = conf.get(NodeConfig.GRAPHITE_PREFIX, "") +
        NetworkUtil.getLocalhost().replace('.', '_');

    String host = conf.get(NodeConfig.GRAPHITE_HOST);
    if (host != null) {
      LOG.info("Enabling graphite metrics");

      int port = conf.getInt(NodeConfig.GRAPHITE_PORT, 2003);
      Graphite graphite = new Graphite(new InetSocketAddress(host, port));
      addMetricReporter(GraphiteReporter.forRegistry(metricRegistry)
          .prefixedWith(prefix)
          .convertRatesTo(TimeUnit.SECONDS)
          .convertDurationsTo(TimeUnit.MILLISECONDS)
          .build(graphite)).start(1, TimeUnit.MINUTES);
    }

    if (LOG.isDebugEnabled()) {
      LOG.debug("Enabling console metrics since DEBUG level enabled");
      Graphite graphite = new GraphiteConsole();
      addMetricReporter(GraphiteReporter.forRegistry(metricRegistry)
          .prefixedWith(prefix)
          .convertRatesTo(TimeUnit.SECONDS)
          .convertDurationsTo(TimeUnit.MILLISECONDS)
          .build(graphite)).start(1, TimeUnit.MINUTES);
    }

  }

  /**
   * Cleanup any configured metrics
   */
  private void cleanupMetrics() {
    if (metricReporters == null) {
      return;
    }
    for (ScheduledReporter reporter : metricReporters) {
      reporter.stop();
    }
  }

  /**
   * Start the node. Open source and enable metric reporting
   */
  @Override
  public void run() {

    try {
      configureMetrics();

      LOG.debug("Opening source");
      this.source.open();
    } catch (IOException e) {
      LOG.error("Failed to open source: " + e.getMessage(), e);
      System.exit(ExitCodes.IO_ERROR);

    }

  }

  /**
   * Stop the node. Close the source and any sinks and disable metric reporting
   */
  public void shutdown() {
    if (source != null) {
      try {
        source.close();
      } catch (IOException e) {
        LOG.error("Source failed to close cleanly: " + e.getMessage(), e);
      }
    }
    cleanupMetrics();
  }


  // GETTERS & SETTERS

  public Source getSource() {
    return source;
  }

  public PlumbingFactory<Sink> getSinkFactory() {
    return sinkFactory;
  }

  public NodeConfig getConf() {
    return conf;
  }

  public MetricRegistry getMetricRegistry() {
    return metricRegistry;
  }

  public List<ScheduledReporter> getMetricReporters() {
    return metricReporters;
  }

  public ScheduledReporter addMetricReporter(ScheduledReporter reporter) {
    this.metricReporters.add(reporter);
    return reporter;
  }

}
