package net.pixelcop.sewer.node;

import net.pixelcop.sewer.node.Node.ShutdownHook;

import org.apache.commons.daemon.Daemon;
import org.apache.commons.daemon.DaemonContext;
import org.apache.commons.daemon.DaemonInitException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * {@link Daemon} implementation for starting & stopping Sewer service
 *
 * @author chetan
 *
 */
public class NodeDaemon implements Daemon {

  private static final Logger LOG = LoggerFactory.getLogger(NodeDaemon.class);

  /**
   * For starting without {@link Daemon} interface (jsvc not avail)
   * @param args
   */
  public static void main(String[] args) {
    Node.main(args);
    Node.getInstance().start();
    Runtime.getRuntime().addShutdownHook(new ShutdownHook());
  }

  @Override
  public void init(DaemonContext context) throws DaemonInitException, Exception {
    Node.main(context.getArguments());
  }

  @Override
  public void start() throws Exception {
    Node.getInstance().start();
  }

  @Override
  public void stop() throws Exception {
    new Node.ShutdownHook().run();
  }

  @Override
  public void destroy() {
  }

}
