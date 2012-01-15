package net.pixelcop.sewer.source;

import java.io.IOException;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import net.pixelcop.sewer.Event;
import net.pixelcop.sewer.Sink;
import net.pixelcop.sewer.Source;

import org.apache.commons.lang3.math.NumberUtils;
import org.eclipse.jetty.server.Connector;
import org.eclipse.jetty.server.Handler;
import org.eclipse.jetty.server.Request;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.handler.AbstractHandler;
import org.eclipse.jetty.server.nio.SelectChannelConnector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Source that generates events from HTTP and responds with a "204 No Content" status
 *
 * @author chetan
 *
 */
public class HttpPixelSource extends Source {

  /**
   * Simple handler that always returns HTTP/1.1 200 and does <em>not</em> log the request
   *
   * @author chetan
   *
   */
  class StatusHandler extends AbstractHandler {
    @Override
    public void handle(String target, Request baseRequest, HttpServletRequest request,
        HttpServletResponse response) throws IOException, ServletException {

      response.setStatus(HttpServletResponse.SC_OK);
      baseRequest.setHandled(true);
    }
  }

  /**
   * Simple handler that always returns HTTP/1.1 204 and logs the request
   * @author chetan
   *
   */
  class PixelHandler extends AbstractHandler {
    @Override
    public void handle(String target, Request baseRequest, HttpServletRequest request,
        HttpServletResponse response) throws IOException, ServletException {

      // handle response
      response.setStatus(HttpServletResponse.SC_NO_CONTENT);
      baseRequest.setHandled(true);

      // log request
      Event event = AccessLogExtractor.extractAccessLogEvent(baseRequest);
      sink.append(event);
    }
  }

  private static final Logger LOG = LoggerFactory.getLogger(HttpPixelSource.class);

  private static final int DEFAULT_HTTP_PORT = 8080;

  private final int port;
  private Server server;
  private Server statusServer;

  private Sink sink;

  public HttpPixelSource(String[] args) {
    if (args == null) {
      this.port = DEFAULT_HTTP_PORT;
    } else {
      this.port = NumberUtils.toInt(args[0], DEFAULT_HTTP_PORT);
    }

    initServer();
  }

  private void initServer() {
    this.server = createServer(port, createConnector(port, true), new PixelHandler());
    this.statusServer = createServer(port, createConnector(port+1, false), new StatusHandler());
  }

  private Server createServer(int port, Connector conn, Handler handler) {
    Server server = new Server(port);

    server.setGracefulShutdown(1000);
    server.setStopAtShutdown(false);
    server.setSendServerVersion(false);
    server.setSendDateHeader(false);

    server.setConnectors(new Connector[]{ conn });
    server.setHandler(handler);

    return server;
  }

  private Connector createConnector(int port, boolean checkForwardHeaders) {
    SelectChannelConnector conn = new SelectChannelConnector();

    conn.setPort(port);
    // conn.setAcceptors(4); // default seems good enough
    conn.setAcceptQueueSize(100);
    conn.setReuseAddress(true);
    conn.setSoLingerTime(1000);
    conn.setResolveNames(false);

    if (checkForwardHeaders) {
      conn.setForwarded(true);
    }

    return conn;
  }

  @Override
  public void close() throws IOException {

    if (getStatus() == CLOSED) {
      return;
    }

    setStatus(CLOSING);
    LOG.info("Closing " + this.getClass().getSimpleName());
    try {
      LOG.debug("stopping server gracefully");
      this.server.stop();
      LOG.debug("server stopped");
    } catch (Exception e) {
      LOG.warn("Exception while stopping server: " + e.getMessage(), e);
    }
    try {
      LOG.debug("waiting for server thread to join");
      this.server.join();
      LOG.debug("server thread has joined");
    } catch (InterruptedException e) {
      LOG.error("Interrupted waiting for server thread to join", e);
    }

    try {
      sink.close();
    } catch (IOException e) {
      LOG.error("Exception closing sink: " + e.getMessage(), e);
    }
    setStatus(CLOSED);
  }

  @Override
  public void open() throws IOException {

    setStatus(OPENING);

    this.sink = createSink();

    if (LOG.isInfoEnabled()) {
      LOG.info("Opening " + this.getClass().getSimpleName() + " on port " + port + " (status on "
          + (port + 1) + ")");
    }

    try {
      this.server.start();
      this.statusServer.start();
    } catch (Exception e) {
      throw new IOException("Failed to start server: " + e.getMessage(), e);
    }

    setStatus(FLOWING);

  }

  @Override
  public Class<?> getEventClass() {
    return AccessLogEvent.class;
  }

  public int getPort() {
    return port;
  }

}
