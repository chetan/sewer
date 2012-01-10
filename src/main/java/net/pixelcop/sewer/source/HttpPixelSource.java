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
    this.server = new Server(port);

    this.server.setGracefulShutdown(1000);
    this.server.setStopAtShutdown(false);
    this.server.setSendServerVersion(false);
    this.server.setSendDateHeader(false);

    SelectChannelConnector conn = new SelectChannelConnector();

    conn.setPort(port);
    // conn.setAcceptors(4);
    conn.setAcceptQueueSize(100);
    conn.setReuseAddress(true);
    conn.setSoLingerTime(1000);

    this.server.setConnectors(new Connector[]{ conn });



    // Create pixel handler & logger (event generator)
//    RequestLogHandler requestLogHandler = new RequestLogHandler();
//    requestLogHandler.setHandler(new PixelHandler());
//    requestLogHandler.setRequestLog(new NCSARequestLog());
//    this.server.setHandler(requestLogHandler);
    this.server.setHandler(new PixelHandler());
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
      LOG.info("Opening " + this.getClass().getSimpleName() + " on port " + port);
    }

    try {
      this.server.start();
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
