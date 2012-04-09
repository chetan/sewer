package net.pixelcop.sewer.source.http;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.lang.management.ManagementFactory;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import net.pixelcop.sewer.Event;
import net.pixelcop.sewer.Sink;
import net.pixelcop.sewer.Source;
import net.pixelcop.sewer.node.Node;

import org.apache.commons.lang3.math.NumberUtils;
import org.eclipse.jetty.http.HttpFields;
import org.eclipse.jetty.http.HttpHeaders;
import org.eclipse.jetty.http.HttpStatus;
import org.eclipse.jetty.io.Buffer;
import org.eclipse.jetty.io.ByteArrayBuffer;
import org.eclipse.jetty.io.WriterOutputStream;
import org.eclipse.jetty.jmx.MBeanContainer;
import org.eclipse.jetty.server.AbstractHttpConnection;
import org.eclipse.jetty.server.Connector;
import org.eclipse.jetty.server.Handler;
import org.eclipse.jetty.server.Request;
import org.eclipse.jetty.server.Response;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.handler.AbstractHandler;
import org.eclipse.jetty.server.handler.StatisticsHandler;
import org.eclipse.jetty.server.nio.SelectChannelConnector;
import org.eclipse.jetty.util.IO;
import org.eclipse.jetty.util.log.Log;
import org.eclipse.jetty.util.resource.Resource;
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

    private final Buffer crossdomainBuffer;
    private final long crossdomainLength;
    private final long crossdomainLastModified;

    private final Buffer gzCrossdomainBuffer;
    private final long gzCrossdomainLength;

    private final AccessLogExtractor accessLogExtractor;

    public PixelHandler(AccessLogExtractor extractor) throws IOException {
      this.accessLogExtractor = extractor;

      Resource crossdomainResource = Resource.newSystemResource("crossdomain.xml");
      crossdomainBuffer = new ByteArrayBuffer(IO.readBytes(crossdomainResource.getInputStream()));
      crossdomainLength = crossdomainBuffer.length();
      crossdomainLastModified = crossdomainResource.lastModified();

      Resource gz = Resource.newSystemResource("crossdomain.xml.gz");
      gzCrossdomainBuffer = new ByteArrayBuffer(IO.readBytes(gz.getInputStream()));
      gzCrossdomainLength = gzCrossdomainBuffer.length();
    }

    @Override
    public void handle(String target, Request baseRequest, HttpServletRequest request,
        HttpServletResponse response) throws IOException, ServletException {

      if (handleCrossDomainXml(baseRequest, request, response)) {
        return; // no logging needed
      }

      // handle response (204)
      response.setStatus(HttpServletResponse.SC_NO_CONTENT);
      baseRequest.setHandled(true);

      // log request
      Event event = accessLogExtractor.extract(baseRequest);
      sink.append(event);
    }

    /**
     * Serve the crossdomain.xml file
     *
     * @param baseRequest
     * @param request
     * @param response
     * @return boolean True if request was for crossdomain.xml
     * @throws IOException
     */
    protected boolean handleCrossDomainXml(Request baseRequest, HttpServletRequest request,
        HttpServletResponse response) throws IOException {

      if (request.getPathInfo().intern() != CROSSDOMAIN) {
        return false;
      }

      baseRequest.setHandled(true);

      if (request.getDateHeader(HttpHeaders.IF_MODIFIED_SINCE) > 0) {
        // always respond that it hasn't been modified
        response.setStatus(HttpStatus.NOT_MODIFIED_304);
        return true;
      }


      Buffer buff = null;
      long length = 0;

      // check gzip support
      String accept = request.getHeader(HttpHeaders.ACCEPT_ENCODING);
      if (accept != null && accept.indexOf(GZIP) >= 0) {
        response.setHeader(HttpHeaders.CONTENT_ENCODING, GZIP);
        buff = gzCrossdomainBuffer;
        length = gzCrossdomainLength;

      } else {
        buff = crossdomainBuffer;
        length = crossdomainLength;
      }


      // set headers
      response.setContentType(APPXML);
      HttpFields fields = ((Response) response).getHttpFields();
      fields.putLongField(HttpHeaders.CONTENT_LENGTH_BUFFER, length);
      fields.put(HttpHeaders.CACHE_CONTROL_BUFFER, CACHECONTROL);
      response.setDateHeader(HttpHeaders.LAST_MODIFIED, crossdomainLastModified);

      // send response
      OutputStream out = null;
      try {
        out = response.getOutputStream();
      } catch (IllegalStateException e) {
        out = new WriterOutputStream(response.getWriter());
      }

      ((AbstractHttpConnection.Output) out).sendContent(new ByteArrayInputStream(buff.array()));
      return true;
    }

  }

  private static final Logger LOG = LoggerFactory.getLogger(HttpPixelSource.class);

  private static final String CROSSDOMAIN = "/crossdomain.xml".intern();
  private static final String APPXML = "application/xml";
  private static final String CACHECONTROL = "max-age=315360000, public";
  private static final String GZIP = "gzip";

  private static final String CONFIG_EXTRACTOR = "sewer.source.pixel.extractor";
  private static final String CONFIG_GRACEFUL = "sewer.source.pixel.graceful";
  private static final String CONFIG_ACCEPT_QUEUE = "sewer.source.pixel.accept_queue";
  private static final String CONFIG_MAX_IDLE = "sewer.source.pixel.max_idle";
  private static final String CONFIG_LOW_RESOURCE_CONNS = "sewer.source.pixel.low_resource_conns";
  private static final String CONFIG_LOW_RESOURCE_MAX_IDLE = "sewer.source.pixel.low_resource_max_idle";
  private static final String CONFIG_STATUS_PORT = "sewer.source.pixel.status.port";

  private static final int DEFAULT_HTTP_PORT = 8080;

  private final int port;
  private Server server;
  private Server statusServer;

  private Sink sink;

  @SuppressWarnings("rawtypes")
  private Class eventClass;

  public HttpPixelSource(String[] args) {
    if (args == null) {
      this.port = DEFAULT_HTTP_PORT;
    } else {
      this.port = NumberUtils.toInt(args[0], DEFAULT_HTTP_PORT);
    }
  }

  @Override
  public void init() throws IOException {
    super.init();
    initServer();
  }

  private AccessLogExtractor createExtractor() throws IOException {
    try {
      Class<? extends AccessLogExtractor> clazz = Node.getInstance().getConf().getClass(
          CONFIG_EXTRACTOR, DefaultAccessLogExtractor.class, AccessLogExtractor.class);

      AccessLogExtractor obj = clazz.newInstance();
      this.eventClass = obj.getEventClass();
      return obj;

    } catch (Throwable t) {
      throw new IOException("Unable to load custom extractor: "
          + Node.getInstance().getConf().get(CONFIG_EXTRACTOR), t);
    }
  }

  /**
   * Creates the server but does not start it
   *
   * @throws IOException
   */
  private void initServer() throws IOException {
    StatisticsHandler handler = new StatisticsHandler();
    handler.setHandler(new PixelHandler(createExtractor()));
    this.server = createServer(createConnector(getPort(), true), handler);
    this.statusServer = createServer(createConnector(getStatusPort(), false), new StatusHandler());
  }

  private Server createServer(Connector conn, Handler handler) {
    Server server = new Server();

    if (handler instanceof StatisticsHandler) {
      // Setup JMX
      MBeanContainer mbContainer = new MBeanContainer(ManagementFactory.getPlatformMBeanServer());
      server.getContainer().addEventListener(mbContainer);
      server.addBean(mbContainer);
      mbContainer.addBean(Log.getRootLogger());
    }

    server.setGracefulShutdown(Node.getInstance().getConf().getInt(CONFIG_GRACEFUL, 1000));
    server.setStopAtShutdown(false);
    server.setSendServerVersion(false);
    server.setSendDateHeader(false);

    server.setConnectors(new Connector[]{ conn });
    server.setHandler(handler);

    return server;
  }

  private Connector createConnector(int port, boolean checkForwardHeaders) throws IOException {
    SelectChannelConnector conn = new SelectChannelConnector();

    conn.setPort(port);
    // conn.setAcceptors(4); // default seems good enough
    conn.setAcceptQueueSize(Node.getInstance().getConf().getInt(CONFIG_ACCEPT_QUEUE, 100));

    // Set limits on idle keep-alive connections
    conn.setMaxIdleTime(
        Node.getInstance().getConf().getInt(CONFIG_MAX_IDLE, 30000));
    conn.setLowResourcesConnections(
        Node.getInstance().getConf().getInt(CONFIG_LOW_RESOURCE_CONNS, 500));
    conn.setLowResourcesMaxIdleTime(
        Node.getInstance().getConf().getInt(CONFIG_LOW_RESOURCE_MAX_IDLE, 5000));

    conn.setReuseAddress(true);
    // conn.setSoLingerTime(1000);
    conn.setResolveNames(false);

    if (checkForwardHeaders) {
      conn.setForwarded(true);
    }

    // binds to port, but does not accept() connections
    conn.open();

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
      this.statusServer.stop();
      LOG.debug("server stopped");
    } catch (Exception e) {
      LOG.warn("Exception while stopping server: " + e.getMessage(), e);
    }
    try {
      LOG.debug("waiting for server thread to join");
      this.server.join();
      this.statusServer.join();
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
          + getStatusPort() + ")");
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
    if (eventClass == null) {
      try {
        createExtractor();
      } catch (IOException e) {
        throw new RuntimeException("Error getting event class", e);
      }
    }
    return eventClass;
  }

  public int getPort() {
    return port;
  }

  /**
   * Return the configured status port. Defaults to {@link #getPort()} + 1
   *
   * @return
   */
  public int getStatusPort() {
    return Node.getInstance().getConf().getInt(CONFIG_STATUS_PORT, port + 1);
  }

}
