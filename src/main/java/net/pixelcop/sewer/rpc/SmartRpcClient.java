package net.pixelcop.sewer.rpc;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;

import net.minidev.json.JSONArray;
import net.minidev.json.JSONValue;
import net.minidev.json.parser.JSONParser;
import net.minidev.json.parser.ParseException;
import net.pixelcop.sewer.node.Node;

import org.apache.hadoop.io.WritableUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SmartRpcClient extends Thread implements InvocationHandler {

  public static final int MSG_COMMAND = 1;
  public static final int MSG_RESPONSE = 2;

  private static final Logger LOG = LoggerFactory.getLogger(SmartRpcServer.class);

  private String host;
  private int port;

  private Socket socket;
  private DataOutputStream out;
  private DataInputStream in;

  private JSONParser parser;

  private Map<String, MethodHandler> handlers;

  private LinkedBlockingQueue<String> responseQueue;

  private SmartRpcClientEventHandler eventHandler;

  /**
   * This is used to construct proxy instances for API consumers
   *
   * @param clazz API class to proxy
   * @return
   * @throws IllegalArgumentException
   * @throws UnknownHostException
   * @throws IOException
   */
  public static Object createClient(Class<?> clazz, SmartRpcClient client) throws IOException {

    return Proxy.newProxyInstance(
        Node.class.getClassLoader(),
        new Class[]{ (Class<?>) clazz },
        client);
  }

  public SmartRpcClient(String host, int port) throws IOException {
    this.host = host;
    this.port = port;

    init();
  }

  public SmartRpcClient(Socket socket) throws IOException {
    this.socket = socket;
    init();
  }

  private void init() throws IOException {
    parser = new JSONParser(JSONParser.MODE_PERMISSIVE);
    responseQueue = new LinkedBlockingQueue<String>(); // TODO set a capacity limit?

    if (socket != null) {
      createStreams();
    }

    setName("Smart RPC Client " + getId());
  }

  private void createStreams() throws IOException {
    in = new DataInputStream(new BufferedInputStream(socket.getInputStream(), 64 * 1024));
    out = new DataOutputStream(new BufferedOutputStream(socket.getOutputStream(), 64 * 1024));
  }

  /**
   * Send the method call over the wire and return the answer
   */
  @Override
  public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {

    String command = method.getName();
    String json = JSONValue.toJSONString(args);

    out.writeInt(MSG_COMMAND);
    WritableUtils.writeString(out, command);
    WritableUtils.writeString(out, json);
    out.flush();

    // Read response back from the queue to avoid racing on input stream
    String retJson = responseQueue.take();

    if (LOG.isDebugEnabled()) {
      LOG.debug("method: " + method.getName());
      LOG.debug("args: " + args[0]);
      LOG.debug("reply: " + retJson);
    }

    return parser.parse(retJson);
  }

  @Override
  public void run() {

    // Handle server socket connection
    if (this.host == null) {
      readLoop();
      return;
    }

    // Client connections should try to reconnect on any failure
    while (true) {
      connectLoop();
      readLoop();
    }
  }

  /**
   * Continuously tries to connect to the server specified by host & port fields
   */
  private void connectLoop() {

    int failures = 0;

    while (true) {

      try {
        this.socket = new Socket(this.host, this.port);
        createStreams();
        if (this.eventHandler != null) {
          new Thread() {
            public void run() {
              eventHandler.onConnect();
            };
          }.start();
        }

        return; // we got a socket, all done here!

      } catch (Exception e) {
        failures++;
        if (LOG.isWarnEnabled()) {
          LOG.warn("Error connecting to server [" + host + ":" + port + "], failures = " + failures, e);
        }

        int backoff = 5000;
        if (failures > 30) {
          backoff = 10000;

        } else if (failures > 100) {
          backoff = 30000;
        }

        try {
          Thread.sleep(backoff);
        } catch (InterruptedException e1) {
          return;
        }

      }

    }

  }

  /**
   * Continuously tries to read from the socket connection
   */
  private void readLoop() {

    try {
      while (true) {
        read();
        out.flush();
      }

    } catch (EOFException e) {
      LOG.warn("Client closed..", e);

    } catch (IOException e) {
      LOG.warn("IO Exception in ClientThread", e);

    } catch (ParseException e) {
      // TODO Bad command/input
      LOG.warn("bad input", e);

    } catch (Exception e) {

    } finally {
      try {
        socket.close();
      } catch (IOException e) {
        LOG.info("Error closing socket during shutdown");
      }
      if (this.eventHandler != null) {
        this.eventHandler.onDisconnect();
      }
    }
  }

  private void read() throws IOException, ParseException {

    int type = in.readInt();
    if (type == MSG_RESPONSE) {
      responseQueue.add(WritableUtils.readString(in));

    } else if (type == MSG_COMMAND) {
      handleNextCommand();

    }
  }

  private void handleNextCommand() throws IOException, ParseException {

    String command = WritableUtils.readString(in);
    String json = WritableUtils.readString(in);
    JSONArray args = (JSONArray) parser.parse(json);

    if (LOG.isDebugEnabled()) {
      LOG.debug("COMMAND: " + command);
      LOG.debug("JSON: " + json);
      LOG.debug("ARGS: " + args);
    }

    try {
      Object ret = invoke(command, args);
      out.writeInt(MSG_RESPONSE);
      WritableUtils.writeString(out, JSONValue.toJSONString(ret));

    } catch (Exception e) {
      LOG.error("error executing command " + command, e);
    }
  }

  public Object invoke(String command, JSONArray args) throws IllegalArgumentException,
      IllegalAccessException, InvocationTargetException {

    MethodHandler rpc = getHandlers().get(command);
    Method method = rpc.method;
    return method.invoke(rpc.handler, args.toArray());
  }

  /**
   * Tests whether or not this client has an active connection
   * @return
   */
  public boolean isConnected() {
    return (socket != null && socket.isConnected());
  }


  public void setHandlers(Map<String, MethodHandler> handlers) {
    this.handlers = handlers;
  }

  public Map<String, MethodHandler> getHandlers() {
    return handlers;
  }

  public void setEventHandler(SmartRpcClientEventHandler eventHandler) {
    this.eventHandler = eventHandler;
  }

  public SmartRpcClientEventHandler getEventHandler() {
    return eventHandler;
  }

}
