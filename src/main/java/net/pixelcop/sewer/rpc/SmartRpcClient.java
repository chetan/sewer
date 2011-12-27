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

  private Socket socket;
  private DataOutputStream out;
  private DataInputStream in;

  private JSONParser parser;

  private Map<String, MethodHandler> handlers;

  private LinkedBlockingQueue<String> responseQueue;

  /**
   * This is used to construct proxy instances for API consumers
   *
   * @param clazz API class to proxy
   * @return
   * @throws IllegalArgumentException
   * @throws UnknownHostException
   * @throws IOException
   */
  public static Object createClient(Class<?> clazz) throws IllegalArgumentException, UnknownHostException, IOException {

    SmartRpcClient client = new SmartRpcClient();
    client.start();

    return Proxy.newProxyInstance(
        Node.class.getClassLoader(),
        new Class[]{ (Class<?>) clazz },
        client);
  }

  public SmartRpcClient() throws UnknownHostException, IOException {
    this("localhost", SmartRpcServer.DEFAULT_PORT);
  }

  public SmartRpcClient(String host, int port) throws UnknownHostException, IOException {

    socket = new Socket(host, port);
    if (!socket.isConnected()) {
      LOG.error("Unable to connect to server!! bailing");
      return;
    }

    init();
  }

  public SmartRpcClient(Socket socket) throws IOException {
    this.socket = socket;
    init();
  }

  private void init() throws IOException {
    parser = new JSONParser(JSONParser.MODE_PERMISSIVE);
    responseQueue = new LinkedBlockingQueue<String>(); // TODO set a capacity limit?

    in = new DataInputStream(new BufferedInputStream(socket.getInputStream(), 64 * 1024));
    out = new DataOutputStream(new BufferedOutputStream(socket.getOutputStream(), 64 * 1024));

    setName("Smart RPC Client " + getId());
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

    try {
      System.out.println("new customer, starting event loop");

      while (true) {
        read();
        out.flush();
      }

    } catch (EOFException e) {
      LOG.info("Client closed..");

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

  public void setHandlers(Map<String, MethodHandler> handlers) {
    this.handlers = handlers;
  }

  public Map<String, MethodHandler> getHandlers() {
    return handlers;
  }

}
