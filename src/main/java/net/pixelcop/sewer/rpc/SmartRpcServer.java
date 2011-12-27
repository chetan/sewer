package net.pixelcop.sewer.rpc;

import java.io.IOException;
import java.lang.reflect.Method;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SmartRpcServer extends Thread {

  private static final Logger LOG = LoggerFactory.getLogger(SmartRpcServer.class);

  private Map<String, MethodHandler> handlers;

  public static final int DEFAULT_PORT = 9595;
  private ServerSocket server;
  private List<SmartRpcClient> clients;

  public SmartRpcServer() throws IOException {
    this(DEFAULT_PORT);
  }

  public SmartRpcServer(int port) throws IOException {
    super();
    server = new ServerSocket(port);
    server.setReuseAddress(true);
    server.setReceiveBufferSize(64*1024);
    clients = new ArrayList<SmartRpcClient>();
    handlers = new HashMap<String, MethodHandler>();
    setName("Smart RPC Server " + getId());
  }

  @Override
  public void run() {

    try {
      Socket clientSocket = null;
      while ((clientSocket = server.accept()) != null) {
        SmartRpcClient client = new SmartRpcClient(clientSocket);
        client.setHandlers(handlers);
        clients.add(client);
        client.start();
      }
    } catch (IOException e) {

    }

  }

  /**
   *
   * @param clazz API interface class
   * @param handler handler instance
   */
  public void registerAPIHandler(Class<?> clazz, Object handler) {

    Method[] methods = clazz.getMethods();
    for (int i = 0; i < methods.length; i++) {
      Method method = methods[i];
      handlers.put(method.getName(), new MethodHandler(handler, method));
    }

  }



}
