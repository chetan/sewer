package net.pixelcop.sewer.rpc;

import java.lang.reflect.Method;

public class MethodHandler {

  public Method method;
  public Object handler;

  public MethodHandler(Object handler, Method method) {
    this.handler = handler;
    this.method = method;
  }

}
