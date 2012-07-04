package net.pixelcop.sewer.source.http;

import net.pixelcop.sewer.Event;

import org.eclipse.jetty.http.HttpHeaders;
import org.eclipse.jetty.server.Request;

public class DefaultAccessLogExtractor implements AccessLogExtractor {

  @Override
  public Event extract(Request req) {

    return new AccessLogEvent(
        System.nanoTime(),
        req.getRemoteAddr(),
        req.getServerName(),
        req.getRequestURI(),
        req.getQueryString(),
        req.getHeader(HttpHeaders.REFERER),
        req.getHeader(HttpHeaders.USER_AGENT),
        req.getHeader(HttpHeaders.COOKIE));
  }

  @Override
  public Class<?> getEventClass() {
    return AccessLogEvent.class;
  }

}
