package net.pixelcop.sewer.source.http;

import net.pixelcop.sewer.Event;

import org.eclipse.jetty.http.HttpHeaders;
import org.eclipse.jetty.server.Request;

public class AccessLogExtractor {

  public static Event extractAccessLogEvent(Request req) {

    String ip = req.getHeader(HttpHeaders.X_FORWARDED_FOR);
    if (ip == null) {
        ip = req.getRemoteAddr();
    }

    return new AccessLogEvent(
        System.nanoTime(),
        ip,
        req.getServerName(),
        req.getRequestURI(),
        req.getQueryString(),
        req.getHeader(HttpHeaders.REFERER),
        req.getHeader(HttpHeaders.USER_AGENT),
        req.getHeader(HttpHeaders.COOKIE));
  }

}
