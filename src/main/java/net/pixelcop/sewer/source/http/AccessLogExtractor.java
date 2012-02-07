package net.pixelcop.sewer.source.http;

import net.pixelcop.sewer.Event;

import org.eclipse.jetty.server.Request;

/**
 * Generic interface for extracting {@link Event}s from a {@link Request}
 * @author chetan
 *
 */
public interface AccessLogExtractor {

  public Event extract(Request req);

}
