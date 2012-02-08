package net.pixelcop.sewer.source.http;

import net.pixelcop.sewer.Event;

import org.eclipse.jetty.server.Request;

/**
 * Generic interface for extracting {@link Event}s from a {@link Request}
 * @author chetan
 *
 */
public interface AccessLogExtractor {

  /**
   * Build {@link Event} from the {@link Request}
   *
   * @param req
   * @return
   */
  public Event extract(Request req);

  /**
   * Get the class of the {@link Event}s created by this Extractor
   *
   * @return
   */
  public Class<?> getEventClass();

}
