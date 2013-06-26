package net.pixelcop.sewer.util;

import java.util.concurrent.ArrayBlockingQueue;

import net.pixelcop.sewer.Event;
import net.pixelcop.sewer.Sink;

/**
 * Helper for adding batch append capabilities to a {@link Sink}
 *
 * @author chetan
 *
 */
public class BatchHelper {

  private final ArrayBlockingQueue<Event> batch;

  public BatchHelper(int batchSize) {
    batch = new ArrayBlockingQueue<Event>(batchSize);
  }

  /**
   * Add the given event to the queue, if there is room.
   *
   * @param event
   * @return true if room was available, else false
   */
  public boolean append(Event event) {
    return batch.offer(event);
  }

  public boolean isEmpty() {
    return batch.isEmpty();
  }

  /**
   * Retrieves the current batch as an array and clears the queue
   *
   * @return
   */
  public Event[] getBatch() {
    Event[] b = (Event[]) batch.toArray();
    batch.clear();
    return b;
  }

}
