package net.pixelcop.sewer.sink.buffer;

import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import net.pixelcop.sewer.Event;
import net.pixelcop.sewer.Sink;
import net.pixelcop.sewer.node.Node;
import net.pixelcop.sewer.source.http.AccessLogEvent;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.lmax.disruptor.BlockingWaitStrategy;
import com.lmax.disruptor.EventFactory;
import com.lmax.disruptor.EventHandler;
import com.lmax.disruptor.MultiThreadedClaimStrategy;
import com.lmax.disruptor.dsl.Disruptor;

public class DisruptorSink extends Sink {

  class SewerEventHandler implements EventHandler<Event> {
    @Override
    public void onEvent(Event event, long sequence, boolean endOfBatch) throws Exception {
      subSink.append(event);
    }
  }

  private static final Logger LOG = LoggerFactory.getLogger(DisruptorSink.class);

  protected Disruptor<Event> disruptor;

  public DisruptorSink(String[] args) {
  }

  @Override
  public void close() throws IOException {

    setStatus(CLOSING);
    disruptor.shutdown(); // blocks until buffer is clear
    subSink.close();
    setStatus(CLOSED);

  }

  @Override
  public void append(Event event) throws IOException {

    long sequence = disruptor.getRingBuffer().next(); // get next avail seq
    AccessLogEvent devent = (AccessLogEvent) disruptor.getRingBuffer().get(sequence); // get the placeholder object

    // copy from event into devent??
    // TODO temp hardcoded copy
    devent.copyFrom(event);

    disruptor.getRingBuffer().publish(sequence); // tell the buffer to publish
  }

  @SuppressWarnings("unchecked")
  @Override
  public void open() throws IOException {

    LOG.debug("opening");

    setStatus(OPENING);
    createSubSink();

    final ExecutorService executor = Executors.newFixedThreadPool(4);

    //  disruptor =
    //    new Disruptor<DisruptorEvent>(createEventFactory(), 131072, executor); // 2^17 = 131,072

    disruptor =
      new Disruptor<Event>(createEventFactory(), executor,
          new MultiThreadedClaimStrategy(2^17+1), // 2^17 = 131,072
          new BlockingWaitStrategy()
      );

    disruptor.handleEventsWith(new SewerEventHandler());
    disruptor.start();

    subSink.open();
    setStatus(FLOWING);
    LOG.debug("flowing");
  }

  private EventFactory<Event> createEventFactory() {
    return new EventFactory<Event>() {
      public Event newInstance() {
        try {
          return (Event) Node.getInstance().getSource().getEventClass().newInstance();
        } catch (Exception e) {
          LOG.error("Error creating new event", e);
        }
        return null;
      }
    };
  }

}
