package net.pixelcop.sewer.sink.buffer;

import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import net.pixelcop.sewer.Event;
import net.pixelcop.sewer.Sink;
import net.pixelcop.sewer.node.Node;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.lmax.disruptor.BlockingWaitStrategy;
import com.lmax.disruptor.EventFactory;
import com.lmax.disruptor.EventHandler;
import com.lmax.disruptor.MultiThreadedClaimStrategy;
import com.lmax.disruptor.dsl.Disruptor;

public class DisruptorSink extends Sink {

  class SewerEventHandler implements EventHandler<DelegateEvent> {
    @Override
    public void onEvent(DelegateEvent event, long sequence, boolean endOfBatch) throws Exception {
      subSink.append(event.getDelegate());
    }
  }

  public static final String CONF_THREADS = "sewer.sink.disruptor.threads";

  private static final Logger LOG = LoggerFactory.getLogger(DisruptorSink.class);

  protected Disruptor<DelegateEvent> disruptor;

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
    try {
      DelegateEvent devent = disruptor.getRingBuffer().get(sequence); // get the placeholder object
      devent.setDelegate(event);
    } finally {
      disruptor.getRingBuffer().publish(sequence); // tell the buffer to publish
    }
  }

  @SuppressWarnings("unchecked")
  @Override
  public void open() throws IOException {

    LOG.debug("opening");
    setStatus(OPENING);
    createSubSink();

    final ExecutorService executor = Executors.newFixedThreadPool(
        Node.getInstance().getConf().getInt(CONF_THREADS, 1));

    disruptor =
      new Disruptor<DelegateEvent>(createEventFactory(), executor,
          new MultiThreadedClaimStrategy(Double.valueOf(Math.pow(2, 17)).intValue()), // 2^17 = 131,072
          new BlockingWaitStrategy()
      );

    disruptor.handleEventsWith(new SewerEventHandler());
    disruptor.start();

    subSink.open();
    setStatus(FLOWING);
    LOG.debug("flowing");
  }

  private EventFactory<DelegateEvent> createEventFactory() {
    return new EventFactory<DelegateEvent>() {
      public DelegateEvent newInstance() {
        return new DelegateEvent();
      }
    };
  }

}
