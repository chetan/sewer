package net.pixelcop.sewer.sink.buffer;

import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import net.pixelcop.sewer.Event;
import net.pixelcop.sewer.Sink;
import net.pixelcop.sewer.node.Node;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.lmax.disruptor.BlockingWaitStrategy;
import com.lmax.disruptor.BusySpinWaitStrategy;
import com.lmax.disruptor.EventFactory;
import com.lmax.disruptor.EventHandler;
import com.lmax.disruptor.SleepingWaitStrategy;
import com.lmax.disruptor.TimeoutBlockingWaitStrategy;
import com.lmax.disruptor.WaitStrategy;
import com.lmax.disruptor.YieldingWaitStrategy;
import com.lmax.disruptor.dsl.Disruptor;
import com.lmax.disruptor.dsl.ProducerType;

public class DisruptorSink extends Sink {

  class SewerEventHandler implements EventHandler<DelegateEvent> {
    @Override
    public void onEvent(DelegateEvent event, long sequence, boolean endOfBatch) throws Exception {
      subSink.append(event.getDelegate());
    }
  }

  public static final String CONF_THREADS = "sewer.sink.disruptor.threads";
  public static final String CONF_WAIT_STRATEGY = "sewer.sink.disruptor.wait.strategy";
  public static final String CONF_TIMEOUT_MS = "sewer.sink.disruptor.timeout.ms";

  public static final String WAIT_BLOCKING = "blocking";
  public static final String WAIT_BUSYSPIN = "busyspin";
  public static final String WAIT_PHASED   = "phased";
  public static final String WAIT_SLEEPING = "sleeping";
  public static final String WAIT_TIMEOUT  = "timeout";
  public static final String WAIT_YIELDING = "yielding";

  public static final String WAIT_DEFAULT  = WAIT_BLOCKING;

  private static final Logger LOG = LoggerFactory.getLogger(DisruptorSink.class);

  protected Disruptor<DelegateEvent> disruptor;

  public DisruptorSink(String[] args) {
  }

  @Override
  public void close() throws IOException {
    LOG.debug("closing");
    setStatus(CLOSING);
    disruptor.shutdown(); // blocks until buffer is clear
    subSink.close();
    setStatus(CLOSED);
    LOG.debug("closed");
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

    int numThreads = Node.getInstance().getConf().getInt(CONF_THREADS, 1);
    WaitStrategy waitStrategy = createWaitStrategy();

    if (LOG.isDebugEnabled()) {
      LOG.debug("num executor threads: " + numThreads);
      LOG.debug("wait strategy: " + waitStrategy.getClass().getSimpleName());
    }

    final ExecutorService executor = Executors.newFixedThreadPool(numThreads);

    disruptor =
      new Disruptor<DelegateEvent>(
          createEventFactory(),
          Double.valueOf(Math.pow(2, 17)).intValue(), // 2^17 = 131,072
          executor,
          ProducerType.MULTI,
          waitStrategy
          );

    disruptor.handleEventsWith(new SewerEventHandler());
    disruptor.start();

    subSink.open();
    setStatus(FLOWING);
    LOG.debug("flowing");
  }

  /**
   * Create a {@link WaitStrategy} based on configuration. Deafults to {@link BlockingWaitStrategy}
   * @return
   */
  private WaitStrategy createWaitStrategy() {
    String strat = Node.getInstance().getConf().get(CONF_WAIT_STRATEGY, WAIT_DEFAULT).toLowerCase();

    if (WAIT_BLOCKING.equals(strat)) {
      return new BlockingWaitStrategy();
    } else if (WAIT_BUSYSPIN.equals(strat)) {
      return new BusySpinWaitStrategy();
//    } else if (WAIT_PHASED.equals(strat)) {
//      return new PhasedBackoffWaitStrategy(spinTimeoutMillis, yieldTimeoutMillis, units, lockingStrategy);
    } else if (WAIT_SLEEPING.equals(strat)) {
      return new SleepingWaitStrategy();
    } else if (WAIT_TIMEOUT.equals(strat)) {
      long timeout = Node.getInstance().getConf().getLong(CONF_TIMEOUT_MS, 1);
      return new TimeoutBlockingWaitStrategy(timeout, TimeUnit.MILLISECONDS);
    } else if (WAIT_YIELDING.equals(strat)) {
      return new YieldingWaitStrategy();
    } else {
      return new BlockingWaitStrategy(); // default
    }
  }

  private EventFactory<DelegateEvent> createEventFactory() {
    return new EventFactory<DelegateEvent>() {
      public DelegateEvent newInstance() {
        return new DelegateEvent();
      }
    };
  }

}
