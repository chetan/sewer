package net.pixelcop.sewer.sink.durable;

import java.io.IOException;
import java.util.Iterator;
import java.util.LinkedList;

import net.pixelcop.sewer.Event;
import net.pixelcop.sewer.Sink;

import org.apache.commons.lang3.math.NumberUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ThreadLocalBufferSink extends Sink {

  private static final Logger LOG = LoggerFactory.getLogger(ThreadLocalBufferSink.class);

  private static final int DEFAULT_SIZE = 8;

  private LinkedList<AsyncBufferSink> buffers;

  private int maxPoolSize;

  private Iterator<AsyncBufferSink> iterator;

  private ThreadLocal<AsyncBufferSink> localBuffer = new ThreadLocal<AsyncBufferSink>() {
    @Override
    protected AsyncBufferSink initialValue() {
      synchronized (buffers) {
        if (!iterator.hasNext()) {
          iterator = buffers.iterator();
        }
        return iterator.next();
      }
    }
  };

  public ThreadLocalBufferSink(String[] args) {
    if (args == null || args.length == 0) {
      maxPoolSize = DEFAULT_SIZE;

    } else {
      maxPoolSize = NumberUtils.toInt(args[0], DEFAULT_SIZE);
    }
  }

  @Override
  public void close() throws IOException {
    setStatus(CLOSING);
    subSink.close();
    for (AsyncBufferSink buff : buffers) {
      buff.close();
    }
    setStatus(CLOSED);
  }

  @Override
  public void append(Event event) throws IOException {
    localBuffer.get().append(event);
  }

  @Override
  public void open() throws IOException {
    setStatus(OPENING);

    createSubSink();
    BufferFactory factory = new BufferFactory(subSink);
    buffers = new LinkedList<AsyncBufferSink>();

    for (int i = 0; i < maxPoolSize; i++) {
      try {
        buffers.add(factory.makeObject());
      } catch (Exception e) {
        throw new IOException("Error populating buffers", e);
      }
    }

    iterator = buffers.iterator();

    subSink.open();
    setStatus(FLOWING);
  }

}
