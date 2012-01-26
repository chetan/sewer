package net.pixelcop.sewer.sink.buffer;

import java.io.IOException;

import net.pixelcop.sewer.Event;
import net.pixelcop.sewer.Sink;

import org.apache.commons.lang3.math.NumberUtils;
import org.apache.commons.pool.ObjectPool;
import org.apache.commons.pool.impl.GenericObjectPool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BufferPoolSink extends Sink {

  private static final Logger LOG = LoggerFactory.getLogger(BufferPoolSink.class);

  private static final int DEFAULT_SIZE = 8;

  private ObjectPool<AsyncBufferSink> bufferPool;

  private int maxPoolSize;

  public BufferPoolSink(String[] args) {
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
    try {
      bufferPool.close();
    } catch (Exception e) {
      setStatus(ERROR);
      throw new IOException("Error closing buffer pool", e);
    }
    setStatus(CLOSED);
  }

  @Override
  public void append(Event event) throws IOException {

    AsyncBufferSink buffer = null;
    try {
      buffer = bufferPool.borrowObject();
    } catch (Exception e) {
      throw new IOException("Failed to get buffer", e);
    }

    try {
      buffer.append(event);

    } finally {
      try {
        bufferPool.returnObject(buffer);
      } catch (Exception e) {
        throw new IOException("Failed to return buffer", e);
      }
    }

  }

  @Override
  public void open() throws IOException {
    setStatus(OPENING);

    createSubSink();
    BufferFactory factory = new BufferFactory(subSink);
    bufferPool = new GenericObjectPool<AsyncBufferSink>(factory, maxPoolSize);

    subSink.open();
    setStatus(FLOWING);
  }

}
