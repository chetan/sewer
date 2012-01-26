package net.pixelcop.sewer.sink.buffer;

import net.pixelcop.sewer.Sink;

import org.apache.commons.pool.BasePoolableObjectFactory;

public class BufferFactory extends BasePoolableObjectFactory<AsyncBufferSink> {

  private static final String[] nullArgs = new String[] {};
  private Sink subSink;

  public BufferFactory(Sink subSink) {
    this.subSink = subSink;
  }

  @Override
  public AsyncBufferSink makeObject() throws Exception {

    AsyncBufferSink buffer = new AsyncBufferSink(nullArgs);
    buffer.setSubSink(subSink);
    buffer.open();

    return buffer;
  }

  @Override
  public void destroyObject(AsyncBufferSink obj) throws Exception {
    obj.close();
  }

}
