package net.pixelcop.sewer.sink.buffer;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import net.pixelcop.sewer.Event;

/**
 * Simple {@link Event} implementation that wraps some other {@link Event}
 *
 * @author chetan
 *
 */
public class DelegateEvent implements Event {

  private Event delegate;

  public DelegateEvent() {
  }

  public DelegateEvent(Event delegate) {
    this();
    this.delegate = delegate;
  }

  public Event getDelegate() {
    return delegate;
  }

  public void setDelegate(Event delegate) {
    this.delegate = delegate;
  }

  @Override
  public void write(DataOutput out) throws IOException {
    delegate.write(out);
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    throw new IOException("delegates are write only");
  }

}
