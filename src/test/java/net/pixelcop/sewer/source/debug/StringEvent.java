package net.pixelcop.sewer.source.debug;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import net.pixelcop.sewer.Event;

import org.apache.hadoop.io.WritableUtils;

public class StringEvent implements Event {

  private String string;

  public StringEvent() {
  }

  public StringEvent(String str) {
    this.string = str;
  }

  @Override
  public void write(DataOutput out) throws IOException {
    WritableUtils.writeString(out, string);
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    this.string = WritableUtils.readString(in);
  }

}
