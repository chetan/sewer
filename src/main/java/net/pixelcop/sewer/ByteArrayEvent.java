package net.pixelcop.sewer;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Simple Event implementation wrapping a byte array
 *
 * @author chetan
 *
 */
public class ByteArrayEvent implements Event {

    private static final String NULL = "null";

    private byte[] body; // TODO use re-usable buffer for reading sequence files

    public ByteArrayEvent() {
    }

    public ByteArrayEvent(byte[] body) {
        this.body = body;
    }

    public ByteArrayEvent(DataInput in) throws IOException {
        readFields(in);
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(body.length);
        out.write(body);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.body = new byte[in.readInt()];
        in.readFully(body);
    }

    @Override
    public String toString() {
      if (body == null) {
        return NULL;
      }
      return new String(body);
    }

}
