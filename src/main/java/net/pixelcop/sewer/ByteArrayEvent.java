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

    private byte[] body;

    public ByteArrayEvent() {
    }

    public ByteArrayEvent(byte[] body) {
        this.setBody(body);
    }

    public ByteArrayEvent(DataInput in) throws IOException {
        readFields(in);
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(getBody().length);
        out.write(getBody());
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        setBody(new byte[in.readInt()]);
        in.readFully(getBody());
    }

    public void setBody(byte[] body) {
        this.body = body;
    }

    public byte[] getBody() {
        return body;
    }

    @Override
    public String toString() {
      return new String(body);
    }

}
