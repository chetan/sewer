package net.pixelcop.sewer.sink;

import java.io.BufferedOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.Socket;

import net.pixelcop.sewer.Event;
import net.pixelcop.sewer.Sink;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TcpWriteableEventSink extends Sink {

  private static final Logger LOG = LoggerFactory.getLogger(TcpWriteableEventSink.class);

  private Socket socket;
  private DataOutputStream out;

  public TcpWriteableEventSink() {
  }

  public void open() throws IOException {

    socket = new Socket("localhost", 9999);
    if (!socket.isConnected()) {
      LOG.error("Unable to connect to server!! bailing");
      return;
    }

    out = new DataOutputStream(new BufferedOutputStream(socket.getOutputStream(), 64 * 1024));
    // TODO: enable snappy compression
    // new SnappyCodec().createOutputStream(out);

  }

  @Override
  public void append(Event e) throws IOException {
    // TODO some sort of stat counting?
    // super.append(e); // update stats, etc
    e.write(this.out);
  }

  @Override
  public void close() throws IOException {

    if (socket == null) {
      return;
    }

    socket.close();
  }

}
