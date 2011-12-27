package net.pixelcop.sewer.source;

import java.io.IOException;
import java.net.Socket;

import net.pixelcop.sewer.ByteArrayEvent;
import net.pixelcop.sewer.Sink;
import net.pixelcop.sewer.Source;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 3 clients: 24k/sec 25k/sec
 * 2 clients: 34k/sec
 * 1 client:  46k/sec
 *
 *
 * @author chetan
 *
 */
public class TcpWriteableEventSource extends Source {

  private static final Logger LOG = LoggerFactory.getLogger(TcpWriteableEventSource.class);

  private TCPServerThread serverThread;

  @Override
  public void open() throws IOException {

    this.serverThread = new TCPServerThread(9999, getSink()) {

      @Override
      public TCPReaderThread createReader(Socket socket, Sink sink) {

        return new TCPReaderThread(socket, sink) {

          @Override
          public void read() throws IOException {
            ByteArrayEvent event = new ByteArrayEvent(in);
            this.sink.append(event);
          }
        };

      }
    };

    this.serverThread.start();
  }

  @Override
  public void close() throws IOException {
    this.serverThread.interrupt();
  }

}
