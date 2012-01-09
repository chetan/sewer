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

    this.serverThread = new TCPServerThread("TCP Writeable Server", 9999, getSinkFactory(), this) {

      @Override
      public TCPReaderThread createReader(Socket socket, Sink sink) {

        return new TCPReaderThread("TCP Writeable Reader", socket, sink, TcpWriteableEventSource.this) {

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
    setStatus(CLOSING);
    LOG.info("Closing " + this.getClass().getSimpleName());
    try {
      LOG.debug("joining server thread");
      this.serverThread.join();
      LOG.debug("server thread has joined");
    } catch (InterruptedException e) {
      LOG.error("Interrupted waiting for server thread to join");
    }
    try {
      this.serverThread.joinReaders();
      LOG.debug("all reader threads have joined");
    } catch (InterruptedException e) {
      LOG.error("Interrupted waiting for reader threads to join");
    }
    setStatus(CLOSED);
  }

  @Override
  public Class<?> getEventClass() {
    return ByteArrayEvent.class;
  }

}
