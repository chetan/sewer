package net.pixelcop.sewer.source;

import static net.pixelcop.sewer.StatusProvider.*;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.util.List;
import java.util.Vector;

import net.pixelcop.sewer.Sink;
import net.pixelcop.sewer.PlumbingFactory;
import net.pixelcop.sewer.StatusProvider;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class TCPServerThread extends Thread {

  /**
   * Waits for the given {@link TCPReaderThread} to join() then removes it from the
   * readers list.
   *
   * @author chetan
   *
   */
  class CleanupThread extends Thread {
    private TCPReaderThread reader;
    public CleanupThread(TCPReaderThread reader) {
      this.reader = reader;
      setName("TCP Server Cleanup " + getId());
    }
    @Override
    public void run() {
      try {
        reader.join();
        readers.remove(reader);
      } catch (InterruptedException e) {
        LOG.warn("tcp server cleanup thread was interrupted");
      }
    }
  }

  private static final Logger LOG = LoggerFactory.getLogger(TCPServerThread.class);

  private PlumbingFactory<Sink> sinkFactory;
  private ServerSocket sock;
  private List<TCPReaderThread> readers;
  private final StatusProvider statusProvider;

  public TCPServerThread(String name, int port, PlumbingFactory<Sink> sinkFactory,
      StatusProvider statusProvider) throws IOException {

    setName(name + " " + getId());

    this.readers = new Vector<TCPReaderThread>(5);

    this.sinkFactory = sinkFactory;
    this.statusProvider = statusProvider;

    this.sock = new ServerSocket(port);
    this.sock.setSoTimeout(1000);
    this.sock.setReuseAddress(true);
    this.sock.setReceiveBufferSize(64*1024);

    LOG.info("Now listening on " + port);
  }

  public abstract TCPReaderThread createReader(Socket socket, Sink sink);

  @Override
  public void run() {

    while (statusProvider.getStatus() != CLOSING) {

      try {

        Socket socket = null;
        while ((socket = this.sock.accept()) != null) {
          socket.setSoLinger(true, 60);

          if (LOG.isDebugEnabled()) {
            LOG.debug("new connection from: " + socket.getInetAddress().getHostName());
          }

          TCPReaderThread rt = createReader(socket, sinkFactory.build());
          rt.start();
          readers.add(rt);
          new CleanupThread(rt).start();
        }

      } catch (SocketTimeoutException e) {
        continue;

      } catch (IOException e) {
        LOG.error("Caught IOException during accept(); shutting down", e);

      } catch (Exception e) {
        LOG.error("Failed to setup ReaderThread sink; shutting down", e);

      }

    }

    if (this.sock != null) {
      try {
        this.sock.close();
      } catch (IOException e) {
        LOG.error("Error closing socket: " + e.getMessage(), e);
      }
    }

    // cleanup
    LOG.debug("TCPServerThread closing down");
    if (readers != null && !readers.isEmpty()) {
      LOG.debug("Interrupting " + readers.size() + " reader threads");
      for (TCPReaderThread reader : readers) {
        // reader.interrupt();
      }
    }

  }

  /**
   * Wait for all {@link TCPReaderThread}s to join
   * @throws InterruptedException
   */
  public void joinReaders() throws InterruptedException {
    if (readers == null) {
      return;
    }
    LOG.debug("joining " + readers.size() + " reader thread(s)");
    for (TCPReaderThread reader : readers) {
      reader.join();
    }
  }

}