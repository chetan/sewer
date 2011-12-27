package net.pixelcop.sewer.source;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.List;

import net.pixelcop.sewer.Sink;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class TCPServerThread extends Thread {

  private static final Logger LOG = LoggerFactory.getLogger(TCPServerThread.class);

  private Sink sink;
  private ServerSocket sock;
  private List<TCPReaderThread> readers;

  public TCPServerThread(int port, Sink sink) throws IOException {

    this.readers = new ArrayList<TCPReaderThread>(5);

    this.sink = sink;

    this.sock = new ServerSocket(port);
    this.sock.setReuseAddress(true);
    this.sock.setReceiveBufferSize(64*1024);
    LOG.warn("Now listening on " + port);

  }

  public abstract TCPReaderThread createReader(Socket socket, Sink sink);

  @Override
  public void run() {

    try {

      Socket socket = null;
      while ((socket = this.sock.accept()) != null) {
        System.out.println("Got a customer!");
        TCPReaderThread rt = createReader(socket, sink);
        rt.start();
        readers.add(rt);
      }

    } catch (IOException e) {
      LOG.error("Caught IOException during accept(); shutting down", e);

    } catch (Exception e) {
      LOG.error("Failed to setup ReaderThread sink; shutting down", e);

    } finally {
      if (readers != null && !readers.isEmpty()) {
        for (TCPReaderThread reader : readers) {
          reader.interrupt();
        }

      }

    }

  }

}