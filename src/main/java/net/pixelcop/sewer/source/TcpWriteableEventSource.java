package net.pixelcop.sewer.source;

import java.io.BufferedInputStream;
import java.io.DataInputStream;
import java.io.EOFException;
import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.List;

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

  private ServerThread serverThread;

  @Override
  public void open() throws IOException {
    this.serverThread = new ServerThread();
    this.serverThread.start();
  }

  @Override
  public void close() throws IOException {
    this.serverThread.interrupt();
  }

  class ServerThread extends Thread {

    private ServerSocket sock;
    private List<ReaderThread> readers;

    public ServerThread() throws IOException {

      this.readers = new ArrayList<ReaderThread>(5);

      this.sock = new ServerSocket(9999);
      this.sock.setReuseAddress(true);
      this.sock.setReceiveBufferSize(64*1024);
      LOG.warn("Now listening on 9999");

    }

    @Override
    public void run() {

      try {

        Socket socket = null;
        while ((socket = this.sock.accept()) != null) {
          System.out.println("Got a customer!");
          ReaderThread rt = new ReaderThread(socket);
          rt.start();
          readers.add(rt);
        }

      } catch (IOException e) {
        LOG.error("Caught IOException during accept(); shutting down", e);

      } catch (InterruptedException e) {
        LOG.warn("ServerThread interrupted, must be shutting down", e);

      } catch (Exception e) {
        LOG.error("Failed to setup ReaderThread sink; shutting down", e);

      } finally {
        if (readers != null && !readers.isEmpty()) {
          for (ReaderThread reader : readers) {
            reader.interrupt();
          }

        }

      }

    }

  }

  class ReaderThread extends Thread {

    private Socket socket;
    private Sink sink;

    public ReaderThread(Socket socket) throws Exception {
      this.socket = socket;
      this.sink = getSink();
    }

    @Override
    public void run() {

      DataInputStream in = null;
      try {
        in = new DataInputStream(new BufferedInputStream(socket.getInputStream(), 64 * 1024));
        // TODO: enable snappy compression
        // new SnappyCodec().createInputStream(in);

        while (true) {
          ByteArrayEvent event = new ByteArrayEvent(in);
          sink.append(event);
        }

      } catch (EOFException e) {
        LOG.info("Client closed..");

      } catch (IOException e) {
        LOG.warn("IO Exception in ReaderThread", e);

      } finally {
        try {
          sink.close();
        } catch (Exception e) {
          LOG.info("Error closing sink during shutdown");
        }
        try {
          socket.close();
        } catch (IOException e) {
          LOG.info("Error closing socket during shutdown");
        }

      }

    }

  }

}
