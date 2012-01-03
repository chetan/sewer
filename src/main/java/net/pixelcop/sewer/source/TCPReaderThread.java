package net.pixelcop.sewer.source;

import static net.pixelcop.sewer.StatusProvider.*;

import java.io.BufferedInputStream;
import java.io.DataInputStream;
import java.io.EOFException;
import java.io.IOException;
import java.net.Socket;

import net.pixelcop.sewer.Sink;
import net.pixelcop.sewer.StatusProvider;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class TCPReaderThread extends Thread {

  private static final Logger LOG = LoggerFactory.getLogger(TCPReaderThread.class);

  protected final Socket socket;
  protected final Sink sink;
  protected DataInputStream in;
  protected final StatusProvider statusProvider;

  public TCPReaderThread(String name, Socket socket, Sink sink, StatusProvider statusProvider) {
    setName(name + " " + getId());
    this.socket = socket;
    this.sink = sink;
    this.statusProvider = statusProvider;
  }

  @Override
  public void run() {

    try {

      sink.open();
      createInputStream();

      while (true) {
        if (statusProvider.getStatus() == CLOSING) {
          this.socket.close();
          break;
        }
        read();
      }

      // attempt to read out the input buffer for any more messages
      while (true) {
        read();
      }

    } catch (EOFException e) {
      LOG.info("Client closed connection (EOF)");

    } catch (IOException e) {
      if (statusProvider.getStatus() == CLOSING) {
        LOG.debug("IO Exception in ReaderThread: " + e.getMessage());
      } else {
        LOG.warn("IO Exception in ReaderThread", e);
      }

    } finally {
      LOG.debug("TCPReaderThread closing down");
      try {
        LOG.debug("Closing sink");
        sink.close();
      } catch (IOException e) {
        LOG.warn("Error while closing sink");
      }
      try {
        LOG.debug("Closing socket");
        socket.close();
      } catch (IOException e) {
        LOG.info("Error closing socket during shutdown");
      }

    }

  }

  protected void createInputStream() throws IOException {

    this.in = new DataInputStream(new BufferedInputStream(socket.getInputStream(), 64 * 1024));
    // TODO: enable snappy compression
    // new SnappyCodec().createInputStream(in);

  }

  public abstract void read() throws IOException;

}
