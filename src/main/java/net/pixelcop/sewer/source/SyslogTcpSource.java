package net.pixelcop.sewer.source;

import java.io.IOException;
import java.net.Socket;

import net.pixelcop.sewer.Event;
import net.pixelcop.sewer.Sink;
import net.pixelcop.sewer.Source;

import org.apache.commons.lang3.math.NumberUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Source that listens for TCP Syslog events
 *
 * @author chetan
 *
 */
public class SyslogTcpSource extends Source {

  private static final Logger LOG = LoggerFactory.getLogger(SyslogTcpSource.class);

  public static final int SYSLOG_TCP_PORT = 514;

  private final int port;

  private TCPServerThread serverThread;

  public SyslogTcpSource() {
    this(SYSLOG_TCP_PORT);
  }

  public SyslogTcpSource(int port) {
    this.port = port;
  }

  public SyslogTcpSource(String[] args) {
    if (args == null) {
      this.port = SYSLOG_TCP_PORT;
    } else {
      this.port = NumberUtils.toInt(args[0], SYSLOG_TCP_PORT);
    }
  }

  @Override
  public void close() throws IOException {
    LOG.info("Closing " + this.getClass().getSimpleName());
    this.serverThread.interrupt();
  };

  @Override
  public void open() throws IOException {
    if (LOG.isInfoEnabled()) {
      LOG.info("Opening " + this.getClass().getSimpleName() + " on port " + port);
    }

    this.serverThread = new TCPServerThread("Syslog Server", port, getSinkFactory()) {

      @Override
      public TCPReaderThread createReader(Socket socket, Sink sink) {

        return new TCPReaderThread("Syslog Reader", socket, sink) {

          private SyslogWireExtractor reader;

          protected void createInputStream() throws IOException {
            this.reader = new SyslogWireExtractor(this.socket.getInputStream());
          };

          @Override
          public void read() throws IOException {
            Event e = reader.extractEvent();
            this.sink.append(e);
          }

        };


      }
    };

    this.serverThread.start();
  }

}
