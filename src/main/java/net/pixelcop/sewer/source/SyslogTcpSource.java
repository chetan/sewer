/**
 * Licensed to Cloudera, Inc. under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  Cloudera, Inc. licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package net.pixelcop.sewer.source;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import net.pixelcop.sewer.Event;
import net.pixelcop.sewer.Sink;
import net.pixelcop.sewer.Source;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Source that listens for TCP Syslog events
 *
 * @author chetan
 *
 */
public class SyslogTcpSource extends Source {

  static final Logger LOG = LoggerFactory
      .getLogger(SyslogTcpSource.class);

  final public static int SYSLOG_TCP_PORT = 514;
  final int port;
  final List<ReaderThread> readers = Collections
      .synchronizedList(new ArrayList<ReaderThread>());
  final AtomicLong rejects = new AtomicLong();
  volatile boolean closed = true;

  public SyslogTcpSource(int port) {
    this.port = port;
  }

  public SyslogTcpSource() {
    this(SYSLOG_TCP_PORT); // this is syslog-ng's default tcp port.
  }

  // must synchronize this variable
  ServerThread svrthread;
  ServerSocket sock = null;
  Object sockLock = new Object();

  /**
   * This thread just waits to accept incoming connections and spawn a reader
   * thread.
   */
  class ServerThread extends Thread {
    final int port;

    ServerThread(int port) {
      this.port = port;
      setName("Syslog Server " + getId());
    }

    @Override
    public void run() {
      while (!closed) {
        ServerSocket mySock = null; // guarantee no NPE at accept
        synchronized (sockLock) {
          mySock = sock; // get a local reference to sock.
        }

        if (mySock == null || mySock.isClosed())
          return;

        try {
          Socket client = mySock.accept();
          client.setSoLinger(true, 60);
          new ReaderThread(client).start();

        } catch (IOException e) {
          if (!closed) {
            // could be IOException where we run out of file/socket handles.
            LOG.error("accept had a problem", e);
          }
          return;

        } catch (Exception e) {
          return;

        }
      }
    }
  };

  /**
   * This thread takes a accepted socket and pull data out until it is empty.
   */
  class ReaderThread extends Thread {
    Socket in;
    long count;
    Sink sink;

    ReaderThread(Socket sock) throws Exception {
      setName("Syslog Reader " + getId());
      readers.add(this);
      this.in = sock;
      try {
        this.sink = getSink();
      } catch (Exception e) {
        LOG.error("Failed to create sink for ReaderThread", e);
        throw e;
      }

    }

    @Override
    public void run() {
      //LineReader reader = null;
      //BufferedReader reader = null;
      //DataInputStream reader = null;
      SyslogWireExtractor reader = null;
      try {
        //reader = new LineReader(in.getInputStream());
        //reader = new BufferedReader(new InputStreamReader(in.getInputStream()));
        //reader = new DataInputStream(new BufferedInputStream(in.getInputStream(), 1024 * 64));
        reader = new SyslogWireExtractor(in.getInputStream());
        while (true) {
          try {
            //Event e = SyslogWireExtractor.extractEvent(reader);
            Event e = reader.extractEvent();
            if (e == null) {
              LOG.warn("Got a null syslog event, bailing...");
              break;
            }

            this.sink.append(e);

            if (LOG.isDebugEnabled()) {
              count++;
            }

            if (closed && !in.isClosed()) {
              // close the underlying stream, but try to read out the buffer
              if (LOG.isDebugEnabled()) {
                LOG.debug("Closing underlying SocketInputStream but continuing to read from buffer");
                LOG.debug("syslog count: " + count);
              }
              in.close();
            }

          } catch (IOException ex) {
            LOG.debug("Caught err", ex);
            rejects.incrementAndGet();
            if (closed) {
              break;
            }

          }
        }
        // done.
        in.close();
        sink.close();

        if (LOG.isDebugEnabled()) {
          LOG.debug("syslog run() complete");
          LOG.debug("syslog count: " + count);
        }

      } catch (IOException e) {
        LOG.error("IOException with SyslogTcpSources", e);

      } finally {
        if (in != null && in.isConnected()) {
          try {
            in.close();
          } catch (IOException e) {
            LOG.debug("socket inputstream close failed", e);
          }
        }
        if (reader != null) {
          try {
            reader.close();
          } catch (IOException e) {
            LOG.debug("close failed", e);
          }
        }
        try {
          sink.close();
        } catch (Exception e) {
          LOG.debug("sink close failed", e);
        }
        readers.remove(this);
      }
    }
  }

  @Override
  public void close() throws IOException {
    LOG.info("Closing " + this);
    synchronized (sockLock) {
      closed = true;
      if (sock != null) {
        sock.close();
        sock = null;
      }
    }
    // wait for all readers to close (This is not robust!)
    if (readers.size() != 0) {
      List<ReaderThread> rs = new ArrayList<ReaderThread>(readers); //
      for (ReaderThread r : rs) {
        try {
          r.join();
        } catch (InterruptedException e) {
          LOG.error("Reader threads interrupted, but we are closing", e);
        }
      }
    }
    try {
      if (svrthread != null) {
        svrthread.join();
        svrthread = null;
      }
    } catch (InterruptedException e) {
      LOG.error("Reader threads interrupted, but we are closing", e);
    }
  };

  @Override
  public void open() throws IOException {
    if (LOG.isInfoEnabled()) {
      LOG.info("Opening " + this + " on port " + port);
    }
    synchronized (sockLock) {
      if (!closed) {
        throw new IOException("Attempted to double open socket");
      }
      closed = false;

      if (sock == null) {
        // depending on number of connections, may need to increase backlog
        // value (automatic server socket argument, default is 50)
        try {
          sock = new ServerSocket(port);
          sock.setReuseAddress(true);
          sock.setReceiveBufferSize(64*1024);

        } catch (IOException e) {
          throw new IOException("Failed to create ServerSocket on port " + port + ": " + e);
        }
      }
    }
    svrthread = new ServerThread(port);
    svrthread.start();
  }

}
