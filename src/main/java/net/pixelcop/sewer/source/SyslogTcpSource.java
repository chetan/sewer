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
import java.net.Socket;

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

  private static final Logger LOG = LoggerFactory.getLogger(SyslogTcpSource.class);

  public static final int SYSLOG_TCP_PORT = 514;

  private final int port;

  private TCPServerThread serverThread;

  public SyslogTcpSource(int port) {
    this.port = port;
  }

  public SyslogTcpSource() {
    this(SYSLOG_TCP_PORT); // this is syslog-ng's default tcp port.
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
