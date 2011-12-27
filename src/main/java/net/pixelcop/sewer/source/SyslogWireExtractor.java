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

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;

import net.pixelcop.sewer.ByteArrayEvent;
import net.pixelcop.sewer.Event;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SyslogWireExtractor implements Closeable {

  static final Logger LOG =
      LoggerFactory.getLogger(SyslogWireExtractor.class);

  private static final byte LT = '<';
  private static final byte GT = '>';
  private static final byte LF = '\n';

  private static final int maxLineLength = Integer.MAX_VALUE;
  private static final int maxBytesToConsume = Integer.MAX_VALUE;

  private static final int DEFAULT_BUFFER_SIZE = 64 * 1024;

  private int bufferSize = DEFAULT_BUFFER_SIZE;
  private InputStream in;
  private byte[] buffer;
  // the number of bytes of real data in the buffer
  private int bufferLength = 0;
  // the current position in the buffer
  private int bufferPosn = 0;

  /**
   * Create a line reader that reads from the given stream using the
   * default buffer-size (64k).
   * @param in The input stream
   * @throws IOException
   */
  public SyslogWireExtractor(InputStream in) {
    this(in, DEFAULT_BUFFER_SIZE);
  }

  /**
   * Create a line reader that reads from the given stream using the
   * given buffer-size.
   * @param in The input stream
   * @param bufferSize Size of the read buffer
   * @throws IOException
   */
  public SyslogWireExtractor(InputStream in, int bufferSize) {
    this.in = in;
    this.bufferSize = bufferSize;
    this.buffer = new byte[this.bufferSize];
  }

  public Event extractEvent() throws IOException {

    int txtLength = 0; //tracks str.getLength(), as an optimization
    int newlineLength = 0; //length of terminating newline
    long bytesConsumed = 0;
    do {
      int startPosn = bufferPosn; //starting from where we left off the last time
      int ltPosn = -1;
      if (bufferPosn >= bufferLength) {
        startPosn = bufferPosn = 0;
        bufferLength = in.read(buffer);
        if (bufferLength <= 0)
          break; // EOF
      }
      if (buffer[bufferPosn] == LT) {
        ltPosn = bufferPosn;
      }
      for (; bufferPosn < bufferLength; ++bufferPosn) { //search for newline
        if (ltPosn >= 0 && buffer[bufferPosn] == GT) {
          startPosn = bufferPosn + 1;
        }
        if (buffer[bufferPosn] == LF) {
          newlineLength = 1;
          ++bufferPosn; // at next invocation proceed from following byte
          break;
        }
      }
      int readLength = bufferPosn - startPosn;
      bytesConsumed += readLength;
      int appendLength = readLength - newlineLength;
      if (appendLength > maxLineLength - txtLength) {
        appendLength = maxLineLength - txtLength;
      }
      if (appendLength > 0) {
        byte[] bytes = new byte[appendLength];
        System.arraycopy(buffer, startPosn, bytes, 0, appendLength);
        return new ByteArrayEvent(bytes);
        //str.append(buffer, startPosn, appendLength);
        //txtLength += appendLength;
      }
    } while (newlineLength == 0 && bytesConsumed < maxBytesToConsume);

    if (bytesConsumed > (long)Integer.MAX_VALUE)
      throw new IOException("Too many bytes before newline: " + bytesConsumed);

    if (bytesConsumed == 0) {
      return null; // TODO EOFException ??
    }

    return null;
  }


  @Override
  public void close() throws IOException {
    if (this.in != null) {
      this.in.close();
    }
  }


}
