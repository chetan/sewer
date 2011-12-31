package net.pixelcop.sewer.source;

import java.io.Closeable;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;

import net.pixelcop.sewer.ByteArrayEvent;
import net.pixelcop.sewer.Event;

import org.apache.hadoop.io.Text;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Simple Syslog Event extractor. Events are expected to be of the format:
 *
 * <p><code>&lt;PRI&gt;msg data\n</code></p>
 *
 * @author chetan
 *
 */
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

  private final Text text;

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
    this.text = new Text();
  }

  /**
   * Reads the next line of input and creates an Event
   *
   * @return Event from next line of input
   * @throws IOException If the line length is extremely large (greater than Integer.MAX_VALUE)
   * @throws EOFException When stream EOF is reached
   */
  public Event extractEvent() throws IOException {

    text.clear();
    int txtLength = 0; //tracks str.getLength(), as an optimization
    int newlineLength = 0; //length of terminating newline
    long bytesConsumed = 0;
    do {
      int startPosn = bufferPosn; //starting from where we left off the last time
      int ltPosn = -1;
      if (bufferPosn >= bufferLength) {
        // fill buffer
        startPosn = bufferPosn = 0;
        bufferLength = in.read(buffer);
        if (bufferLength <= 0) {
          break; // EOF
        }
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
        text.append(buffer, startPosn, appendLength);
        txtLength += appendLength;
      }
    } while (newlineLength == 0 && bytesConsumed < maxBytesToConsume);


    // Check for errors, else return a new event

    if (bytesConsumed > (long)Integer.MAX_VALUE) {
      throw new IOException("Too many bytes before newline: " + bytesConsumed);
    }

    if (bytesConsumed == 0 && bufferLength < 0) {
      throw new EOFException();
    }

    if (bytesConsumed == 0) {
      LOG.warn("no bytes consumed, nothing left, no EOF?!");
    }

    return new ByteArrayEvent(text.getBytes());
  }


  @Override
  public void close() throws IOException {
    if (this.in != null) {
      this.in.close();
    }
  }


}
