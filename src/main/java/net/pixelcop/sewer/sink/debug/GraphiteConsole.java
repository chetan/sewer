package net.pixelcop.sewer.sink.debug;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.net.InetSocketAddress;
import java.text.DateFormat;
import java.util.Date;

import javax.net.SocketFactory;

import com.codahale.metrics.graphite.Graphite;

public class GraphiteConsole extends Graphite {

  private Writer writer;
  private DateFormat dateFormat;

  public GraphiteConsole() {
    super(null);
    this.dateFormat = DateFormat.getDateTimeInstance(DateFormat.SHORT, DateFormat.MEDIUM);
  }

  public GraphiteConsole(InetSocketAddress address, SocketFactory socketFactory) {
    this();
  }

  public GraphiteConsole(InetSocketAddress address) {
    this();
  }

  @Override
  public void connect() throws IllegalStateException, IOException {
    this.writer = new BufferedWriter(new OutputStreamWriter(System.out));
  }

  @Override
  public void close() throws IOException {
    this.writer.close();
  }

  /**
   * Writes the given measurement to the console.
   *
   * @param name         the name of the metric
   * @param value        the value of the metric
   * @param timestamp    the timestamp of the metric
   * @throws IOException if there was an error sending the metric
   */
  @Override
  public void send(String name, String value, long timestamp) throws IOException {
    try {
      writer.write(dateFormat.format(new Date(timestamp*1000)));
      writer.write(' ');
      writer.write(sanitize(name));
      writer.write(' ');
      writer.write(sanitize(value));
      writer.write('\n');
      writer.flush();
    } catch (IOException e) {
      throw e;
    }
  }

}
