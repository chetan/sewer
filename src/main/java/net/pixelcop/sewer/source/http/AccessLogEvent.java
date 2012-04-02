package net.pixelcop.sewer.source.http;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import net.pixelcop.sewer.Event;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.io.BinaryComparable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableUtils;

public class AccessLogEvent extends BinaryComparable implements
    WritableComparable<BinaryComparable>, Event {

  private long timestamp;
  private Text ip;
  private Text host;
  private Text requestPath;
  private Text queryString;
  private Text referer;
  private Text userAgent;
  private Text cookies;

  // Hash used for sorting in map/reduce jobs
  private Text sortHash;

  public AccessLogEvent() {
    ip = new Text();
    host = new Text();
    requestPath = new Text();
    queryString = new Text();
    referer = new Text();
    userAgent = new Text();
    cookies = new Text();
    sortHash = new Text();
  }

  public AccessLogEvent(long timestamp, String ip, String host, String requestPath,
      String queryString, String referer, String userAgent, String cookies) {

    this.timestamp   = timestamp;
    this.ip          = ip == null ? new Text() : new Text(ip);
    this.host        = host == null ? new Text() : new Text(host);
    this.requestPath = requestPath == null ? new Text() : new Text(requestPath);
    this.queryString = queryString == null ? new Text() : new Text(queryString);
    this.referer     = referer == null ? new Text() : new Text(referer);
    this.userAgent   = userAgent == null ? new Text() : new Text(userAgent);
    this.cookies     = cookies == null ? new Text() : new Text(cookies);

    this.sortHash = new Text(ThreadLocalMD5Util.md5Hex(timestamp + ip + host + requestPath
        + queryString + referer + userAgent + cookies));
  }

  @Override
  public void write(DataOutput out) throws IOException {
    WritableUtils.writeVLong(out, timestamp);
    ip.write(out);
    host.write(out);
    requestPath.write(out);
    queryString.write(out);
    referer.write(out);
    userAgent.write(out);
    cookies.write(out);
    sortHash.write(out);
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    this.timestamp = WritableUtils.readVLong(in);
    this.ip.readFields(in);
    this.host.readFields(in);
    this.requestPath.readFields(in);
    this.queryString.readFields(in);
    this.referer.readFields(in);
    this.userAgent.readFields(in);
    this.cookies.readFields(in);
    this.sortHash.readFields(in);
  }

  @Override
  public String toString() {
    return StringUtils.join(new String[] {
        Long.toString(timestamp), ip.toString(), host.toString(), requestPath.toString(),
        queryString.toString(), referer.toString(), userAgent.toString(), cookies.toString(),
        sortHash.toString()
    }, '\t');
  }

  /**
   * Only used by {@link BinaryComparable} for comparisons
   */
  @Override
  public int getLength() {
    return sortHash.getLength();
  }

  /**
   * Only used by {@link BinaryComparable} for comparisons
   */
  @Override
  public byte[] getBytes() {
    return sortHash.getBytes();
  }

}
