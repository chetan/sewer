package net.pixelcop.sewer.source.http;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import net.pixelcop.sewer.Event;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.io.WritableUtils;

public class AccessLogEvent implements Event {

  // ${time_nsecs},${ip_remote},"${http_host}","${request}","${query_string}"
  // "${http_referrer}","${http_user_agent}","${http_cookie}"

  private long timestamp;
  private String ip;
  private String host;
  private String requestPath;
  private String queryString;
  private String referer;
  private String userAgent;
  private String cookies;

  public AccessLogEvent() {
  }

  public AccessLogEvent(long timestamp, String ip, String host, String requestPath,
      String queryString, String referer, String userAgent, String cookies) {

    this.timestamp = timestamp;
    this.ip = ip;
    this.host = host;
    this.requestPath = requestPath;
    this.queryString = queryString;
    this.referer = referer;
    this.userAgent = userAgent;
    this.cookies = cookies;

  }

  @Override
  public void write(DataOutput out) throws IOException {
    out.writeLong(timestamp);
    WritableUtils.writeString(out, ip);
    WritableUtils.writeString(out, host);
    WritableUtils.writeString(out, requestPath);
    WritableUtils.writeString(out, queryString);
    WritableUtils.writeString(out, referer);
    WritableUtils.writeString(out, userAgent);
    WritableUtils.writeString(out, cookies);
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    this.timestamp = in.readLong();
    this.ip = WritableUtils.readString(in);
    this.host = WritableUtils.readString(in);
    this.requestPath = WritableUtils.readString(in);
    this.queryString = WritableUtils.readString(in);
    this.referer = WritableUtils.readString(in);
    this.userAgent = WritableUtils.readString(in);
    this.cookies = WritableUtils.readString(in);
  }

  @Override
  public String toString() {
    return StringUtils.join(new String[] {
        Long.toString(timestamp), ip, host, requestPath,
        queryString, referer, userAgent, cookies
    }, '\t');
  }

  public void copyFrom(Event event) {
    if (!(event instanceof AccessLogEvent)) {
      return;
    }
    AccessLogEvent sevent = (AccessLogEvent) event;
    timestamp = sevent.timestamp;
    ip = sevent.ip;
    host = sevent.host;
    requestPath = sevent.requestPath;
    queryString = sevent.queryString;
    referer = sevent.referer;
    userAgent = sevent.userAgent;
    cookies = sevent.cookies;
  }

}
