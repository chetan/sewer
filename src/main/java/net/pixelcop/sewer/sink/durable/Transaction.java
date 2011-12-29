package net.pixelcop.sewer.sink.durable;

import java.util.Date;

import org.apache.commons.lang3.time.FastDateFormat;

public class Transaction {

  private final static FastDateFormat DATE_FORMAT = FastDateFormat.getInstance("yyyyMMdd-HHmmssSSSZ");

  private String id;
  private String bucket;

  private long startTime;

  public Transaction(String bucket) {
    this.bucket = bucket;
    this.id = generateId();
    this.startTime = System.currentTimeMillis();
  }

  private String generateId() {
    long tid = Thread.currentThread().getId();
    String formattedDate = DATE_FORMAT.format(new Date());

    // formatted so that lexigraphical and chronological can use same sort
    // yyyyMMdd-HHmmssSSSz.0000000nanos.00000pid
    return String.format("%s.%012d.%08d", formattedDate, System.nanoTime(), tid);
  }

  public String getId() {
    return id;
  }

  public void setId(String id) {
    this.id = id;
  }

  public String getBucket() {
    return bucket;
  }

  public void setBucket(String bucket) {
    this.bucket = bucket;
  }

  public long getStartTime() {
    return startTime;
  }

  public void setStartTime(long startTime) {
    this.startTime = startTime;
  }

}
