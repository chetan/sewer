package net.pixelcop.sewer.sink.durable;

import java.util.Date;

import net.pixelcop.sewer.Event;

import org.apache.commons.lang3.time.FastDateFormat;
import org.apache.hadoop.fs.Path;

public class Transaction {

  private final static FastDateFormat DATE_FORMAT = FastDateFormat.getInstance("yyyyMMdd-HHmmssSSSZ");

  /**
   * Unique ID for this Transaction. Used for reading/writing local buffers
   */
  private String id;

  /**
   * File extension to use for local buffer file
   */
  private String fileExt;

  /**
   * Destination sink path this Transaction will be written to
   */
  private String bucket;

  /**
   * Time (in ms) this Transaction was started
   */
  private long startTime;

  /**
   * Class name of Event Class used in this Transaction
   */
  private String eventClass;


  /**
   * Create new Transaction instance
   *
   * @param clazz
   * @param bucket
   */
  public Transaction(Class<?> clazz, String bucket, String fileExt) {
    this.eventClass = clazz.getCanonicalName();
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

  /**
   * Path that local buffer will be written to
   *
   * @return Path
   */
  public Path createTxPath() {
    return new Path("file://" +
        TransactionManager.getInstance().getWALPath() + "/" + this.id + this.getFileExt());
  }

  /**
   * Helper method for creating a new Event object from eventClass
   *
   * @return
   * @throws ClassNotFoundException
   * @throws IllegalAccessException
   * @throws InstantiationException
   */
  public Event newEvent() throws Exception {
    return (Event) Class.forName(eventClass).newInstance();
  }

  @Override
  public String toString() {
    return this.id;
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

  public String getEventClass() {
    return eventClass;
  }

  public void setEventClass(String eventClass) {
    this.eventClass = eventClass;
  }

  public void setFileExt(String fileExt) {
    this.fileExt = fileExt;
  }

  public String getFileExt() {
    return fileExt;
  }

}
