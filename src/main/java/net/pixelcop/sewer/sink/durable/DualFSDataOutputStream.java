package net.pixelcop.sewer.sink.durable;

import java.io.IOException;
import java.io.OutputStream;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import net.pixelcop.sewer.StatusProvider;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem.Statistics;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DualFSDataOutputStream extends FSDataOutputStream implements StatusProvider {

  class DualOutputStream extends OutputStream {

    private FSDataOutputStream remoteOut;
    private FSDataOutputStream localOut;

    public DualOutputStream() {
    }

    public DualOutputStream(FSDataOutputStream localOut, FSDataOutputStream remoteOut) {
      this.localOut = localOut;
      this.remoteOut = remoteOut;
    }

    @Override
    public void write(int b) throws IOException {

      // always write to local buffer
      localOut.write(b);

      if (remoteOut == null) {
        // received write before remote opened, mark as errored
        setStatus(ERROR);
        return;
      }

      if (getStatus() == ERROR) {
        // short-circuit further attempts to write to remote
        return;
      }

      try {
        remoteOut.write(b);
      } catch (Throwable t) {
        // remote write failed, mark as error
        setStatus(ERROR);
        LOG.error("error writing to remote", t);
      }
    }

    @Override
    public void flush() throws IOException {
      localOut.flush();
      if (remoteOut != null) {
        remoteOut.flush();
      }
    }

    @Override
    public void close() throws IOException {
      localOut.close();
      if (remoteOut != null) {
        remoteOut.close();
      }
    }

    public FSDataOutputStream getRemoteOut() {
      return remoteOut;
    }

    public void setRemoteOut(FSDataOutputStream remoteOut) {
      this.remoteOut = remoteOut;
    }

    public FSDataOutputStream getLocalOut() {
      return localOut;
    }

    public void setLocalOut(FSDataOutputStream localOut) {
      this.localOut = localOut;
    }
  }

  class RemoteOutputOpenerThread extends Thread {

    private Path remotePath;
    private Configuration conf;

    private CountDownLatch latch;

    public RemoteOutputOpenerThread(Path remotePath, Configuration conf) {
      this.remotePath = remotePath;
      this.conf = conf;
      this.latch = new CountDownLatch(1);
    }

    @Override
    public void run() {

      try {
        // try to open remote path and write back local copy
        FSDataOutputStream newOut = remotePath.getFileSystem(conf).create(remotePath, true);
        ((DualOutputStream) out).setRemoteOut(newOut);

      } catch (Throwable t) {
        LOG.warn("Error opening remote output stream: " + t.getMessage(), t);
        markFailed();

      }

      this.latch.countDown();
    }

    /**
     * Wait the specified time for the stream to open
     *
     * @param timeout In seconds
     * @return
     * @throws InterruptedException
     */
    public boolean await(long timeout) throws InterruptedException {
      return latch.await(timeout, TimeUnit.SECONDS);
    }

  }

  private static final Logger LOG = LoggerFactory.getLogger(DualFSDataOutputStream.class);

  private static final String CONFIG_TIMEOUT = "sewer.sink.secure.remote.timeout";

  private AtomicInteger failCount = new AtomicInteger(0);
  private AtomicInteger status = new AtomicInteger(CLOSED);

  private RemoteOutputOpenerThread thread;

  public DualFSDataOutputStream(Path localPath, Path remotePath, Configuration conf) throws IOException {
    super(null, null, 0); // this will get replaced but must call a super constructor

    // replace out with local buffer stream / remote stream pair
    this.out = new DualOutputStream(localPath.getFileSystem(conf).create(localPath, true), null);


    // we try to asynchronously open the remote stream, but block for (default) 5 seconds
    // waiting for it to open. if it does not open, we return to caller but do not mark an error
    // state. error state will be marked as soon as a write occurs and the remote stream has not
    // yet opened. (this will generally be almost immediately, as we are going to write sequence
    // file headers as soon as we return)

    thread = new RemoteOutputOpenerThread(remotePath, conf);
    thread.start();
    long timeout = conf.getLong(CONFIG_TIMEOUT, 5);
    try {
      LOG.debug("Going to try opening for " + timeout + " seconds");
      if (!thread.await(timeout)) {
        LOG.warn("Remote stream didn't open within " + timeout
            + " seconds; continuing with local buffer only");
      }
    } catch (InterruptedException e) {
      LOG.debug("Interrupted while waiting for remote stream to open");
    }
  }

  private DualFSDataOutputStream(OutputStream out, Statistics stats, long startPosition)
      throws IOException {

    super(out, stats, startPosition);
  }

  private DualFSDataOutputStream(OutputStream out, Statistics stats) throws IOException {
    super(out, stats);
  }

  @Override
  public long getPos() throws IOException {
    return ((DualOutputStream) this.out).getLocalOut().getPos();
  }

  @Override
  public int getStatus() {
    return status.get();
  }

  public void setStatus(int status) {
    this.status.set(status);
  }

  public void markFailed() {
    this.status.set(ERROR);
    this.failCount.incrementAndGet();
  }

  public void reset() {
    this.status.set(FLOWING);
    this.failCount.set(0);
  }

  public int getFailCount() {
    return this.failCount.get();
  }

  public boolean isRemoteOpen() {
    return ((DualOutputStream) out).getRemoteOut() != null;
  }

}
