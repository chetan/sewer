package net.pixelcop.sewer.util;

import java.io.IOException;

import net.pixelcop.sewer.node.Node;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.DefaultCodec;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.util.NativeCodeLoader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HdfsUtil {

  /**
   * Helper class for asynchronously opening HDFS {@link FileSystem}'s
   * @author chetan
   *
   */
  static class AsyncFSOpener extends Thread {

    private Path path;
    private Configuration conf;

    private FileSystem fs;

    public AsyncFSOpener(Path path, Configuration conf) {
      this.path = path;
      this.conf = conf;
    }

    @Override
    public void run() {
      try {
        fs = path.getFileSystem(conf);
      } catch (IOException e) {
        LOG.warn("Error opening " + pathToString(path), e);
      }
    }

    public FileSystem getFS() {
      return fs;
    }

  }

  private static final Logger LOG = LoggerFactory.getLogger(HdfsUtil.class);

  /**
   * Delete the given Path if it exists.
   *
   * Works for both file:// and hdfs:// paths.
   *
   * @param path
   * @throws IOException
   * @throws InterruptedException
   */
  public static void deletePath(Path path) throws IOException, InterruptedException {

    if (LOG.isDebugEnabled()) {
      LOG.debug("Deleting path: " + pathToString(path));
    }

    FileSystem hdfs = getFilesystemAsync(path, null);

    if (hdfs.exists(path)) {
      hdfs.delete(path, false);

    } else if (LOG.isDebugEnabled()) {
      LOG.debug("Path not found: " + pathToString(path));
    }
  }

  /**
   * Wrapper around Path.toString() to catch random NPE
   *
   * @param path
   * @return String
   */
  public static String pathToString(Path path) {
    try {
      return path.toString();
    } catch (Throwable t) {
      // path.toString() throws a nullpointer sometimes, not sure why
      return path.toUri().toString();
    }
  }

  /**
   * Return the best available codec for this system
   *
   * @return {@link CompressionCodec}
   */
  public static CompressionCodec createCodec() {
    return createCodec(false);
  }

  /**
   * Return the best available codec for this system
   *
   * @param ipc Codec will be used for inter-process communication
   * @return {@link CompressionCodec}
   */
  public static CompressionCodec createCodec(boolean ipc) {

    if (NativeCodeLoader.isNativeCodeLoaded()) {
      // this codec is only available in newer versions of Hadoop
      // if (ipc) {
      // return new SnappyCodec();
      // }

      return new GzipCodec();

    } else {
      return new DefaultCodec();
    }

  }

  /**
   * Asynchronously creates a {@link FileSystem} for the given {@link Path}
   *
   * @param path
   * @return {@link FileSystem}
   * @throws InterruptedException If open is interrupted
   */
  public static FileSystem getFilesystemAsync(Path path)
      throws InterruptedException {

    AsyncFSOpener afso = new AsyncFSOpener(path, Node.getInstance().getConf());
    afso.start();
    afso.join();

    return afso.getFS();
  }

  /**
   * Asynchronously creates a {@link FileSystem} for the given {@link Path}
   *
   * @param path
   * @param conf
   * @return {@link FileSystem}
   * @throws InterruptedException If open is interrupted
   */
  public static FileSystem getFilesystemAsync(Path path, Configuration conf)
      throws InterruptedException {

    if (conf == null) {
      conf = Node.getInstance().getConf();
    }

    AsyncFSOpener afso = new AsyncFSOpener(path, conf);
    afso.start();
    afso.join();

    return afso.getFS();
  }


}
