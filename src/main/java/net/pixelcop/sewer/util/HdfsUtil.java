package net.pixelcop.sewer.util;

import java.io.IOException;
import java.util.List;

import net.pixelcop.sewer.node.Node;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.io.compress.DefaultCodec;
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

  public static final String CONFIG_COMPRESSION = "sewer.sink.compression";
  public static final String DEFAULT_COMPRESSION = NativeCodeLoader.isNativeCodeLoaded() ? "gzip" : "default";

  private static CompressionCodec codec = null;

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
   * Return the configured {@link CompressionCodec} (via sewer.sink.compression setting)
   *
   * @return {@link CompressionCodec}
   */
  public static CompressionCodec createCodec() {
    if (codec == null) {
      codec = selectCodec(Node.getInstance().getConf());
    }
    return codec;
  }

  /**
   * Creates and returns the configured {@link CompressionCodec}
   * @param conf Hadoop {@link Configuration} to use
   * @return {@link CompressionCodec}
   */
  public static CompressionCodec selectCodec(Configuration conf) {

    String target = conf.get(CONFIG_COMPRESSION, DEFAULT_COMPRESSION);
    if (target.equalsIgnoreCase("deflate")) {
      // older versions don't have the DeflateCodec alias class
      target = DEFAULT_COMPRESSION;
    }

    List<Class<? extends CompressionCodec>> codecClasses =
      CompressionCodecFactory.getCodecClasses(new Configuration());

    for (Class<? extends CompressionCodec> c : codecClasses) {

      if (c.getCanonicalName().equalsIgnoreCase(target)
          || c.getSimpleName().toLowerCase().contains(target.toLowerCase())) {

        try {
          return c.newInstance();
        } catch (Exception e) {
          LOG.warn("Error creating codec: " + e.getMessage());
          return new DefaultCodec();
        }
      }
    }

    LOG.warn("No match for '" + target + "'; selecting DefaultCodec (deflate)");
    return new DefaultCodec();
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
