package net.pixelcop.sewer.util;

import java.io.IOException;

import net.pixelcop.sewer.node.Node;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.DeflateCodec;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.io.compress.SnappyCodec;
import org.apache.hadoop.util.NativeCodeLoader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HdfsUtil {

  private static final Logger LOG = LoggerFactory.getLogger(HdfsUtil.class);

  /**
   * Delete the given Path if it exists.
   *
   * Works for both file:// and hdfs:// paths.
   *
   * @param path
   * @throws IOException
   */
  public static void deletePath(Path path) throws IOException {

    FileSystem hdfs = path.getFileSystem(Node.getInstance().getConf());

    if (LOG.isDebugEnabled()) {
      LOG.debug("Deleting path: " + pathToString(path));
    }

    if (hdfs.exists(path)) {
      hdfs.delete(path, false);

    } else if (LOG.isDebugEnabled()) {
      LOG.debug("Path not found: " + pathToString(path));
    }
  }

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
      if (ipc) {
        return new SnappyCodec();
      }

      return new GzipCodec();

    } else {
      return new DeflateCodec();
    }

  }


}
