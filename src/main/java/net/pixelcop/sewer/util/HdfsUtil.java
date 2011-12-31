package net.pixelcop.sewer.util;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
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

    Configuration conf = new Configuration();
    conf.setInt("io.file.buffer.size", 16384*4); // TODO temp workaround until we fix Config
    FileSystem hdfs = path.getFileSystem(conf);

    if (hdfs.exists(path)) {
      if (LOG.isDebugEnabled()) {
        try {
          LOG.debug("Deleting path: " + path.toString());
        } catch (Throwable t) {
          // path.toString() throws a nullpointer sometimes, not sure why
          LOG.debug("Deleting path: " + path.toUri().toString());
        }
      }

      hdfs.delete(path, false);
    }
  }

}
