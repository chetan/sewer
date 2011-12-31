package net.pixelcop.sewer.util;

import java.io.IOException;

import net.pixelcop.sewer.node.Node;

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

    FileSystem hdfs = path.getFileSystem(Node.getInstance().getConf());

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
