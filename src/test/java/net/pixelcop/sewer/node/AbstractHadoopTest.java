package net.pixelcop.sewer.node;

import java.io.File;
import java.io.IOException;

import net.pixelcop.sewer.source.debug.StringEvent;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.Reader;
import org.apache.hadoop.io.VLongWritable;
import org.junit.After;

public class AbstractHadoopTest extends AbstractNodeTest {

  private MiniDFSCluster dfsCluster;
  private FileSystem fileSystem;

  private int namenodePort = 0;

  public void setupHdfs() throws IOException {
    // Fix for API change between CDH3 & CDH4
    // MiniDFSCluster.getBaseDir()
    // new File(MiniDFSCluster.getBaseDirectory())
    File dfsBaseDir = new File(System.getProperty("test.build.data", "build/test/data"), "/dfs/");
    FileUtil.fullyDelete(dfsBaseDir);

    dfsCluster = new MiniDFSCluster.Builder(createConfig())
      .nameNodePort(getNamenodePort())
      .numDataNodes(1)
      .format(true)
      .manageDataDfsDirs(true)
      .manageNameDfsDirs(true)
      .manageNameDfsSharedDirs(true)
      .build();

    fileSystem = dfsCluster.getFileSystem();
  }

  private Configuration createConfig() throws IOException {
    return loadTestConfig(false, (String[]) null);
  }

  @After
  public void teardownHdfs() throws IOException {

    FileSystem.closeAll();

    try {
      if (fileSystem != null) {
        fileSystem.close();
      }
    }
    catch (Exception e) {
      LOG.debug("Error shutting down HDFS: " + e.getMessage(), e);
    }

    try {
      if (dfsCluster != null) {
        dfsCluster.shutdown();
      }
    }
    catch (Exception e) {
      LOG.debug("Error shutting down HDFS: " + e.getMessage(), e);
    }

    fileSystem = null;
    dfsCluster = null;
  }

  public String getConnectionString() {
    return "hdfs://localhost:" + getNamenodePort() + "/";
  }

  public FileSystem getFileSystem() {
    return fileSystem;
  }

  public int getNamenodePort() {
    if (namenodePort == 0) {
      namenodePort = findOpenPort();
    }
    return namenodePort;
  }

  /**
   * Count the number of events in the given sequence file
   *
   * @param path
   * @return
   * @throws IOException
   */
  protected long countEventsInSequenceFile(String path) throws IOException {

    Reader reader = new SequenceFile.Reader(getFileSystem().getConf(), Reader.file(new Path(path)));

    StringEvent event = new StringEvent();
    VLongWritable lng = new VLongWritable();

    long count = 0;

    while (true) {
      try {
        if (!reader.next(event, lng)) {
          break;
        }
      } catch (IOException e) {
        break;
      }
      count++;
    }

    return count;
  }

}
