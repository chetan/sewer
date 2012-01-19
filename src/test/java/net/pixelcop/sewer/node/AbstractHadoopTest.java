package net.pixelcop.sewer.node;

import java.io.IOException;
import java.net.ServerSocket;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.junit.After;

public class AbstractHadoopTest extends AbstractNodeTest {

  private MiniDFSCluster dfsCluster;
  private FileSystem fileSystem;

  private int namenodePort = 0;

  public void setupHdfs() throws IOException {
    FileUtil.fullyDelete(MiniDFSCluster.getBaseDir());
    dfsCluster = new MiniDFSCluster(getNamenodePort(), createConfig(), 1, true, true, null, null);
    fileSystem = dfsCluster.getFileSystem();
  }

  private Configuration createConfig() {
    return new NodeConfigurator().configure(null, false);
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

  public int findOpenPort() {

    int port = 30000;

    while (true) {

      try {
        ServerSocket s = new ServerSocket(port);
        s.setReuseAddress(true);
        s.close();
        return port;

      } catch (IOException e) {
        e.printStackTrace();
      }

      port += 1;

    }

  }

}
