package net.pixelcop.sewer.node;

import java.io.IOException;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.mapred.JobConf;
import org.junit.After;

public class AbstractHadoopTest extends AbstractNodeTest {

  private MiniDFSCluster dfsCluster;
  private FileSystem fileSystem;

  private int dataNodes = 1;


  //@Before
  public void setupHdfs() throws IOException {
    dfsCluster = new MiniDFSCluster(new JobConf(), dataNodes, true, null);
    fileSystem = dfsCluster.getFileSystem();
  }

  @After
  public void teardownHdfs() {
    try {
      if (dfsCluster != null) {
        dfsCluster.shutdown();
      }
    }
    catch (Exception e) {
      LOG.debug("Error shutting down HDFS: " + e.getMessage(), e);
    }
  }

  public String getConnectionString() {
    if (dfsCluster == null) {
      return "hdfs://localhost:30000/";
    }
    return "hdfs://localhost:" + dfsCluster.getNameNodePort() + "/";
  }

  public FileSystem getFileSystem() {
    return fileSystem;
  }

}
