package net.pixelcop.sewer.node;

import java.io.File;
import java.io.IOException;

import net.pixelcop.sewer.source.debug.StringEvent;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DFSTestUtil;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.protocol.HdfsConstants.SafeModeAction;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.Reader;
import org.apache.hadoop.io.VLongWritable;
import org.junit.After;

public class AbstractHadoopTest extends AbstractNodeTest {

  private static final int BLOCK_SIZE = 1024;

  private Configuration conf;
  private MiniDFSCluster dfsCluster;
  private FileSystem fileSystem;
  private DistributedFileSystem dfs;

  private int namenodePort = 0;

  public void setupHdfs() throws IOException {
    // Fix for API change between CDH3 & CDH4
    // MiniDFSCluster.getBaseDir()
    // new File(MiniDFSCluster.getBaseDirectory())
    File dfsBaseDir = new File(System.getProperty("test.build.data", "build/test/data"), "/dfs/");
    FileUtil.fullyDelete(dfsBaseDir);

    conf = createConfig();
    conf.setInt(DFSConfigKeys.DFS_BLOCK_SIZE_KEY, BLOCK_SIZE);

    dfsCluster = new MiniDFSCluster.Builder(conf)
      .nameNodePort(getNamenodePort())
      .numDataNodes(3)
      //.waitSafeMode(false)
      .format(true)
      .manageDataDfsDirs(true)
      .manageNameDfsDirs(true)
      .manageNameDfsSharedDirs(true)
      .build();

    fileSystem = dfsCluster.getFileSystem();
    dfs = (DistributedFileSystem) fileSystem;
  }

  public void initBlocks(String r) throws IOException {
    Path file1 = new Path("/tmp/testManualSafeMode/file1" + r);
    Path file2 = new Path("/tmp/testManualSafeMode/file2" + r);

    // create two files with one block each.
    DFSTestUtil.createFile(dfs, file1, 1000, (short)1, 0);
    DFSTestUtil.createFile(dfs, file2, 1000, (short)1, 0);
  }

  /**
   * Manually force the cluster into safe mode
   *
   * @throws IOException
   */
  public void enterSafeMode() throws IOException {

    dfs.close();
    dfsCluster.shutdown();

    // now bring up just the NameNode.
    dfsCluster = new MiniDFSCluster.Builder(conf).numDataNodes(0).format(false).build();
    dfsCluster.waitActive();
    fileSystem = dfsCluster.getFileSystem();
    dfs = (DistributedFileSystem) fileSystem;

    assertTrue("No datanode is started. Should be in SafeMode",
               dfs.setSafeMode(SafeModeAction.SAFEMODE_GET));

    // manually set safemode.
    dfs.setSafeMode(SafeModeAction.SAFEMODE_ENTER);

    // now bring up the datanode and wait for it to be active.
    dfsCluster.startDataNodes(conf, 1, true, null, null);
    dfsCluster.waitActive();

    assertTrue("should still be in SafeMode",
        dfs.setSafeMode(SafeModeAction.SAFEMODE_GET));
  }

  /**
   * Exit safe mode
   *
   * @throws IOException
   */
  public void leaveSafeMode() throws IOException {
    assertFalse("should not be in SafeMode",
        dfs.setSafeMode(SafeModeAction.SAFEMODE_LEAVE));
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

  public MiniDFSCluster getDfsCluster() {
    return dfsCluster;
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
