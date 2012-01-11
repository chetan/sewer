package net.pixelcop.sewer.sink;

import static org.junit.Assert.*;

import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import net.pixelcop.sewer.Event;
import net.pixelcop.sewer.node.Node;
import net.pixelcop.sewer.node.NodeConfig;
import net.pixelcop.sewer.sink.durable.TestableTransactionManager;

import org.apache.commons.lang3.RandomStringUtils;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.Reader;

public class TxTestHelper {

  private static final List<TxTestHelper> helpers = new ArrayList<TxTestHelper>();

  private final String tmpWalPath;
  private Node node;

  public TxTestHelper(Node node) throws IOException {
    helpers.add(this);
    this.node = node;
    tmpWalPath = "/tmp/sewer/" + RandomStringUtils.randomAlphabetic(8);

    TestableTransactionManager.reset();
    reset();
    node.getConf().set(NodeConfig.WAL_PATH, tmpWalPath);
  }

  public void cleanup() throws IOException {
    FileUtil.fullyDelete(new File(tmpWalPath));
  }

  public void reset() throws IOException {
    cleanup();
    new File(tmpWalPath).mkdirs();
  }

  public void assertTxLogExists() {
    assertTrue("txn.log exists", new File(tmpWalPath + "/" + "txn.log").exists());
  }

  /**
   * Test the count of records written into the disk buffers
   *
   * @param filesExpected Minimum number of buffers that should have been created
   * @param rowCountExpected Expected record count
   * @throws IOException
   */
  public void verifyRecordsInBuffers(int filesExpected, int rowCountExpected, Event event) throws IOException {

    File[] files = new File(tmpWalPath).listFiles(new FilenameFilter() {
      @Override
      public boolean accept(File dir, String name) {
        return (name.charAt(0) != '.'); // ignore .crc file for this test
      }
    });
    assertTrue(files.length >= filesExpected);

    Path path = new Path("file://" + files[0].getAbsolutePath());
    FileSystem fs = path.getFileSystem(node.getConf());
    Reader reader = new SequenceFile.Reader(fs, path, node.getConf());

    NullWritable nil = NullWritable.get();

    int rowCount = 0;
    while (reader.next(nil, event)) {
      rowCount++;
    }

    assertEquals(rowCountExpected, rowCount); // TODO change to >= ?
  }

  public String getTmpWalPath() {
    return tmpWalPath;
  }

  public static void cleanupAllHelpers() {
    for (TxTestHelper helper : helpers) {
      try {
        helper.cleanup();
      } catch (IOException e) {
      }
    }
    helpers.clear();
  }

}
