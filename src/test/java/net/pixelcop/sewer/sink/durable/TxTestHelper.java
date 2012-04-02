package net.pixelcop.sewer.sink.durable;

import static org.junit.Assert.*;

import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import net.pixelcop.sewer.Event;
import net.pixelcop.sewer.node.NodeConfig;

import org.apache.commons.lang3.RandomStringUtils;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.Reader;
import org.apache.hadoop.io.VLongWritable;

public class TxTestHelper {

  private static final List<TxTestHelper> helpers = new ArrayList<TxTestHelper>();

  private final String tmpWalPath;
  private NodeConfig conf;

  private TransactionManager txMan;

  public TxTestHelper(NodeConfig conf, String tmpWalPath) throws IOException {
    helpers.add(this);
    this.tmpWalPath = tmpWalPath != null ? tmpWalPath : "/tmp/sewer/" + RandomStringUtils.randomAlphabetic(8);
    this.conf = conf;
    conf.set(NodeConfig.WAL_PATH, this.tmpWalPath);
    new File(this.tmpWalPath).mkdirs();
  }

  public void cleanup() throws IOException {
    FileUtil.fullyDelete(new File(tmpWalPath));
    if (txMan != null) {
      txMan.shutdown();
    }
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
        // ignore .crc files and txn.log for this test
        return (name.charAt(0) != '.' && !name.equalsIgnoreCase("txn.log"));
      }
    });

    if (filesExpected == 0) {
      assertEquals(0, files.length);
      return;

    } else {
      assertTrue(files.length >= filesExpected);
    }

    int rowCount = 0;

    for (int i = 0; i < files.length; i++) {
      File file = files[i];

      Path path = new Path(file.toURI());
      FileSystem fs = path.getFileSystem(conf);
      Reader reader = new SequenceFile.Reader(fs, path, conf);

      VLongWritable lng = new VLongWritable();

      while (reader.next(event, lng)) {
        rowCount++;
      }
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

  public void addTxMan() {
    this.txMan = TransactionManager.getInstance();
  }

}
