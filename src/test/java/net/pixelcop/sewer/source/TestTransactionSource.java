package net.pixelcop.sewer.source;

import java.io.File;
import java.io.IOException;

import net.pixelcop.sewer.node.AbstractNodeTest;
import net.pixelcop.sewer.sink.durable.TestableTransactionManager;
import net.pixelcop.sewer.sink.durable.Transaction;

import org.junit.Test;

public class TestTransactionSource extends AbstractNodeTest {

  /**
   * Opening a tx file that does not exist should throw no exceptions. Just return silently
   * from the open() call
   *
   * @throws IOException
   */
  @Test
  public void testTxFileNotFound() throws IOException {

    createNode("null", "null");

    Transaction tx = new Transaction();
    tx.setId("foobar");
    tx.setStartTime(System.currentTimeMillis());
    tx.setBucket("baz");
    tx.setFileExt(".seq.deflate");

    TransactionSource txSource = new TransactionSource(tx);
    txSource.setSinkFactory(TestableTransactionManager.getSinkFactory());
    txSource.open();

  }

  /**
   * Opening a tx file that is zero bytes should throw no exceptions. Just return silently
   * from the open() call
   *
   * @throws IOException
   */
  @Test
  public void testTxZeroLength() throws IOException {

    createNode("null", "null");

    Transaction tx = new Transaction();
    tx.setId("foobar");
    tx.setStartTime(System.currentTimeMillis());
    tx.setBucket("baz");
    tx.setFileExt(".seq.deflate");

    new File(tx.createTxPath().toUri()).createNewFile();

    TransactionSource txSource = new TransactionSource(tx);
    txSource.setSinkFactory(TestableTransactionManager.getSinkFactory());
    txSource.open();

  }

}
