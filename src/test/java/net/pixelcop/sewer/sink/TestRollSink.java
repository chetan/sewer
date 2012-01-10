package net.pixelcop.sewer.sink;

import java.io.IOException;

import net.pixelcop.sewer.Plumbing;
import net.pixelcop.sewer.node.BaseNodeTest;
import net.pixelcop.sewer.node.TestableNode;
import net.pixelcop.sewer.sink.debug.CountingSink;

import org.junit.Test;

public class TestRollSink extends BaseNodeTest {

  @Test
  public void testSimpleRollCount() throws IOException, InterruptedException {

    TestableNode node = createAndStartNode("gen(1000, 32, 1)", "roll(1) > counting");
    Thread.sleep(3000);
    node.getSource().close();

    while (node.getSource().getStatus() != Plumbing.CLOSED) {
    }
    Thread.sleep(600); // need to wait for roll to finish last rotate() if necessary

    assertTrue(CountingSink.getOpenCount() > 1);
    assertTrue(CountingSink.getCloseCount() > 1);


  }

}
