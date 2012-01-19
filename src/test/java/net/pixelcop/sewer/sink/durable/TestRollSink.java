package net.pixelcop.sewer.sink.durable;

import java.io.IOException;

import net.pixelcop.sewer.node.AbstractNodeTest;
import net.pixelcop.sewer.node.TestableNode;
import net.pixelcop.sewer.sink.debug.CountingSink;

import org.junit.Test;

public class TestRollSink extends AbstractNodeTest {

  @Test
  public void testSimpleRollCount() throws IOException, InterruptedException {

    // gen 3 events, 1 sec apart (each event is 32 bytes)
    TestableNode node = createAndStartNode("gen(3, 32, 1)", "roll(1) > counting");

    assertTrue(CountingSink.getOpenCount() > 1);
    assertTrue(CountingSink.getCloseCount() > 1);
  }

}
