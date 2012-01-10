package net.pixelcop.sewer.source;

import static org.junit.Assert.*;

import java.io.IOException;
import java.net.URL;
import java.net.URLConnection;

import net.pixelcop.sewer.node.BaseNodeTest;
import net.pixelcop.sewer.node.TestableNode;
import net.pixelcop.sewer.sink.debug.CountingSink;

import org.junit.Test;

public class TestHttpPixelSource extends BaseNodeTest {

  @Test
  public void testCreateSourceArgs() throws IOException {
    TestableNode node = createNode("pixel", "counting");
    assertNotNull(node);
    assertEquals(8080, ((HttpPixelSource) node.getSource()).getPort());

    node = createNode("pixel(8181)", "counting");
    assertNotNull(node);
    assertEquals(8181, ((HttpPixelSource) node.getSource()).getPort());

    node = createNode("pixel('888')", "counting");
    assertNotNull(node);
    assertEquals(888, ((HttpPixelSource) node.getSource()).getPort());
  }

  @Test
  public void testAppend() throws IOException {
    TestableNode node = createNode("pixel", "counting");
    assertNotNull(node);
    node.start();
    try {
      node.await();
    } catch (InterruptedException e) {
      fail("error");
    }

    System.out.println("starting test");
    ping(30);
    assertEquals(30, CountingSink.getCount());
    CountingSink.reset();

    ping(15);
    assertEquals(15, CountingSink.getCount());
    CountingSink.reset();
  }

  private void ping(int count) throws IOException {
    URL url = new URL("http://localhost:8080/foobar");
    for (int i = 0; i < count; i++) {
      openUrl(url);
    }
  }

  private void openUrl(URL url) throws IOException {
    URLConnection conn = url.openConnection();
    conn.connect();
    conn.getHeaderField(0);
  }

}
