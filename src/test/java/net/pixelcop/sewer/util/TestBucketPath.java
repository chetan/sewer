package net.pixelcop.sewer.util;

import net.pixelcop.sewer.node.AbstractNodeTest;
import net.pixelcop.sewer.sink.BucketPath;

import org.junit.Test;

public class TestBucketPath extends AbstractNodeTest {

  @Test
  public void testEscapeString() {

    String str = "hdfs://localhost:9000/test/collect/%{yyyy-MM-dd/HH00}/data-%{host}-%{rand}-%{thread}-%{nanos}-%{yyyyMMdd-HHmmss}";

    String escaped = BucketPath.escapeString(str);

    assertNotNull(escaped);
    assertNotSame(str, escaped);
    assertFalse(escaped.contains("%"));

  }

}
