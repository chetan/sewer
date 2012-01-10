package net.pixelcop.sewer.node;

import junit.framework.TestCase;
import net.pixelcop.sewer.SinkRegistry;
import net.pixelcop.sewer.SourceRegistry;
import net.pixelcop.sewer.sink.debug.CountingSink;
import net.pixelcop.sewer.source.debug.EventGeneratorSource;
import net.pixelcop.sewer.source.debug.NullSource;

import org.junit.Before;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class BaseNodeTest extends TestCase {

  protected static final Logger LOG = LoggerFactory.getLogger(BaseNodeTest.class);

  @Before
  @Override
  protected void setUp() throws Exception {
    SourceRegistry.register("null", NullSource.class);
    SourceRegistry.register("gen", EventGeneratorSource.class);
    SinkRegistry.register("counting", CountingSink.class);
  }

}
