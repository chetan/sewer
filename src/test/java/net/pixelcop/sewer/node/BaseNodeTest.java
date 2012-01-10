package net.pixelcop.sewer.node;

import java.security.Permission;

import junit.framework.TestCase;
import net.pixelcop.sewer.SinkRegistry;
import net.pixelcop.sewer.SourceRegistry;
import net.pixelcop.sewer.sink.debug.CountingSink;
import net.pixelcop.sewer.source.debug.EventGeneratorSource;
import net.pixelcop.sewer.source.debug.FailOpenSource;
import net.pixelcop.sewer.source.debug.NullSource;

import org.junit.Before;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class BaseNodeTest extends TestCase {

  private static class NoExitSecurityManager extends SecurityManager {
    @Override
    public void checkPermission(Permission perm) {
      // allow anything.
    }

    @Override
    public void checkPermission(Permission perm, Object context) {
      // allow anything.
    }

    @Override
    public void checkExit(int status) {
      super.checkExit(status);
      throw new ExitException(status);
    }
  }

  protected static final Logger LOG = LoggerFactory.getLogger(BaseNodeTest.class);

  private SecurityManager securityManager;

  @Before
  @Override
  protected void setUp() throws Exception {
    securityManager = System.getSecurityManager();
    System.setSecurityManager(new NoExitSecurityManager());
    SourceRegistry.register("null", NullSource.class);
    SourceRegistry.register("gen", EventGeneratorSource.class);
    SourceRegistry.register("failopen", FailOpenSource.class);
    SinkRegistry.register("counting", CountingSink.class);
  }

  @Override
  protected void tearDown() throws Exception {
    super.tearDown();
    System.setSecurityManager(securityManager);
  }

}
