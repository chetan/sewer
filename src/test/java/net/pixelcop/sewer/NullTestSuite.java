package net.pixelcop.sewer;

import org.junit.runner.RunWith;
import org.junit.runners.Suite;
import org.junit.runners.Suite.SuiteClasses;

// Hacky way to skip running tests, but still compile them

@RunWith(Suite.class)
@SuiteClasses({ })
public class NullTestSuite {
}
