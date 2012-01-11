package net.pixelcop.sewer.sink;

import org.junit.runner.RunWith;
import org.junit.runners.Suite;
import org.junit.runners.Suite.SuiteClasses;

@RunWith(Suite.class)
@SuiteClasses({ TestRollSink.class, TestReliableSink.class })
public class SinkTestSuite {

}
