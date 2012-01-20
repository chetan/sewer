package net.pixelcop.sewer;

import net.pixelcop.sewer.node.TestNodeWiring;
import net.pixelcop.sewer.sink.SinkTestSuite;
import net.pixelcop.sewer.source.SourceTestSuite;
import net.pixelcop.sewer.util.TestHdfsUtil;

import org.junit.runner.RunWith;
import org.junit.runners.Suite;
import org.junit.runners.Suite.SuiteClasses;

@RunWith(Suite.class)
@SuiteClasses({ TestPlumbingFactory.class, TestNodeWiring.class, SourceTestSuite.class,
    SinkTestSuite.class, TestHdfsUtil.class })
public class SewerTestSuite {

}
