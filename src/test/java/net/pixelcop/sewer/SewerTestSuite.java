package net.pixelcop.sewer;

import net.pixelcop.sewer.node.TestNodeWiring;
import net.pixelcop.sewer.source.SourceTestSuite;

import org.junit.runner.RunWith;
import org.junit.runners.Suite;
import org.junit.runners.Suite.SuiteClasses;

@RunWith(Suite.class)
@SuiteClasses({ TestSourceSinkFactory.class, TestNodeWiring.class, SourceTestSuite.class })
public class SewerTestSuite {

}
