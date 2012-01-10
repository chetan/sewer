package net.pixelcop.sewer;

import net.pixelcop.sewer.node.TestNodeWiring;

import org.junit.runner.RunWith;
import org.junit.runners.Suite;
import org.junit.runners.Suite.SuiteClasses;

@RunWith(Suite.class)
@SuiteClasses({ TestSourceSinkFactory.class, TestNodeWiring.class })
public class SewerTestSuite {

}
