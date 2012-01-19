package net.pixelcop.sewer.sink;

import net.pixelcop.sewer.sink.durable.TestReliableSequenceFileSink;
import net.pixelcop.sewer.sink.durable.TestReliableSink;
import net.pixelcop.sewer.sink.durable.TestRollSink;

import org.junit.runner.RunWith;
import org.junit.runners.Suite;
import org.junit.runners.Suite.SuiteClasses;

@RunWith(Suite.class)
@SuiteClasses({ TestRollSink.class, TestReliableSink.class, TestReliableSequenceFileSink.class })
public class SinkTestSuite {

}
