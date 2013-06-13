package net.pixelcop.sewer;
import java.io.IOException;
import java.text.DecimalFormat;
import java.text.NumberFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import net.pixelcop.sewer.node.AbstractNodeTest;
import net.pixelcop.sewer.node.NodeConfig;
import net.pixelcop.sewer.node.TestableNode;
import net.pixelcop.sewer.sink.buffer.DisruptorSink;
import net.pixelcop.sewer.source.debug.ThreadedEventGeneratorSource;
import net.pixelcop.sewer.util.HdfsUtil;

import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.junit.Test;
import org.junit.runner.JUnitCore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class Benchmark extends AbstractNodeTest {

  class Result {
    double duration;
    double count;
    double opsPerSec;
    String source;
    String sink;
    String notes;
  }

  private static final long TEST_WARMUP = 10000;
  private static final long TEST_DURATION = 30000;

  protected static final Logger LOG = LoggerFactory.getLogger(Benchmark.class);

  protected static final List<String> waitStrategies = new ArrayList<>();
  static {
    waitStrategies.add(DisruptorSink.WAIT_BLOCKING);
    waitStrategies.add(DisruptorSink.WAIT_BUSYSPIN);
    waitStrategies.add(DisruptorSink.WAIT_SLEEPING);
    waitStrategies.add(DisruptorSink.WAIT_TIMEOUT);
    waitStrategies.add(DisruptorSink.WAIT_YIELDING);
  }

  private String compressor = null;

  public static void main(String[] args) {
    JUnitCore.main(Benchmark.class.getName());
  }

  @Test
  public void testPerf() throws InterruptedException, IOException {

    Properties props = new Properties();
    props.setProperty(DisruptorSink.CONF_THREADS, "1");
    props.setProperty(DisruptorSink.CONF_TIMEOUT_MS, "1");

    String source = "tgen(256)";

    List<Result> results = new ArrayList<>();

    // null tests
    runAllTests(props, source, "null", results);

    // i/o tests
    List<Class<? extends CompressionCodec>> codecs = CompressionCodecFactory.getCodecClasses(loadTestConfig(false, ""));
    for (Class<? extends CompressionCodec> codec : codecs) {
      props.put(HdfsUtil.CONFIG_COMPRESSION, codec.getName());
      compressor = codec.getSimpleName();
      runAllTests(props, source, "seqfile('/tmp/sewer-bench')", results);
    }
    compressor = null;

    // print results
    NumberFormat f = DecimalFormat.getIntegerInstance();

    System.err.println("\n\n\n\n\n\n");
    System.err.println("sink\tmsgs\tops/sec\t\tnotes");
    for (Result result : results) {
      System.err.print(result.sink);
      System.err.print("\t");
      System.err.print(f.format(result.count));
      System.err.print("\t");
      System.err.print(f.format(result.opsPerSec));
      System.err.print("\t\t");
      System.err.print(result.notes);
      System.err.print("\n");
    }
    System.err.println("\n\n\n\n\n\n");
  }

  private void runAllTests(Properties props, String source, String dest, List<Result> results)
      throws InterruptedException, IOException {

    // basic tests
    results.add(runTest(source, dest));
    results.add(runTest(source, "buffer > " + dest));

    // disruptor > null
    runDisruptorTests(props, source, "disruptor > " + dest, results);

    // meter > null
    results.add(runTest(source, "meter > " + dest));
    results.add(runTest(source, "buffer > meter > " + dest));
    runDisruptorTests(props, source, "disruptor > meter > " + dest, results);

    // 2 meters > null
    results.add(runTest(source, "meter > buffer > meter > " + dest));
    runDisruptorTests(props, source, "meter > disruptor > meter > " + dest, results);
  }

  private void runDisruptorTests(Properties props, String source, String sink, List<Result> results)
      throws InterruptedException, IOException {

    for (String strat : waitStrategies) {
      props.setProperty(DisruptorSink.CONF_WAIT_STRATEGY, strat);
      results.add(runTest(source, sink, props, strat));
    }

  }

  private Result runTest(String source, String sink) throws InterruptedException, IOException {
    return runTest(source, sink, null, "");
  }

  private Result runTest(String source, String sink, Properties props, String notes) throws InterruptedException, IOException {

    // add compressor to notes
    if (compressor != null) {
      if (notes == null) {
        notes = "" + compressor;
      } else {
        notes = notes + ", " + compressor;
      }
    }

    Result r = new Result();
    r.source = source;
    r.sink = sink;
    r.notes = notes;

    NodeConfig conf = loadTestConfig(false, "");
    if (props != null) {
      conf.addResource(props);
    }
    TestableNode node = createNode(source, sink, null, conf);

    // start node, opens source
    node.start();

    LOG.info("warming up...");
    Thread.sleep(TEST_WARMUP);

    double startTime = System.nanoTime();
    ((ThreadedEventGeneratorSource) node.getSource()).resetCounters();

    Thread.sleep(TEST_DURATION);
    node.await();
    node.cleanup();

    r.count = ((ThreadedEventGeneratorSource) node.getSource()).getTotalCount();
    r.duration = System.nanoTime() - startTime;
    r.opsPerSec = (1000L * 1000L * 1000L * r.count) / r.duration;

    System.gc(); // why not

    return r;
  }

}
