package net.pixelcop.sewer;
import java.io.IOException;
import java.text.DecimalFormat;
import java.text.NumberFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

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

    @Override
    public String toString() {
      NumberFormat f = DecimalFormat.getIntegerInstance();
      if (duration > 0) {
        return sink + "\t" +
        f.format(count) + "\t" +
        f.format(opsPerSec) + "\t\t" +
        notes;
      }
      return sink + "\t" + notes;
    }
  }

  private static final long TEST_WARMUP = 10000;
  private static final long TEST_DURATION = 30000;

  protected static final Logger LOG = LoggerFactory.getLogger(Benchmark.class);

  protected static final List<String> waitStrategies = new ArrayList<String>();
  static {
    waitStrategies.add(DisruptorSink.WAIT_BLOCKING);
    waitStrategies.add(DisruptorSink.WAIT_BUSYSPIN);
    waitStrategies.add(DisruptorSink.WAIT_SLEEPING);
    waitStrategies.add(DisruptorSink.WAIT_TIMEOUT);
    waitStrategies.add(DisruptorSink.WAIT_YIELDING);
  }

  private String compressor = null;
  private long test = 0;
  private static long runTestNum = 0;

  public static void main(String[] args) {

    if (args != null && args.length > 0 && args[0] != null && !args[0].isEmpty()) {
      try {
        runTestNum = Long.parseLong(args[0]);
      } catch (Throwable t) {
      }
    }

    JUnitCore.main(Benchmark.class.getName());
  }

  @Test
  public void testPerf() throws InterruptedException, IOException {

    Properties props = new Properties();
    props.setProperty(DisruptorSink.CONF_THREADS, "1");
    props.setProperty(DisruptorSink.CONF_TIMEOUT_MS, "1");

    String source = "tgen(256)";

    List<Result> results = new ArrayList<Result>();
    //System.err.println("sink\tmsgs\tops/sec\t\tnotes");

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
    //System.err.println("\n\n\n\n\n\n");

    // for (Result result : results) {
    // System.err.println(result);
    // }
    //System.err.println("\n\n\n\n\n\n");
  }

  private void runAllTests(Properties props, String source, String dest, List<Result> results)
      throws InterruptedException, IOException {

    // basic tests
    results.add(runTest(source, dest));
    results.add(runTest(source, "buffer > " + dest));

    // disruptor > null
    runDisruptorTests(props, source, "disruptor > " + dest, results);

    // meter before
    results.add(runTest(source, "meter > " + dest));
    results.add(runTest(source, "meter > buffer > " + dest));
    runDisruptorTests(props, source, "meter > disruptor > " + dest, results);

    // meter after
    results.add(runTest(source, "buffer > meter > " + dest));
    runDisruptorTests(props, source, "disruptor > meter > " + dest, results);

    // meter before & after
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

    test++;
    if (runTestNum > 0 && test != runTestNum) {
      return null;
    }

    // add compressor to notes
    if (compressor != null) {
      if (notes == null || notes.isEmpty()) {
        notes = "\t" + compressor;
      } else {
        notes = notes + "\t" + compressor;
      }
    }

    Result r = new Result();
    r.source = source;
    r.sink = sink;
    r.notes = notes;

    System.out.println("running test: " + r);

    NodeConfig conf = loadTestConfig(false, "");
    if (props != null) {
      conf.addResource(props);
    }
    final TestableNode node = createNode(source, sink, null, conf);

    // start node, opens source
    node.start();

    LOG.info("warming up...");
    Thread.sleep(TEST_WARMUP);

    LOG.info("warmed up. starting test...");
    double startTime = System.nanoTime();
    ((ThreadedEventGeneratorSource) node.getSource()).resetCounters();

    Thread.sleep(TEST_DURATION);

    LOG.info("test complete. cleaning up...");
    final CountDownLatch stopLatch = new CountDownLatch(1);
    Thread t = new Thread() {
      @Override
      public void run() {
        try {
          node.await();
        } catch (InterruptedException e) {
        }
        node.cleanup();
        stopLatch.countDown();
      }
    };
    t.start();
    if (!stopLatch.await(30, TimeUnit.SECONDS)) {
      LOG.warn("node didn't cleanup within 30 seconds, interrupting");
      t.interrupt();
    }


    r.count = ((ThreadedEventGeneratorSource) node.getSource()).getTotalCount();
    r.duration = System.nanoTime() - startTime;
    r.opsPerSec = (1000L * 1000L * 1000L * r.count) / r.duration;

    System.gc(); // why not

    LOG.info("test run completed.");
    System.err.println(r);
    return r;
  }

}
