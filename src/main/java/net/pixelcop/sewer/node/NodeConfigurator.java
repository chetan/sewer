package net.pixelcop.sewer.node;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.List;
import java.util.Properties;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.PosixParser;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

/**
 * Process command line options and create a {@link NodeConfig}
 *
 * @author chetan
 *
 */
public class NodeConfigurator {

  private boolean verbose = false;
  private String filename;

  public NodeConfig configure(String[] args) {
    processCommandLine(args);
    return createConfig(filename);
  }

  private void processCommandLine(String[] args) {
    CommandLineParser parser = new PosixParser();

    Options opts = new Options();
    opts.addOption("c", "config", true, "config filename");
    opts.addOption("h", "help", false, "display help message");
    opts.addOption("v", "verbose", false, "verbose startup");

    CommandLine cmd = null;
    try {
      cmd = parser.parse(opts, args);

    } catch (ParseException e) {
      System.err.println("Failed to parse command line: " + e.getMessage());
      System.exit(2);
    }

    if (cmd.hasOption('h')) {
      new HelpFormatter().printHelp("sewer", opts);
      System.exit(1);
    }

    if (cmd.hasOption('v')) {
      this.verbose = true;
    }

    if (cmd.hasOption('c')) {
      filename = cmd.getOptionValue('c');
    }
  }

  /**
   * Create the NodeConfig
   *
   * @param filename
   * @return
   */
  private NodeConfig createConfig(String filename) {
    NodeConfig conf = new NodeConfig();
    loadHadoopConfigs(conf);

    // Load config.properties from Classpath as well as command line, if passed
    addPropsFromClasspath(conf);

    if (filename != null) {
      addPropsFromFile(conf, filename);
    }

    if (verbose) {
      System.out.println(NodeConfig.SOURCE + " = " + conf.get(NodeConfig.SOURCE));
      System.out.println(NodeConfig.SINK + "   = " + conf.get(NodeConfig.SINK));
    }

    return conf;
  }

  private void addPropsFromClasspath(NodeConfig conf) {

    try {

      Enumeration<URL> urls = Thread.currentThread().getContextClassLoader().getResources("config.properties");
      while (urls.hasMoreElements()) {
        addPropsFromUrl(conf, urls.nextElement());
      }

    } catch (IOException e) {
      System.err.println("unable to load config from classpath: " + e.getMessage());
      System.exit(2);
    }
  }

  @SuppressWarnings("static-access")
  private void addPropsFromUrl(NodeConfig conf, URL props) throws IOException {

    if (verbose) {
      System.out.println("loading config: " + props.toString());
    }

    addProps(conf, props.openStream());
  }

  private void addPropsFromFile(NodeConfig conf, String filename) {
    InputStream stream = null;
    try {
      File file = new File(filename);
      if (verbose) {
        System.out.println("loading config: " + file.toString());
      }
      stream = new FileInputStream(file);

    } catch (FileNotFoundException e) {
      System.err.println("file not found: " + filename);
      System.exit(2);
    }

    if (stream == null) {
      System.err.println("unable to locate a config file. try passing -c <file>");
      System.exit(2);
    }

    try {
      addProps(conf, stream);
    } catch (IOException e) {
      System.err.println("unable to load config from '" + filename + "': " + e.getMessage());
      System.exit(2);
    }
  }

  private void addProps(NodeConfig conf, InputStream stream) throws IOException {
    Properties props = new Properties();
    props.load(stream);
    conf.addResource(props);
  }

  /**
   * Load HADOOP configs from common locations
   *
   * @param conf {@link Configuration} to load into
   * @see {@link #findHadoopConfigs}
   */
  private void loadHadoopConfigs(Configuration conf) {

    for (File file : findHadoopConfigs()) {
      if (verbose) {
        System.out.println("loading config: " + file.toString());;
      }
      conf.addResource(new Path(file.toString()));
    }

  }

  /**
   * Look for HADOOP config files in a few common places:
   * /etc/hadoop/conf
   * /usr/local/hadoop/conf

   * @return List of config files
   */
  private List<File> findHadoopConfigs() {

    String[] search = new String[] {
        "/etc/hadoop/conf/core-site.xml",
        "/etc/hadoop/conf/hdfs-site.xml",
        "/etc/hadoop/conf/mapred-site.xml",
        "/usr/local/hadoop/conf/core-site.xml",
        "/usr/local/hadoop/conf/hdfs-site.xml",
        "/usr/local/hadoop/conf/mapred-site.xml",
    };

    List<File> list = new ArrayList<File>();
    for (int i = 0; i < search.length; i++) {
      File f = new File(search[i]);
      if (f.exists()) {
        list.add(f);
      }
    }

    return list;
  }

}
