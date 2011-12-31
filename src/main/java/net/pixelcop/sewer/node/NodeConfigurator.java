package net.pixelcop.sewer.node;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.ArrayList;
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
      e.printStackTrace();
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

    File file = null;
    try {
      file = getConfigFile(filename);
    } catch (FileNotFoundException e) {
      System.err.println("file not found: " + filename);
      System.exit(1);
    }

    if (file == null) {
      System.err.println("unable to locate a config file. try passing -c <file>");
      System.exit(1);
    }

    Properties props = new Properties();
    try {
      props.load(new FileInputStream(file));
    } catch (IOException e) {
      System.err.println("unable to load config from '" + file.toString() + "': " + e.getMessage());
      System.exit(1);
    }
    conf.addResource(props);
    return conf;
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

  /**
   * Find the sewer config, falling back to the classpath if one was not passed in
   *
   * @param filename
   * @return
   * @throws FileNotFoundException
   */
  @SuppressWarnings("static-access")
  private File getConfigFile(String filename) throws FileNotFoundException {

    if (filename != null) {
      File file = new File(filename);
      if (file.exists()) {
        return file;
      }
      throw new FileNotFoundException(filename);
    }

    URL props = Thread.currentThread().getContextClassLoader()
        .getSystemResource("config.properties");

    try {
      File file = new File(props.toURI());
      if (file.exists()) {
        if (verbose) {
          System.out.println("loading config: " + file.toString());
        }
        return file;
      }

    } catch (URISyntaxException e) {
    }

    return null;
  }

}
