package net.pixelcop.sewer;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;


@SuppressWarnings({"rawtypes", "unchecked"})
public class SourceSinkFactory<T> {

  private static final Pattern configPattern = Pattern.compile("^(.*?)(\\((.*?)\\))?$");

  public class SourceSinkBuilder {
    private Class clazz;
    private String[] args;

    public SourceSinkBuilder(Class clazz, String[] args) {
      this.clazz = clazz;
      this.args = args;
    }

    public Object build() throws Exception {
      return (T) clazz.getConstructor(String[].class).newInstance((Object) args);
    }

    @Override
    public String toString() {
      StringBuilder sb = new StringBuilder();
      sb.append(clazz.getSimpleName());
      sb.append("(");
      for (int i = 0; i < args.length; i++) {
        if (i > 0) {
          sb.append(", ");
        }
        sb.append(args[i]);
      }
      sb.append(")");
      return sb.toString();
    }

    public Class getClazz() {
      return clazz;
    }

    public String[] getArgs() {
      return args;
    }
  }

  private String config;
  private List<SourceSinkBuilder> classes;

  private SourceSinkFactory() {
  }

  public SourceSinkFactory(List<SourceSinkBuilder> classes) {
    this.classes = classes;
  }

  public SourceSinkFactory(String config) {
    this.config = config;
    this.classes = new ArrayList<SourceSinkBuilder>();
    parseConfig();
  }

  private void parseConfig() {

    String[] pieces = config.split("\\s*>\\s*");
    for (int i = 0; i < pieces.length; i++) {

      String piece = pieces[i];

      Matcher matcher = configPattern.matcher(piece);
      if (!matcher.find()) {
        // TODO throw exception
      }

      String clazzId = matcher.group(1);
      if (!SinkRegistry.exists(clazzId)) {
        // TODO throw exception
      }

      String[] args = null;
      if (matcher.group(3) != null && !matcher.group(3).isEmpty()) {
        args = matcher.group(3).split("\\s*,\\s*");
        if (args != null && args.length > 0) {
          for (int j = 0; j < args.length; j++) {
            args[j] = args[j].trim();
            if (args.length > 0 && (args[j].charAt(0) == '\'' || args[j].charAt(0) == '"')) {
              args[j] = args[j].substring(1, args[j].length() - 1);
            }
          }
        }
      }

      classes.add(new SourceSinkBuilder(SinkRegistry.get(clazzId), args));
    }

  }

  public T build() {

    try {

      if (classes.size() == 1) {
        // Sources will not be chained
        return (T) classes.get(0).build();
      }

      // Create first Sink in chain
      Sink sink = (Sink) classes.get(0).build();

      // Create a new factory minus the sink that was just created
      SourceSinkFactory<Sink> factory = new SourceSinkFactory<Sink>();
      List list = new ArrayList(this.classes.size());
      list.addAll(this.classes);
      list.remove(0);
      factory.classes = list;

      sink.setSinkFactory(factory);

      return (T) sink;

    } catch (Exception e) {
      e.printStackTrace();
    }

    return null; // TODO throw exception?
  }

  public List<SourceSinkBuilder> getClasses() {
    return classes;
  }

}
