package net.pixelcop.sewer;


public abstract class Source extends Plumbing {

  /**
   * The class which this source uses for new Events
   *
   * @return
   */
  public abstract Class<?> getEventClass();

  /**
   * Helper method for reading long values from arguments
   *
   * @param args String[]
   * @param pos int position in args array
   * @param defaultValue to return if arg is null or invalid
   * @return long
   */
  protected long getLong(String[] args, int pos, long defaultValue) {

    if (args.length < pos + 1) {
      return defaultValue;
    }

    String arg = args[pos];
    if (arg == null || arg.isEmpty()) {
      return defaultValue;
    }

    try {
      return Long.parseLong(arg);
    } catch (Throwable t) {
    }

    return defaultValue;
  }

  /**
   * Helper method for reading int values from arguments
   *
   * @param args String[]
   * @param pos int position in args array
   * @param defaultValue to return if arg is null or invalid
   * @return int
   */
  protected int getInt(String[] args, int pos, int defaultValue) {
    return (int) getLong(args, pos, defaultValue);
  }

}
