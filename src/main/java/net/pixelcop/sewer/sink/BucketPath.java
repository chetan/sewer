package net.pixelcop.sewer.sink;

import java.util.Date;
import java.util.Random;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import net.pixelcop.sewer.util.NetworkUtil;

import org.apache.commons.lang3.time.FastDateFormat;

public class BucketPath {

  /**
   * These are useful to other classes which might want to search for tags in
   * strings.
   */
  private static final String TAG_REGEX = "\\%\\{([\\w-./]+)\\}";
  private static final Pattern TAG_PATTERN = Pattern.compile(TAG_REGEX);

  private static final String EMPTY_STRING = "";

  private static final Random RANDOM = new Random();

  private static final String NANOS = "nanos";
  private static final String TIMESTAMP = "timestamp";
  private static final String THREAD = "thread";
  private static final String RAND = "rand";

  private static final String HOST = "host";
  private static final String HOSTNAME = "hostname";

  private static final String HOST_VAL = NetworkUtil.getLocalhost();

  /**
   * Replace all substrings of form %{var} or %{pattern} with correct value.
   *
   * Any unrecognized / not found tags will be replaced with the empty string.
   */
  public static String escapeString(String in) {

    long ts = System.currentTimeMillis();
    Date date = new Date(ts);

    Matcher matcher = TAG_PATTERN.matcher(in);
    StringBuffer sb = new StringBuffer();

    while (matcher.find()) {

      String replacement = null;

      // Group 1 is the %{...} pattern

      if (matcher.group(1) != null) {
        String key = matcher.group(1);

        if (key.equalsIgnoreCase(RAND)) {
          replacement = Integer.toString(Math.abs(RANDOM.nextInt()));

        } else if (key.equalsIgnoreCase(HOST) || key.equalsIgnoreCase(HOSTNAME)) {
          replacement = HOST_VAL;

        } else if (key.equalsIgnoreCase(TIMESTAMP)) {
          replacement = Long.toString(ts);

        } else if (key.equalsIgnoreCase(NANOS)) {
          replacement = Long.toString(System.nanoTime());

        } else if (key.equalsIgnoreCase(THREAD)) {
          replacement = Long.toString(Thread.currentThread().getId());

        } else {
          replacement = FastDateFormat.getInstance(matcher.group(1)).format(date);
        }

      }

      if (replacement == null) {
        replacement = EMPTY_STRING;
      }

      matcher.appendReplacement(sb, replacement);
    }
    matcher.appendTail(sb);
    return sb.toString();
  }

}

