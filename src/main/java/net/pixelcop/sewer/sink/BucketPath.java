/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package net.pixelcop.sewer.sink;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import net.pixelcop.sewer.util.NetworkUtil;

import com.google.common.base.Preconditions;

public class BucketPath {

  /**
   * These are useful to other classes which might want to search for tags in
   * strings.
   */
  private static final String TAG_REGEX = "\\%(\\w|\\%)|\\%\\{([\\w\\.-]+)\\}";
  private static final Pattern tagPattern = Pattern.compile(TAG_REGEX);

  private static final Random RANDOM = new Random();

  private static final String NANOS = "nanos";
  private static final String TIMESTAMP = "timestamp";
  private static final String DATE = "date";
  private static final String THREAD = "thread";
  private static final String RAND = "rand";

  private static final String HOST = "host";
  private static final String HOSTNAME = "hostname";

  private static final Map<String, Object> EMPTY_HEADERS = new HashMap<String, Object>(5);
  static {
    EMPTY_HEADERS.put(HOST, NetworkUtil.getLocalhost());
    EMPTY_HEADERS.put(HOSTNAME, NetworkUtil.getLocalhost());
  }

  /**
   * Hardcoded lookups for %x style escape replacement. Add your own!
   *
   * All shorthands are Date format strings, currently.
   *
   * Returns the empty string if an escape is not recognized.
   *
   * Dates follow the same format as unix date, with a few exceptions.
   *
   */
  private static String replaceShorthand(char c, Map<String, Object> headers) {
    // It's a date
    String formatString = "";
    switch (c) {
    case '%':
      return "%";
    case 'a':
      formatString = "EEE";
      break;
    case 'A':
      formatString = "EEEE";
      break;
    case 'b':
      formatString = "MMM";
      break;
    case 'B':
      formatString = "MMMM";
      break;
    case 'c':
      formatString = "EEE MMM d HH:mm:ss yyyy";
      break;
    case 'd':
      formatString = "dd";
      break;
    case 'D':
      formatString = "MM/dd/yy";
      break;
    case 'H':
      formatString = "HH";
      break;
    case 'I':
      formatString = "hh";
      break;
    case 'j':
      formatString = "DDD";
      break;
    case 'k':
      formatString = "H";
      break;
    case 'l':
      formatString = "h";
      break;
    case 'm':
      formatString = "MM";
      break;
    case 'M':
      formatString = "mm";
      break;
    case 'p':
      formatString = "a";
      break;
    case 's':
      formatString = "s";
      break;
    case 'S':
      formatString = "ss";
      break;
    case 't':
      // This is different from unix date (which would insert a tab character
      // here)
      return headers.get(TIMESTAMP).toString();
    case 'y':
      formatString = "yy";
      break;
    case 'Y':
      formatString = "yyyy";
      break;
    case 'z':
      formatString = "ZZZ";
      break;
    default:
      // LOG.warn("Unrecognized escape in event format string: %" + c);
      return "";
    }
    SimpleDateFormat format = new SimpleDateFormat(formatString);
    return format.format((Date) headers.get(DATE));
  }

  /**
   * Replace all substrings of form %{tagname} with get(tagname).toString() and
   * all shorthand substrings of form %x with a special value.
   *
   * Any unrecognized / not found tags will be replaced with the empty string.
   *
   * TODO(henry): we may want to consider taking this out of Event and into a
   * more general class when we get more use cases for this pattern.
   */
  public static String escapeString(String in, Map<String, Object> headers) {

    if (headers == null) {
      headers = EMPTY_HEADERS;
    }
    long ts = System.currentTimeMillis();
    headers.put(TIMESTAMP, ts);
    headers.put(DATE, new Date(ts));
    headers.put(NANOS, System.nanoTime());
    headers.put(THREAD, Thread.currentThread().getId());

    Matcher matcher = tagPattern.matcher(in);
    StringBuffer sb = new StringBuffer();

    while (matcher.find()) {

      String replacement = "";
      // Group 2 is the %{...} pattern
      if (matcher.group(2) != null) {

        String key = matcher.group(2);
        if (key.equalsIgnoreCase(RAND)) {
          replacement = Integer.toString(RANDOM.nextInt());

        } else if (headers.containsKey(key)) {
          replacement = headers.get(key).toString();
        }
        // else { LOG.warn("Tag " + matcher.group(2) + " not found"); }

      } else {
        // The %x pattern.
        // Since we know the match is a single character, we can
        // switch on that rather than the string.
        Preconditions.checkState(matcher.group(1) != null
            && matcher.group(1).length() == 1,
            "Expected to match single character tag in string " + in);
        char c = matcher.group(1).charAt(0);
        replacement = replaceShorthand(c, headers);
      }

      // The replacement string must have '$' and '\' chars escaped. This
      // replacement string is pretty arcane.
      //
      // replacee : '$' -> for java '\$' -> for regex "\\$"
      // replacement: '\$' -> for regex '\\\$' -> for java "\\\\\\$"
      //
      // replacee : '\' -> for java "\\" -> for regex "\\\\"
      // replacement: '\\' -> for regex "\\\\" -> for java "\\\\\\\\"

      // note: order matters
      replacement = replacement.replaceAll("\\\\", "\\\\\\\\");
      replacement = replacement.replaceAll("\\$", "\\\\\\$");

      matcher.appendReplacement(sb, replacement);
    }
    matcher.appendTail(sb);
    return sb.toString();
  }

}

