/*
 * Copyright © Australian e-Health Research Centre, CSIRO. All rights reserved.
 */

package au.csiro.clinsight.utilities;

import java.util.Arrays;
import java.util.LinkedList;

/**
 * @author John Grimes
 */
public abstract class Strings {

  public static String capitalize(String name) {
    if (name == null || name.length() == 0) {
      return name;
    }
    char[] chars = name.toCharArray();
    chars[0] = Character.toUpperCase(chars[0]);
    return new String(chars);
  }

  public static String backTicks(String value) {
    return "`" + value + "`";
  }

  public static LinkedList<String> tokenizePath(String path) {
    return new LinkedList<>(Arrays.asList(path.split("\\.")));
  }

  public static String untokenizePath(Iterable<String> tokens) {
    return String.join(".", tokens);
  }

}
