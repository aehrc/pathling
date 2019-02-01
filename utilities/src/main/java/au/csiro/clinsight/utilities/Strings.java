/*
 * Copyright © Australian e-Health Research Centre, CSIRO. All rights reserved.
 */

package au.csiro.clinsight.utilities;

/**
 * @author John Grimes
 */
public abstract class Strings {

  public static String capitalize(String name) {
    if (name == null || name.length() == 0) {
      return name;
    }
    char chars[] = name.toCharArray();
    chars[0] = Character.toUpperCase(chars[0]);
    return new String(chars);
  }

}
