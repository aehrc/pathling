/*
 * Copyright © Australian e-Health Research Centre, CSIRO. All rights reserved.
 */

package au.csiro.clinsight.utilities;

/**
 * Utility methods for checking preconditions within other methods.
 * <p>
 * Inspired by Google Guava (https://github.com/google/guava).
 *
 * @author John Grimes
 */
public abstract class Preconditions {

  public static <T> T checkNotNull(T argument) {
    if (argument == null) {
      throw new AssertionError("Null precondition detected");
    }
    return argument;
  }

  public static <T> T checkNotNull(T argument, String message) {
    if (argument == null) {
      throw new AssertionError(message);
    }
    return argument;
  }
}
