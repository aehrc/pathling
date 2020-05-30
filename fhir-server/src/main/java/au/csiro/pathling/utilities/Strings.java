/*
 * Copyright © 2018-2020, Commonwealth Scientific and Industrial Research
 * Organisation (CSIRO) ABN 41 687 119 230. Licensed under the CSIRO Open Source
 * Software Licence Agreement.
 */

package au.csiro.pathling.utilities;

import javax.annotation.Nonnull;

/**
 * Utility class containing some methods for string wrangling.
 *
 * @author John Grimes
 */
public abstract class Strings {

  /**
   * @param value A String value
   * @return The string surrounded by parentheses
   */
  @Nonnull
  public static String parentheses(@Nonnull final String value) {
    return "(" + value + ")";
  }

  /**
   * @param value A String surrounded by single quotes
   * @return The unquoted String
   */
  @Nonnull
  public static String unSingleQuote(@Nonnull final String value) {
    return value.replaceAll("^'|'$", "");
  }

}
