/*
 * Copyright © 2018-2022, Commonwealth Scientific and Industrial Research
 * Organisation (CSIRO) ABN 41 687 119 230. Licensed under the CSIRO Open Source
 * Software Licence Agreement.
 */

package au.csiro.pathling.utilities;

import static java.util.function.Predicate.not;

import jakarta.annotation.Nonnull;
import java.util.List;
import java.util.Random;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Utility class containing some methods for string wrangling.
 *
 * @author John Grimes
 */
public abstract class Strings {

  /**
   * @param value a String surrounded by single quotes
   * @return the unquoted String
   */
  @Nonnull
  public static String unSingleQuote(@Nonnull final String value) {
    return value.replaceAll("^'|'$", "");
  }

  /**
   * @return a short, random String for use as a column alias
   */
  @Nonnull
  public static String randomAlias() {
    final int randomNumber = Math.abs(new Random().nextInt());
    return "@" + Integer.toString(randomNumber, Character.MAX_RADIX);
  }

  /**
   * Leniently parses a list of coma separated values. Trims the strings and filters out empty
   * values.
   *
   * @param csvList a coma separated list of equivalence codes
   * @param converter a function that converts single value string to the desired type T
   * @param <T> the type of elements to produce.
   * @return the list of converted values of type T.
   */
  @Nonnull
  public static <T> List<T> parseCsvList(@Nonnull final String csvList, final @Nonnull
  Function<String, T> converter) {
    return Stream.of(csvList.split(",")).map(String::trim).filter(not(String::isEmpty))
        .map(converter).collect(
            Collectors.toList());
  }

}
