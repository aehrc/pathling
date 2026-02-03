/*
 * Copyright © 2018-2026 Commonwealth Scientific and Industrial Research
 * Organisation (CSIRO) ABN 41 687 119 230.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package au.csiro.pathling.utilities;

import static java.util.function.Predicate.not;

import jakarta.annotation.Nonnull;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.Function;
import java.util.stream.Stream;
import org.jetbrains.annotations.Unmodifiable;

/**
 * Utility class containing some methods for string wrangling.
 *
 * @author John Grimes
 */
public abstract class Strings {

  private Strings() {}

  /**
   * Removes surrounding single quotes from a string.
   *
   * @param value a String surrounded by single quotes
   * @return the unquoted String
   */
  @Nonnull
  public static String unSingleQuote(@Nonnull final String value) {
    return value.replaceAll("(?:^')|(?:'$)", "");
  }

  /**
   * Removes surrounding tick quotes from a string.
   *
   * @param value a String surrounded by tick quotes
   * @return the unquoted String
   */
  @Nonnull
  public static String unTickQuote(@Nonnull final String value) {
    return value.replaceAll("(?:^`)|(?:`$)", "");
  }

  /**
   * Generates a short random string for use as a column alias.
   *
   * @return a short, random String for use as a column alias
   */
  @Nonnull
  public static String randomAlias() {
    // Use ThreadLocalRandom to avoid creating new Random instances and to handle the edge case
    // where nextInt() returns Integer.MIN_VALUE (which would overflow with Math.abs()).
    final int randomNumber = ThreadLocalRandom.current().nextInt(0, Integer.MAX_VALUE);
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
  public static <T> @Unmodifiable List<T> parseCsvList(
      @Nonnull final String csvList, final @Nonnull Function<String, T> converter) {
    return Stream.of(csvList.split(","))
        .map(String::trim)
        .filter(not(String::isEmpty))
        .map(converter)
        .toList();
  }
}
