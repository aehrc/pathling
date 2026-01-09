/*
 * Copyright © 2018-2025 Commonwealth Scientific and Industrial Research
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

package au.csiro.pathling.search.filter;

import jakarta.annotation.Nonnull;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Represents the comparison prefix for number search parameters.
 * <p>
 * FHIR number search values can be prefixed with comparison operators like "ge" (greater or
 * equal), "lt" (less than), etc. If no prefix is specified, "eq" (equals) is assumed.
 *
 * @see <a href="https://hl7.org/fhir/search.html#prefix">FHIR Search Prefixes</a>
 * @see <a href="https://hl7.org/fhir/search.html#number">FHIR Number Search</a>
 */
public enum NumberPrefix {

  /**
   * Equals - exact value match.
   */
  EQ("eq"),

  /**
   * Not equals - value does not match.
   */
  NE("ne"),

  /**
   * Greater than - value is greater than the search value.
   */
  GT("gt"),

  /**
   * Greater or equal - value is greater than or equal to the search value.
   */
  GE("ge"),

  /**
   * Less than - value is less than the search value.
   */
  LT("lt"),

  /**
   * Less or equal - value is less than or equal to the search value.
   */
  LE("le");

  /**
   * Pattern to match prefix at the start of a number value. The prefix is 2 lowercase letters
   * followed by an optional sign and digits. Supports integers, decimals, and exponential notation.
   * <p>
   * Examples: "ge0.8", "lt-5", "eq100", "gt1e2", "0.5" (no prefix), "-5.5" (no prefix)
   */
  private static final Pattern PREFIX_PATTERN =
      Pattern.compile("^(eq|ne|gt|ge|lt|le)?(-?[\\d.]+(?:e-?\\d+)?)$", Pattern.CASE_INSENSITIVE);

  @Nonnull
  private final String code;

  NumberPrefix(@Nonnull final String code) {
    this.code = code;
  }

  /**
   * Gets the prefix code as it appears in search values.
   *
   * @return the prefix code (e.g., "ge", "lt")
   */
  @Nonnull
  public String getCode() {
    return code;
  }

  /**
   * Parses the prefix from a search value.
   * <p>
   * If the value starts with a recognized prefix (e.g., "ge0.8"), returns the corresponding
   * prefix. If no prefix is present (e.g., "0.8"), returns {@link #EQ} as the default.
   *
   * @param value the search value, possibly prefixed
   * @return the parsed prefix, or {@link #EQ} if no prefix
   */
  @Nonnull
  public static NumberPrefix fromValue(@Nonnull final String value) {
    final Matcher matcher = PREFIX_PATTERN.matcher(value);
    if (matcher.matches()) {
      final String prefixCode = matcher.group(1);
      if (prefixCode != null) {
        final String lowerPrefix = prefixCode.toLowerCase();
        for (final NumberPrefix prefix : values()) {
          if (prefix.code.equals(lowerPrefix)) {
            return prefix;
          }
        }
      }
    }
    // No prefix found, default to EQ
    return EQ;
  }

  /**
   * Removes the prefix from a search value, returning just the number portion.
   * <p>
   * If the value has a prefix (e.g., "ge0.8"), returns the number part ("0.8"). If no prefix is
   * present, returns the original value unchanged.
   *
   * @param value the search value, possibly prefixed
   * @return the number portion without the prefix
   */
  @Nonnull
  public static String stripPrefix(@Nonnull final String value) {
    final Matcher matcher = PREFIX_PATTERN.matcher(value);
    if (matcher.matches()) {
      return matcher.group(2);
    }
    // No match (shouldn't happen with valid input), return original value
    return value;
  }
}
