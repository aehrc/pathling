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
 * Represents the comparison prefix for date search parameters.
 * <p>
 * FHIR date search values can be prefixed with comparison operators like "ge" (greater or equal),
 * "lt" (less than), etc. If no prefix is specified, "eq" (equals) is assumed.
 *
 * @see <a href="https://hl7.org/fhir/search.html#prefix">FHIR Search Prefixes</a>
 */
public enum DatePrefix {

  /**
   * Equals - the resource range overlaps with the parameter range.
   * <p>
   * Note: This implementation uses overlap semantics rather than strict containment for practical
   * compatibility with common search patterns.
   */
  EQ("eq"),

  /**
   * Not equals - the resource range does not overlap with the parameter range.
   */
  NE("ne"),

  /**
   * Greater than - the resource ends after the parameter.
   */
  GT("gt"),

  /**
   * Greater or equal - the resource starts at or after the parameter start.
   */
  GE("ge"),

  /**
   * Less than - the resource starts before the parameter.
   */
  LT("lt"),

  /**
   * Less or equal - the resource ends at or before the parameter end.
   */
  LE("le");

  /**
   * Pattern to match prefix at the start of a value. The prefix is always 2 lowercase letters
   * followed by a digit (year starts with digit).
   */
  private static final Pattern PREFIX_PATTERN = Pattern.compile("^(eq|ne|gt|ge|lt|le)(\\d.*)$");

  @Nonnull
  private final String code;

  DatePrefix(@Nonnull final String code) {
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
   * If the value starts with a recognized prefix (e.g., "ge2023-01-15"), returns the corresponding
   * prefix. If no prefix is present (e.g., "2023-01-15"), returns {@link #EQ} as the default.
   *
   * @param value the search value, possibly prefixed
   * @return the parsed prefix, or {@link #EQ} if no prefix
   */
  @Nonnull
  public static DatePrefix fromValue(@Nonnull final String value) {
    final Matcher matcher = PREFIX_PATTERN.matcher(value);
    if (matcher.matches()) {
      final String prefixCode = matcher.group(1);
      for (final DatePrefix prefix : values()) {
        if (prefix.code.equals(prefixCode)) {
          return prefix;
        }
      }
    }
    // No prefix found, default to EQ
    return EQ;
  }

  /**
   * Removes the prefix from a search value, returning just the date/time portion.
   * <p>
   * If the value has a prefix (e.g., "ge2023-01-15"), returns the date part ("2023-01-15"). If no
   * prefix is present, returns the original value unchanged.
   *
   * @param value the search value, possibly prefixed
   * @return the date/time portion without the prefix
   */
  @Nonnull
  public static String stripPrefix(@Nonnull final String value) {
    final Matcher matcher = PREFIX_PATTERN.matcher(value);
    if (matcher.matches()) {
      return matcher.group(2);
    }
    // No prefix found, return original value
    return value;
  }
}
