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
 * Represents comparison prefixes for ordered search parameters (date, number, quantity).
 * <p>
 * FHIR search values can be prefixed with comparison operators like "ge" (greater or equal), "lt"
 * (less than), etc. If no prefix is specified, "eq" (equals) is assumed.
 * <p>
 * This enum handles only prefix extraction. Value validation is the responsibility of the
 * type-specific matchers (DateMatcher, NumberMatcher, etc.).
 *
 * @see <a href="https://hl7.org/fhir/search.html#prefix">FHIR Search Prefixes</a>
 */
public enum SearchPrefix {

  /**
   * Equals - the value matches the search value.
   */
  EQ("eq"),

  /**
   * Not equals - the value does not match the search value.
   */
  NE("ne"),

  /**
   * Greater than - the value is greater than the search value.
   */
  GT("gt"),

  /**
   * Greater or equal - the value is greater than or equal to the search value.
   */
  GE("ge"),

  /**
   * Less than - the value is less than the search value.
   */
  LT("lt"),

  /**
   * Less or equal - the value is less than or equal to the search value.
   */
  LE("le");

  /**
   * Pattern to match a prefix at the start of a search value. The prefix is always 2 lowercase
   * letters. This pattern only matches the prefix, not the value portion.
   */
  private static final Pattern PREFIX_PATTERN =
      Pattern.compile("^(eq|ne|gt|ge|lt|le)", Pattern.CASE_INSENSITIVE);

  @Nonnull
  private final String code;

  SearchPrefix(@Nonnull final String code) {
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
   * Extracts the prefix from a search value.
   * <p>
   * If the value starts with a recognized prefix (e.g., "ge2023-01-15" or "gt0.8"), returns the
   * corresponding prefix. If no prefix is present, returns {@link #EQ} as the default.
   *
   * @param value the search value, possibly prefixed
   * @return the parsed prefix, or {@link #EQ} if no prefix
   */
  @Nonnull
  public static SearchPrefix fromValue(@Nonnull final String value) {
    final Matcher matcher = PREFIX_PATTERN.matcher(value);
    if (matcher.find()) {
      final String prefixCode = matcher.group(1).toLowerCase();
      for (final SearchPrefix prefix : values()) {
        if (prefix.code.equals(prefixCode)) {
          return prefix;
        }
      }
    }
    return EQ;
  }

  /**
   * Removes the prefix from a search value, returning the value portion.
   * <p>
   * If the value has a prefix (e.g., "ge2023-01-15"), returns the value part ("2023-01-15"). If no
   * prefix is present, returns the original value unchanged.
   *
   * @param value the search value, possibly prefixed
   * @return the value portion without the prefix
   */
  @Nonnull
  public static String stripPrefix(@Nonnull final String value) {
    final Matcher matcher = PREFIX_PATTERN.matcher(value);
    if (matcher.find()) {
      return value.substring(matcher.end());
    }
    return value;
  }
}
