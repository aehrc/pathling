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
import java.util.Optional;

/**
 * Represents a parsed FHIR quantity search value.
 *
 * <p>FHIR quantity search values can have the following formats:
 *
 * <ul>
 *   <li>{@code [prefix][number]} - value only, matches any unit
 *   <li>{@code [prefix][number]|[system]|[code]} - value with exact system and code
 *   <li>{@code [prefix][number]||[code]} - value with code only (any system)
 * </ul>
 *
 * <p>Note: The format {@code [number]|[system]|} (system without code) is not supported.
 *
 * <p>The prefix (eq, ne, gt, ge, lt, le) is handled separately by {@link SearchPrefix}.
 *
 * <p>System and code use {@link Optional} semantics:
 *
 * <ul>
 *   <li>{@code Optional.empty()} - no constraint
 *   <li>{@code Optional.of("value")} - must match exact value
 * </ul>
 *
 * <p>See SPEC_CLARIFICATIONS.md in the project root for format validation details and rationale.
 *
 * @see <a href="https://hl7.org/fhir/search.html#quantity">FHIR Quantity Search</a>
 */
public class QuantitySearchValue {

  @Nonnull private final SearchPrefix prefix;

  @Nonnull private final String numericValue;

  @Nonnull private final Optional<String> system;

  @Nonnull private final Optional<String> code;

  private QuantitySearchValue(
      @Nonnull final SearchPrefix prefix,
      @Nonnull final String numericValue,
      @Nonnull final Optional<String> system,
      @Nonnull final Optional<String> code) {
    this.prefix = prefix;
    this.numericValue = numericValue;
    this.system = system;
    this.code = code;
  }

  /**
   * Parses a FHIR quantity search value string.
   *
   * <p>Valid formats:
   *
   * <ul>
   *   <li>{@code "5.4"} - value only (any system, any code)
   *   <li>{@code "gt5.4"} - value with prefix
   *   <li>{@code "5.4|http://unitsofmeasure.org|mg"} - value with system and code
   *   <li>{@code "5.4||mg"} - value with code only (any system)
   * </ul>
   *
   * <p>Invalid format:
   *
   * <ul>
   *   <li>{@code "5.4|http://unitsofmeasure.org|"} - system without code is not supported
   * </ul>
   *
   * @param searchValue the search value string
   * @return the parsed quantity search value
   * @throws IllegalArgumentException if the format is invalid
   */
  @Nonnull
  public static QuantitySearchValue parse(@Nonnull final String searchValue) {
    final SearchPrefix.ParsedValue parsedPrefix = SearchPrefix.parseValue(searchValue);
    final SearchPrefix prefix = parsedPrefix.prefix;
    final String valueWithoutPrefix = parsedPrefix.value;

    // Split by pipe - should have 1 or 3 parts
    // Use -1 limit to keep trailing empty strings
    final String[] parts = valueWithoutPrefix.split("\\|", -1);

    if (parts.length == 1) {
      // Value only: [number]
      return new QuantitySearchValue(prefix, parts[0], Optional.empty(), Optional.empty());
    } else if (parts.length == 3) {
      final String systemPart = parts[1];
      final String codePart = parts[2];

      // Validate: non-empty system with empty code is not supported
      if (!systemPart.isEmpty() && codePart.isEmpty()) {
        throw new IllegalArgumentException(
            "Invalid quantity search value format: "
                + searchValue
                + ". System without code is not supported. "
                + "Use [number]|[system]|[code] or [number]||[code]");
      }

      // Normalize: empty strings become Optional.empty() (no constraint)
      final Optional<String> system =
          systemPart.isEmpty() ? Optional.empty() : Optional.of(systemPart);
      final Optional<String> code = codePart.isEmpty() ? Optional.empty() : Optional.of(codePart);

      return new QuantitySearchValue(prefix, parts[0], system, code);
    } else {
      throw new IllegalArgumentException(
          "Invalid quantity search value format: "
              + searchValue
              + ". Expected [number] or [number]|[system]|[code]");
    }
  }

  /**
   * Gets the search prefix.
   *
   * @return the search prefix (defaults to EQ if not specified)
   */
  @Nonnull
  public SearchPrefix getPrefix() {
    return prefix;
  }

  /**
   * Gets the numeric portion of the search value.
   *
   * @return the numeric value string
   */
  @Nonnull
  public String getNumericValue() {
    return numericValue;
  }

  /**
   * Gets the system constraint.
   *
   * @return the system URI to match, or empty for no constraint (any system)
   */
  @Nonnull
  public Optional<String> getSystem() {
    return system;
  }

  /**
   * Gets the code constraint.
   *
   * @return the code to match, or empty for no constraint (any code)
   */
  @Nonnull
  public Optional<String> getCode() {
    return code;
  }

  /**
   * Checks if this is a value-only search (no system/code constraints).
   *
   * @return true if only the numeric value is specified
   */
  public boolean isValueOnly() {
    return system.isEmpty() && code.isEmpty();
  }

  @Override
  public String toString() {
    final StringBuilder sb = new StringBuilder("QuantitySearchValue{");
    sb.append("prefix=").append(prefix);
    sb.append(", numericValue='").append(numericValue).append('\'');
    system.ifPresent(s -> sb.append(", system='").append(s).append('\''));
    code.ifPresent(c -> sb.append(", code='").append(c).append('\''));
    sb.append('}');
    return sb.toString();
  }
}
