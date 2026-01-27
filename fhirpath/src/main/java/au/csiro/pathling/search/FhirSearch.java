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

package au.csiro.pathling.search;

import jakarta.annotation.Nonnull;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import lombok.Value;

/**
 * Represents a FHIR search query consisting of one or more search criteria. Multiple criteria are
 * combined with AND logic.
 *
 * <p>Note: The resource type is not part of the search query itself, but is passed separately to
 * the executor.
 *
 * @see <a href="https://hl7.org/fhir/search.html">FHIR Search</a>
 */
@Value
public class FhirSearch {

  /** The search criteria. Multiple criteria are combined with AND logic. */
  @Nonnull List<SearchCriterion> criteria;

  /**
   * Creates a new builder for constructing a FhirSearch.
   *
   * @return a new builder
   */
  @Nonnull
  public static Builder builder() {
    return new Builder();
  }

  /**
   * Parses a FHIR search query string into a FhirSearch.
   *
   * <p>The query string should be in standard URL query format without the leading '?'. Values are
   * expected to be URL-encoded per RFC 3986.
   *
   * <p>Multiple values for the same parameter (comma-separated) are combined with OR logic within a
   * single criterion. Repeated parameters create separate criteria combined with AND logic.
   *
   * <p>FHIR escape sequences are supported: {@code \,} for literal comma, {@code \\} for literal
   * backslash.
   *
   * @param queryString the query string (e.g., "gender=male&amp;birthdate=ge1990")
   * @return a FhirSearch representing the query
   * @see <a href="https://hl7.org/fhir/search.html#escaping">FHIR Search Escaping</a>
   */
  @Nonnull
  public static FhirSearch fromQueryString(@Nonnull final String queryString) {
    if (queryString.isEmpty()) {
      return new FhirSearch(List.of());
    }

    final Builder builder = builder();

    for (final String pair : queryString.split("&")) {
      if (!pair.isEmpty()) {
        parsePair(pair, builder);
      }
    }

    return builder.build();
  }

  /**
   * Parses a single parameter=value pair and adds it to the builder.
   *
   * @param pair the parameter pair (e.g., "gender=male" or "family:exact=Smith")
   * @param builder the builder to add the criterion to
   */
  private static void parsePair(@Nonnull final String pair, @Nonnull final Builder builder) {
    final int equalsIndex = pair.indexOf('=');
    if (equalsIndex == -1) {
      // Parameter without value - treat as empty value
      builder.criterion(pair, List.of(""));
    } else {
      final String parameterCode = pair.substring(0, equalsIndex);
      final String encodedValue = pair.substring(equalsIndex + 1);

      // URL decode the value
      final String decodedValue = URLDecoder.decode(encodedValue, StandardCharsets.UTF_8);

      // Split on unescaped commas to get OR values
      final List<String> values = splitOnComma(decodedValue);

      builder.criterion(parameterCode, values);
    }
  }

  /**
   * Splits a value on commas, respecting FHIR backslash escape sequences.
   *
   * <p>Escape rules:
   *
   * <ul>
   *   <li>{@code \,} - literal comma (not a delimiter)
   *   <li>{@code \\} - literal backslash
   * </ul>
   *
   * @param value the decoded value to split
   * @return list of individual values with escapes resolved
   */
  @Nonnull
  static List<String> splitOnComma(@Nonnull final String value) {
    final List<String> result = new ArrayList<>();
    final StringBuilder current = new StringBuilder();
    boolean escaped = false;

    for (final char c : value.toCharArray()) {
      if (escaped) {
        // Previous char was backslash - this char is literal
        current.append(c);
        escaped = false;
      } else if (c == '\\') {
        // Start escape sequence
        escaped = true;
      } else if (c == ',') {
        // Unescaped comma - split here
        result.add(current.toString());
        current.setLength(0);
      } else {
        current.append(c);
      }
    }

    // Add the last segment (result is never empty after this)
    result.add(current.toString());

    // Handle trailing backslash (keep it as literal)
    if (escaped) {
      final int lastIndex = result.size() - 1;
      result.set(lastIndex, result.get(lastIndex) + "\\");
    }

    return result;
  }

  /** Builder for constructing FhirSearch instances. */
  public static class Builder {

    private final List<SearchCriterion> criteria = new ArrayList<>();

    /**
     * Adds a search criterion with the given parameter code and values. The parameter code may
     * include a modifier suffix (e.g., "gender:not" or "family:exact").
     *
     * @param parameterCodeWithModifier the parameter code, optionally with modifier suffix
     * @param values the search values (multiple values = OR logic)
     * @return this builder
     */
    @Nonnull
    public Builder criterion(
        @Nonnull final String parameterCodeWithModifier, @Nonnull final String... values) {
      return criterion(parameterCodeWithModifier, Arrays.asList(values));
    }

    /**
     * Adds a search criterion with the given parameter code and values. The parameter code may
     * include a modifier suffix (e.g., "gender:not" or "family:exact").
     *
     * @param parameterCodeWithModifier the parameter code, optionally with modifier suffix
     * @param values the search values (multiple values = OR logic)
     * @return this builder
     */
    @Nonnull
    public Builder criterion(
        @Nonnull final String parameterCodeWithModifier, @Nonnull final List<String> values) {
      final String[] parts = parseParameterCode(parameterCodeWithModifier);
      criteria.add(SearchCriterion.of(parts[0], parts[1], values));
      return this;
    }

    /**
     * Adds a pre-built search criterion.
     *
     * @param criterion the criterion to add
     * @return this builder
     */
    @Nonnull
    public Builder criterion(@Nonnull final SearchCriterion criterion) {
      criteria.add(criterion);
      return this;
    }

    /**
     * Parses a parameter code that may include a modifier suffix.
     *
     * @param parameterCodeWithModifier the parameter code with optional modifier (e.g.,
     *     "gender:not")
     * @return an array of [parameterCode, modifier] where modifier may be null
     */
    @Nonnull
    private String[] parseParameterCode(@Nonnull final String parameterCodeWithModifier) {
      final int colonIndex = parameterCodeWithModifier.indexOf(':');
      if (colonIndex == -1) {
        return new String[] {parameterCodeWithModifier, null};
      }
      return new String[] {
        parameterCodeWithModifier.substring(0, colonIndex),
        parameterCodeWithModifier.substring(colonIndex + 1)
      };
    }

    /**
     * Builds the FhirSearch instance.
     *
     * @return the constructed FhirSearch
     */
    @Nonnull
    public FhirSearch build() {
      return new FhirSearch(List.copyOf(criteria));
    }
  }
}
