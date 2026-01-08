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

import static org.apache.spark.sql.functions.lit;

import au.csiro.pathling.search.TokenSearchValue;
import jakarta.annotation.Nonnull;
import java.util.List;
import org.apache.spark.sql.Column;

/**
 * Builds SparkSQL filter expressions for token search parameters.
 * <p>
 * Currently supports simple code matching (e.g., Patient.gender). Support for CodeableConcept and
 * Coding types will be added in future iterations.
 *
 * @see <a href="https://hl7.org/fhir/search.html#token">Token Search</a>
 */
public class TokenSearchFilter implements SearchFilter {

  @Override
  @Nonnull
  public Column buildFilter(@Nonnull final Column valueColumn,
      @Nonnull final List<String> searchValues) {
    // Parse all values and build filter
    final List<TokenSearchValue> parsedValues = searchValues.stream()
        .map(TokenSearchValue::parse)
        .toList();

    return buildFilterFromParsedValues(valueColumn, parsedValues);
  }

  /**
   * Builds a filter expression from parsed token values.
   *
   * @param valueColumn the Spark Column containing the values to filter on
   * @param parsedValues the parsed token search values
   * @return a SparkSQL Column expression that evaluates to true for matching rows
   */
  @Nonnull
  public Column buildFilterFromParsedValues(@Nonnull final Column valueColumn,
      @Nonnull final List<TokenSearchValue> parsedValues) {
    if (parsedValues.isEmpty()) {
      throw new IllegalArgumentException("At least one search value is required");
    }

    // Combine multiple values with OR logic
    return parsedValues.stream()
        .map(value -> buildSingleValueFilter(valueColumn, value))
        .reduce(Column::or)
        .orElseThrow(() -> new IllegalStateException("Failed to build filter expression"));
  }

  /**
   * Builds a filter expression for a single token value.
   * <p>
   * For simple code types (like Patient.gender):
   * <ul>
   *   <li>{@code gender=male} → {@code valueColumn.equalTo("male")}</li>
   * </ul>
   *
   * @param valueColumn the Spark Column containing the value to filter on
   * @param searchValue the parsed token search value
   * @return a SparkSQL Column expression for this single value
   */
  @Nonnull
  private Column buildSingleValueFilter(@Nonnull final Column valueColumn,
      @Nonnull final TokenSearchValue searchValue) {
    // For simple code types (no system), just match the code
    if (searchValue.getCode() != null) {
      return valueColumn.equalTo(lit(searchValue.getCode()));
    }

    // System-only match is not supported for simple code types
    throw new IllegalArgumentException(
        "Token search with only a system is not supported for simple code types");
  }
}
