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

import static org.apache.spark.sql.functions.exists;
import static org.apache.spark.sql.functions.lit;
import static org.apache.spark.sql.functions.lower;

import au.csiro.pathling.fhirpath.column.ColumnRepresentation;
import jakarta.annotation.Nonnull;
import java.util.List;
import org.apache.spark.sql.Column;

/**
 * Builds SparkSQL filter expressions for string search parameters.
 * <p>
 * Implements FHIR string search semantics:
 * <ul>
 *   <li>Case-insensitive matching</li>
 *   <li>Matches if field equals OR starts with the search value</li>
 *   <li>Multiple values combined with OR logic</li>
 * </ul>
 *
 * @see <a href="https://hl7.org/fhir/search.html#string">String Search</a>
 */
public class StringSearchFilter implements SearchFilter {

  @Override
  @Nonnull
  public Column buildFilter(@Nonnull final ColumnRepresentation valueColumn,
      @Nonnull final List<String> searchValues) {
    if (searchValues.isEmpty()) {
      throw new IllegalArgumentException("At least one search value is required");
    }

    // Combine multiple values with OR logic
    return searchValues.stream()
        .map(value -> buildSingleValueFilter(valueColumn, value))
        .reduce(Column::or)
        .orElseThrow(() -> new IllegalStateException("Failed to build filter expression"));
  }

  /**
   * Builds a filter expression for a single string value.
   * <p>
   * Uses case-insensitive starts-with matching per FHIR specification:
   * <ul>
   *   <li>{@code family=Smith} matches "Smith", "SMITH", "Smithson"</li>
   *   <li>{@code family=smi} matches "Smith", "SMITH", "Smithson"</li>
   * </ul>
   *
   * @param valueColumn the ColumnRepresentation containing the value to filter on
   * @param searchValue the search string value
   * @return a SparkSQL Column expression for this single value
   */
  @Nonnull
  private Column buildSingleValueFilter(@Nonnull final ColumnRepresentation valueColumn,
      @Nonnull final String searchValue) {
    // Case-insensitive starts-with matching (FHIR string search default)
    final Column searchLower = lit(searchValue.toLowerCase());
    return valueColumn.vectorize(
        arr -> exists(arr, elem -> lower(elem).startsWith(searchLower)),
        scalar -> lower(scalar).startsWith(searchLower)
    ).getValue();
  }
}
