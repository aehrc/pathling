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

import static org.apache.spark.sql.functions.coalesce;
import static org.apache.spark.sql.functions.exists;
import static org.apache.spark.sql.functions.lit;
import static org.apache.spark.sql.functions.not;

import au.csiro.pathling.fhirpath.column.ColumnRepresentation;
import jakarta.annotation.Nonnull;
import java.util.List;
import org.apache.spark.sql.Column;

/**
 * Builds SparkSQL filter expressions from search parameter values.
 *
 * <p>This class handles the common logic for all search filters:
 *
 * <ul>
 *   <li>Combining multiple search values with OR logic
 *   <li>Vectorizing the filter to handle both array and scalar columns
 *   <li>Optionally negating the entire filter (for :not modifier)
 * </ul>
 *
 * <p>The actual matching logic is delegated to an {@link ElementMatcher}.
 */
public class SearchFilter {

  @Nonnull private final ElementMatcher matcher;

  private final boolean negated;

  /**
   * Creates a non-negated search filter with the given matcher.
   *
   * @param matcher the element matcher
   */
  public SearchFilter(@Nonnull final ElementMatcher matcher) {
    this(matcher, false);
  }

  /**
   * Creates a search filter with the given matcher and negation flag.
   *
   * @param matcher the element matcher
   * @param negated if true, the filter result will be negated (for :not modifier)
   */
  public SearchFilter(@Nonnull final ElementMatcher matcher, final boolean negated) {
    this.matcher = matcher;
    this.negated = negated;
  }

  /**
   * Builds a SparkSQL Column expression that filters rows based on the search values.
   *
   * <p>If this filter is negated (for :not modifier), the entire expression is negated. Per FHIR
   * specification, negation applies to the SET of values, not each individual element. This means
   * :not returns resources that do NOT have ANY matching value, including resources with no value
   * at all.
   *
   * @param valueColumn the ColumnRepresentation containing the values to filter on
   * @param searchValues the search values to match (multiple values = OR logic)
   * @return a SparkSQL Column expression that evaluates to true for matching rows
   */
  @Nonnull
  public Column buildFilter(
      @Nonnull final ColumnRepresentation valueColumn, @Nonnull final List<String> searchValues) {
    if (searchValues.isEmpty()) {
      throw new IllegalArgumentException("At least one search value is required");
    }

    final Column positiveFilter =
        searchValues.stream()
            .map(
                value ->
                    valueColumn
                        .vectorize(
                            arr -> exists(arr, elem -> matcher.match(elem, value)),
                            // For scalar values, coalesce null to false so NOT(null) -> NOT(false)
                            // -> true
                            // This ensures :not matches resources with no value
                            scalar -> coalesce(matcher.match(scalar, value), lit(false)))
                        .getValue())
            .reduce(Column::or)
            .orElseThrow(() -> new IllegalStateException("Failed to build filter expression"));

    return negated ? not(positiveFilter) : positiveFilter;
  }
}
