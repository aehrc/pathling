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

import au.csiro.pathling.fhirpath.FhirPathNumber;
import jakarta.annotation.Nonnull;
import java.math.BigDecimal;
import org.apache.spark.sql.Column;

/**
 * Utility class for numeric matching logic used by both NumberMatcher and QuantityMatcher.
 * <p>
 * Provides reusable methods for comparing numeric values with range-based semantics (eq/ne)
 * and exact value semantics (gt/ge/lt/le), based on FHIR search specification.
 *
 * @author Generated for code reuse
 */
public final class NumericMatchingSupport {

  private NumericMatchingSupport() {
    // Utility class - do not instantiate
  }

  /**
   * Matches using range semantics based on significant figures.
   * <p>
   * For resource values (treated as points with infinite precision):
   * <ul>
   *   <li>EQ: value is within [lower, upper)</li>
   *   <li>NE: value is outside [lower, upper)</li>
   * </ul>
   * <p>
   * The range is determined by the precision of the search value. For example, searching
   * for "100" (3 significant figures) creates a range [99.5, 100.5).
   *
   * @param valueColumn the column to match against
   * @param numericValue the numeric search value (without prefix)
   * @param prefix the search prefix (EQ or NE)
   * @return a Column expression for the match condition
   */
  @Nonnull
  public static Column matchWithRangeSemantics(
      @Nonnull final Column valueColumn,
      @Nonnull final String numericValue,
      @Nonnull final SearchPrefix prefix) {
    final FhirPathNumber searchNumber = FhirPathNumber.parse(numericValue);
    final BigDecimal lowerBoundary = searchNumber.getLowerBoundary();
    final BigDecimal upperBoundary = searchNumber.getUpperBoundary();

    return switch (prefix) {
      case EQ -> valueColumn.geq(lit(lowerBoundary)).and(valueColumn.lt(lit(upperBoundary)));
      case NE -> valueColumn.lt(lit(lowerBoundary)).or(valueColumn.geq(lit(upperBoundary)));
      case GT, GE, LT, LE -> throw new IllegalArgumentException(
          "Range semantics not supported for prefix: " + prefix);
    };
  }

  /**
   * Matches using exact value semantics (infinite precision).
   * <p>
   * Comparison prefixes ignore the implicit range and treat both the search value and resource
   * value as having arbitrary precision.
   *
   * @param valueColumn the column to match against
   * @param numericValue the numeric search value (without prefix)
   * @param prefix the search prefix (GT, GE, LT, or LE)
   * @return a Column expression for the match condition
   */
  @Nonnull
  public static Column matchWithExactSemantics(
      @Nonnull final Column valueColumn,
      @Nonnull final String numericValue,
      @Nonnull final SearchPrefix prefix) {
    final BigDecimal searchNumber = new BigDecimal(numericValue);

    return switch (prefix) {
      case GT -> valueColumn.gt(lit(searchNumber));
      case GE -> valueColumn.geq(lit(searchNumber));
      case LT -> valueColumn.lt(lit(searchNumber));
      case LE -> valueColumn.leq(lit(searchNumber));
      case EQ, NE -> throw new IllegalArgumentException(
          "Exact semantics requires a comparison prefix, got: " + prefix);
    };
  }
}
