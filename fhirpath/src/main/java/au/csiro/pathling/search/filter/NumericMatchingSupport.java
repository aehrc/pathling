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

package au.csiro.pathling.search.filter;

import static org.apache.spark.sql.functions.lit;
import static org.apache.spark.sql.functions.when;

import au.csiro.pathling.fhirpath.FhirPathNumber;
import au.csiro.pathling.schema.annotation.kinds.DecimalNumericKind;
import au.csiro.pathling.utilities.SystemDecimal;
import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import java.math.BigDecimal;
import java.math.RoundingMode;
import org.apache.spark.sql.Column;

/**
 * Utility class for numeric matching logic used by both NumberMatcher and QuantityMatcher.
 *
 * <p>Provides reusable methods for comparing numeric values with range-based semantics (eq/ne) and
 * exact value semantics (gt/ge/lt/le), based on FHIR search specification.
 *
 * @author Generated for code reuse
 */
public final class NumericMatchingSupport {

  private NumericMatchingSupport() {
    // Utility class - do not instantiate
  }

  /**
   * Returns the number of a search value as the engine's System.Decimal holds it: rounded half up
   * to its scale where it has more fractional digits, as every number the engine computes with is
   * (owner, 2026-11-12, decision 153).
   *
   * @param numericValue the numeric search value (without prefix)
   * @return the number, or null where it is outside the range the System.Decimal holds, so that it
   *     takes no part in a match
   */
  @Nullable
  public static BigDecimal systemValue(@Nonnull final String numericValue) {
    final BigDecimal value = DecimalNumericKind.decodeValue(new BigDecimal(numericValue.trim()));
    return value == null ? null : nonNegativeScale(value);
  }

  /**
   * Matches using range semantics based on significant figures.
   *
   * <p>For resource values (treated as points with infinite precision):
   *
   * <ul>
   *   <li>EQ: value is within [lower, upper)
   *   <li>NE: value is outside [lower, upper)
   * </ul>
   *
   * <p>The range is determined by the precision of the search value. For example, searching for
   * "100" (3 significant figures) creates a range [99.5, 100.5).
   *
   * <p>The search value is first taken as the engine's System.Decimal holds it ({@link
   * #systemValue}), and a value that is rounded has the range of its rounded value, so that {@code
   * eq0.0000002} searches as {@code eq0.000000}. A value outside the range the System.Decimal holds
   * matches nothing, under {@code ne} as under {@code eq}, because a comparison with a value that
   * is not available is not made.
   *
   * <p>The value column is a System.Decimal, so it holds only multiples of the System.Decimal's
   * smallest step. A boundary with more fractional digits than that is therefore raised to the next
   * such multiple, which keeps the same values in the range and keeps the comparison at the
   * System.Decimal's scale, so that Spark does not widen the column to a scale at which its largest
   * values no longer fit.
   *
   * @param valueColumn the System.Decimal column to match against
   * @param numericValue the numeric search value (without prefix)
   * @param prefix the search prefix (EQ or NE)
   * @return a Column expression for the match condition
   */
  @Nonnull
  public static Column matchWithRangeSemantics(
      @Nonnull final Column valueColumn,
      @Nonnull final String numericValue,
      @Nonnull final SearchPrefix prefix) {
    final BigDecimal value = systemValue(numericValue);
    if (value == null) {
      return lit(false);
    }
    // A value that is not rounded keeps its own text, whose form gives its significant figures.
    final FhirPathNumber searchNumber =
        FhirPathNumber.parse(
            new BigDecimal(numericValue.trim()).scale() > SystemDecimal.SCALE
                ? value.toPlainString()
                : numericValue);
    final BigDecimal lowerBoundary = onTheSystemScale(searchNumber.getLowerBoundary());
    final BigDecimal upperBoundary = onTheSystemScale(searchNumber.getUpperBoundary());
    // A boundary beyond the range the System.Decimal holds bounds nothing, as every System.Decimal
    // is within it, and could not be written as one. The test it stands for is then a constant,
    // null where the value is, as the comparison would have been.
    final Column present = valueColumn.isNotNull();
    final boolean lowerBounds = withinRange(lowerBoundary);
    final boolean upperBounds = withinRange(upperBoundary);

    return switch (prefix) {
      case EQ ->
          (lowerBounds ? valueColumn.geq(lit(lowerBoundary)) : when(present, lit(true)))
              .and(upperBounds ? valueColumn.lt(lit(upperBoundary)) : when(present, lit(true)));
      case NE ->
          (lowerBounds ? valueColumn.lt(lit(lowerBoundary)) : when(present, lit(false)))
              .or(upperBounds ? valueColumn.geq(lit(upperBoundary)) : when(present, lit(false)));
      case GT, GE, LT, LE ->
          throw new IllegalArgumentException("Range semantics not supported for prefix: " + prefix);
    };
  }

  /**
   * Raises a boundary with more fractional digits than the System.Decimal holds to the next of its
   * values. A System.Decimal is at least such a boundary exactly where it is at least the raised
   * one, and below it exactly where it is below the raised one.
   */
  @Nonnull
  private static BigDecimal onTheSystemScale(@Nonnull final BigDecimal boundary) {
    return boundary.scale() > SystemDecimal.SCALE
        ? boundary.setScale(SystemDecimal.SCALE, RoundingMode.CEILING)
        : nonNegativeScale(boundary);
  }

  /** Whether a boundary is a value the System.Decimal holds. */
  private static boolean withinRange(@Nonnull final BigDecimal boundary) {
    return DecimalNumericKind.decodeValue(boundary) != null;
  }

  /**
   * Gives a number a scale that is not negative, such as {@code 1E+3} as {@code 1000}, as Spark
   * cannot otherwise take it as a decimal literal under its default settings.
   */
  @Nonnull
  private static BigDecimal nonNegativeScale(@Nonnull final BigDecimal number) {
    return number.scale() < 0 ? number.setScale(0) : number;
  }

  /**
   * Matches using exact value semantics (infinite precision).
   *
   * <p>Comparison prefixes ignore the implicit range. The search value is compared as the engine's
   * System.Decimal holds it ({@link #systemValue}), and one outside its range matches nothing.
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
    final BigDecimal searchNumber = systemValue(numericValue);
    if (searchNumber == null) {
      return lit(false);
    }

    return switch (prefix) {
      case GT -> valueColumn.gt(lit(searchNumber));
      case GE -> valueColumn.geq(lit(searchNumber));
      case LT -> valueColumn.lt(lit(searchNumber));
      case LE -> valueColumn.leq(lit(searchNumber));
      case EQ, NE ->
          throw new IllegalArgumentException(
              "Exact semantics requires a comparison prefix, got: " + prefix);
    };
  }
}
