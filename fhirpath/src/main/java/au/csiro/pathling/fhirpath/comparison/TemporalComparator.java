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

package au.csiro.pathling.fhirpath.comparison;

import au.csiro.pathling.sql.misc.HighBoundaryForDateTime;
import au.csiro.pathling.sql.misc.HighBoundaryForTime;
import au.csiro.pathling.sql.misc.LowBoundaryForDateTime;
import au.csiro.pathling.sql.misc.LowBoundaryForTime;
import jakarta.annotation.Nonnull;
import java.util.function.BinaryOperator;
import lombok.AllArgsConstructor;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.functions;

/**
 * Comparator for Temporal (DateTime and Time) values that handles partial dates by comparing
 * ranges.
 *
 * <p>Since FHIR DateTime/Time values can be partial (e.g., just year or year-month), comparisons
 * need to consider the range of possible values for each DateTime/Time.
 */
@AllArgsConstructor(access = lombok.AccessLevel.PRIVATE)
public class TemporalComparator implements ColumnComparator, ElementWiseEquality {

  private static final TemporalComparator DATE_TIME_COMPARATOR =
      new TemporalComparator(
          LowBoundaryForDateTime.FUNCTION_NAME, HighBoundaryForDateTime.FUNCTION_NAME);

  private static final TemporalComparator TIME_COMPARATOR =
      new TemporalComparator(LowBoundaryForTime.FUNCTION_NAME, HighBoundaryForTime.FUNCTION_NAME);

  /** The names of the UDFs to compute the low and high boundaries for a temporal value. */
  @Nonnull private final String lowBoundaryUDF;

  /** The names of the UDFs to compute the low and high boundaries for a temporal value. */
  @Nonnull private final String highBoundaryUDF;

  /** Record to hold the low and high boundary columns for a dateTime value. */
  private record Bounds(@Nonnull Column low, @Nonnull Column high) {}

  /**
   * Returns a TemporalComparator for DateTime values.
   *
   * @return A TemporalComparator that can compare DateTime values.
   */
  @Nonnull
  public static TemporalComparator forDateTime() {
    return DATE_TIME_COMPARATOR;
  }

  /**
   * Returns a TemporalComparator for Time values.
   *
   * @return A TemporalComparator that can compare Time values.
   */
  @Nonnull
  public static TemporalComparator forTime() {
    return TIME_COMPARATOR;
  }

  /**
   * Gets the low and high boundary columns for a dateTime column.
   *
   * @param column The dateTime column to get boundaries for.
   * @return A Bounds record containing the low and high boundary columns.
   */
  @Nonnull
  private Bounds getBounds(@Nonnull final Column column) {
    return new Bounds(
        functions.callUDF(lowBoundaryUDF, column), functions.callUDF(highBoundaryUDF, column));
  }

  @Nonnull
  @Override
  public Column equalsTo(@Nonnull final Column left, @Nonnull final Column right) {
    return implementWithSql(left, right, Column::equalTo);
  }

  @Nonnull
  @Override
  public Column lessThan(@Nonnull final Column left, @Nonnull final Column right) {
    return implementWithSql(left, right, Column::lt);
  }

  @Nonnull
  @Override
  public Column lessThanOrEqual(@Nonnull final Column left, @Nonnull final Column right) {
    return implementWithSql(left, right, Column::leq);
  }

  @Nonnull
  @Override
  public Column greaterThan(@Nonnull final Column left, @Nonnull final Column right) {
    return implementWithSql(left, right, Column::gt);
  }

  @Nonnull
  @Override
  public Column greaterThanOrEqual(@Nonnull final Column left, @Nonnull final Column right) {
    return implementWithSql(left, right, Column::geq);
  }

  @Nonnull
  private Column implementWithSql(
      @Nonnull final Column left,
      @Nonnull final Column right,
      @Nonnull final BinaryOperator<Column> comparator) {
    final Bounds leftBounds = getBounds(left);
    final Bounds rightBounds = getBounds(right);

    // if canCompare apply the comparator to the low bound (either one is fine)
    // else return null
    return functions
        .when(
            canCompare(leftBounds, rightBounds), comparator.apply(leftBounds.low, rightBounds.low))
        .otherwise(functions.lit(null));
  }

  @Nonnull
  private static Column canCompare(@Nonnull final Bounds left, @Nonnull final Bounds right) {

    // can compare when:
    // - either on overlapping
    // - or equal

    // This assumes that there is not case of partial overlaps, but I cannot see how that be
    // possible.
    // This is because:
    // 1. At any given precision, all the instants are disjointed (considering their lower and upper
    // bounds)
    // 2. Any more precise instant is fully contained within the less precise instant (considering
    // their lower and upper bounds)
    return left.high
        .lt(right.low)
        .or(right.high.lt(left.low))
        .or(right.low.equalTo(left.low).and(left.high.equalTo(right.high)));
  }
}
