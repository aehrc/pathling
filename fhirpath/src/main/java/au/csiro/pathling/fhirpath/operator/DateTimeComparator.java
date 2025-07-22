package au.csiro.pathling.fhirpath.operator;

import au.csiro.pathling.fhirpath.comparison.ColumnComparator;
import au.csiro.pathling.sql.misc.HighBoundaryForDateTime;
import au.csiro.pathling.sql.misc.LowBoundaryForDateTime;
import jakarta.annotation.Nonnull;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.functions;

public class DateTimeComparator implements ColumnComparator {

  /**
   * Record to hold the low and high boundary columns for a dateTime value.
   */
  private record Bounds(@Nonnull Column low, @Nonnull Column high) {}

  /**
   * Gets the low and high boundary columns for a dateTime column.
   *
   * @param column The dateTime column to get boundaries for.
   * @return A Bounds record containing the low and high boundary columns.
   */
  @Nonnull
  private static Bounds getBounds(@Nonnull final Column column) {
    return new Bounds(
        functions.callUDF(LowBoundaryForDateTime.FUNCTION_NAME, column),
        functions.callUDF(HighBoundaryForDateTime.FUNCTION_NAME, column)
    );
  }

  @Nonnull
  @Override
  public Column equalsTo(@Nonnull final Column left, @Nonnull final Column right) {
    final Bounds leftBounds = getBounds(left);
    final Bounds rightBounds = getBounds(right);

    // true if ranges are identical (same low and high boundaries)
    // false if ranges don't overlap at all
    // null if ranges overlap but are not identical (uncertain)
    return functions.when(leftBounds.low.equalTo(rightBounds.low).and(leftBounds.high.equalTo(rightBounds.high)), functions.lit(true))
        .when(leftBounds.high.lt(rightBounds.low).or(leftBounds.low.gt(rightBounds.high)), functions.lit(false))
        .otherwise(functions.lit(null));
  }

  @Nonnull
  @Override
  public Column lessThan(@Nonnull final Column left, @Nonnull final Column right) {
    final Bounds leftBounds = getBounds(left);
    final Bounds rightBounds = getBounds(right);

    // true if left.highBoundary < right.lowBoundary (no overlap, left < right)  
    // false if left.lowBoundary >= right.highBoundary (no overlap, left >= right)
    // null if ranges overlap (uncertain)
    return functions.when(leftBounds.high.lt(rightBounds.low), functions.lit(true))
        .when(leftBounds.low.geq(rightBounds.high), functions.lit(false))
        .otherwise(functions.lit(null));
  }

  @Nonnull
  @Override
  public Column lessThanOrEqual(@Nonnull final Column left, @Nonnull final Column right) {
    final Column lessThanResult = lessThan(left, right);
    final Column equalsResult = equalsTo(left, right);

    // true if lessThan is true OR equals is true
    // false if lessThan is false (meaning left > right with no overlap)
    // null if lessThan is null (meaning ranges overlap)
    return functions.when(lessThanResult.isNotNull().and(lessThanResult), functions.lit(true))
        .when(equalsResult, functions.lit(true))
        .when(lessThanResult.isNotNull(), functions.lit(false))
        .otherwise(functions.lit(null));
  }

  @Nonnull
  @Override
  public Column greaterThan(@Nonnull final Column left, @Nonnull final Column right) {
    final Bounds leftBounds = getBounds(left);
    final Bounds rightBounds = getBounds(right);

    // true if left.lowBoundary > right.highBoundary (no overlap, left > right)
    // false if left.highBoundary <= right.lowBoundary (no overlap, left <= right)  
    // null if ranges overlap (uncertain)
    return functions.when(leftBounds.low.gt(rightBounds.high), functions.lit(true))
        .when(leftBounds.high.leq(rightBounds.low), functions.lit(false))
        .otherwise(functions.lit(null));
  }

  @Nonnull
  @Override
  public Column greaterThanOrEqual(@Nonnull final Column left, @Nonnull final Column right) {
    final Column greaterThanResult = greaterThan(left, right);
    final Column equalsResult = equalsTo(left, right);

    // true if greaterThan is true OR equals is true
    // false if greaterThan is false (meaning left < right with no overlap)
    // null if greaterThan is null (meaning ranges overlap)
    return functions.when(greaterThanResult.isNotNull().and(greaterThanResult), functions.lit(true))
        .when(equalsResult, functions.lit(true))
        .when(greaterThanResult.isNotNull(), functions.lit(false))
        .otherwise(functions.lit(null));
  }
}
