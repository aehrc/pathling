package au.csiro.pathling.fhirpath.operator;

import au.csiro.pathling.fhirpath.comparison.ColumnComparator;
import au.csiro.pathling.sql.misc.HighBoundaryForDateTime;
import au.csiro.pathling.sql.misc.LowBoundaryForDateTime;
import jakarta.annotation.Nonnull;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.functions;

public class DateTimeComparator implements ColumnComparator {

  @Nonnull
  @Override
  public Column equalsTo(@Nonnull final Column left, @Nonnull final Column right) {
    return functions.callUDF(LowBoundaryForDateTime.FUNCTION_NAME, left)
        .equalTo(functions.callUDF(LowBoundaryForDateTime.FUNCTION_NAME, right)).and(
            functions.callUDF(HighBoundaryForDateTime.FUNCTION_NAME, left)
                .equalTo(functions.callUDF(HighBoundaryForDateTime.FUNCTION_NAME, right)));
  }

  @Nonnull
  @Override
  public Column lessThan(@Nonnull final Column left, @Nonnull final Column right) {
    final Column leftHigh = functions.callUDF(HighBoundaryForDateTime.FUNCTION_NAME, left);
    final Column rightLow = functions.callUDF(LowBoundaryForDateTime.FUNCTION_NAME, right);
    final Column leftLow = functions.callUDF(LowBoundaryForDateTime.FUNCTION_NAME, left);
    final Column rightHigh = functions.callUDF(HighBoundaryForDateTime.FUNCTION_NAME, right);

    // true if left.highBoundary < right.lowBoundary (no overlap, left < right)
    // false if left.lowBoundary >= right.highBoundary (no overlap, left >= right)
    // null if ranges overlap (uncertain)
    return functions.when(leftHigh.lt(rightLow), functions.lit(true))
        .when(leftLow.geq(rightHigh), functions.lit(false))
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
    final Column leftLow = functions.callUDF(LowBoundaryForDateTime.FUNCTION_NAME, left);
    final Column rightHigh = functions.callUDF(HighBoundaryForDateTime.FUNCTION_NAME, right);
    final Column leftHigh = functions.callUDF(HighBoundaryForDateTime.FUNCTION_NAME, left);
    final Column rightLow = functions.callUDF(LowBoundaryForDateTime.FUNCTION_NAME, right);

    // true if left.lowBoundary > right.highBoundary (no overlap, left > right)
    // false if left.highBoundary <= right.lowBoundary (no overlap, left <= right)
    // null if ranges overlap (uncertain)
    return functions.when(leftLow.gt(rightHigh), functions.lit(true))
        .when(leftHigh.leq(rightLow), functions.lit(false))
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
