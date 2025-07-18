package au.csiro.pathling.fhirpath.operator;

import au.csiro.pathling.errors.UnsupportedFhirPathFeatureError;
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
    throw new UnsupportedFhirPathFeatureError(
        "Less than comparison is not supported for Date type");
  }

  @Nonnull
  @Override
  public Column lessThanOrEqual(@Nonnull final Column left, @Nonnull final Column right) {
    throw new UnsupportedFhirPathFeatureError(
        "Less than or equal comparison is not supported for Date type");
  }

  @Nonnull
  @Override
  public Column greaterThan(@Nonnull final Column left, @Nonnull final Column right) {
    throw new UnsupportedFhirPathFeatureError(
        "Greater than comparison is not supported for Date type");
  }

  @Nonnull
  @Override
  public Column greaterThanOrEqual(@Nonnull final Column left, @Nonnull final Column right) {
    throw new UnsupportedFhirPathFeatureError(
        "Greater than or equal comparison is not supported for Date type");
  }
}
