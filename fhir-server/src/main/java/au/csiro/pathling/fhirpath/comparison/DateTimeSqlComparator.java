package au.csiro.pathling.fhirpath.comparison;

import static org.apache.spark.sql.functions.callUDF;

import au.csiro.pathling.fhirpath.Comparable;
import au.csiro.pathling.fhirpath.Comparable.ComparisonOperation;
import au.csiro.pathling.fhirpath.Comparable.SqlComparator;
import au.csiro.pathling.sql.dates.datetime.DateTimeEqualsFunction;
import au.csiro.pathling.sql.dates.datetime.DateTimeGreaterThanFunction;
import au.csiro.pathling.sql.dates.datetime.DateTimeGreaterThanOrEqualToFunction;
import au.csiro.pathling.sql.dates.datetime.DateTimeLessThanFunction;
import au.csiro.pathling.sql.dates.datetime.DateTimeLessThanOrEqualToFunction;
import java.util.function.Function;
import javax.annotation.Nonnull;
import org.apache.spark.sql.Column;

/**
 * Implementation of comparator for the DateTime type.
 *
 * @author Piotr Szul
 */
public class DateTimeSqlComparator implements SqlComparator {

  private static final DateTimeSqlComparator INSTANCE = new DateTimeSqlComparator();

  @Override
  public Column equalsTo(@Nonnull final Column left, @Nonnull final Column right) {
    return callUDF(DateTimeEqualsFunction.FUNCTION_NAME, left, right);
  }

  @Override
  public Column lessThan(@Nonnull final Column left, @Nonnull final Column right) {
    return callUDF(DateTimeLessThanFunction.FUNCTION_NAME, left, right);
  }

  @Override
  public Column lessThanOrEqual(final Column left, final Column right) {
    return callUDF(DateTimeLessThanOrEqualToFunction.FUNCTION_NAME, left, right);
  }

  @Override
  public Column greaterThan(final Column left, final Column right) {
    return callUDF(DateTimeGreaterThanFunction.FUNCTION_NAME, left, right);
  }

  @Override
  public Column greaterThanOrEqual(final Column left, final Column right) {
    return callUDF(DateTimeGreaterThanOrEqualToFunction.FUNCTION_NAME, left, right);
  }

  /**
   * Builds a comparison function for date and date/time like paths.
   *
   * @param source the path to build the comparison function for
   * @param operation the {@link ComparisonOperation} that should be built
   * @return a new {@link Function}
   */
  @Nonnull
  public static Function<Comparable, Column> buildComparison(@Nonnull final Comparable source,
      @Nonnull final ComparisonOperation operation) {
    return Comparable.buildComparison(source, operation, INSTANCE);
  }
}
