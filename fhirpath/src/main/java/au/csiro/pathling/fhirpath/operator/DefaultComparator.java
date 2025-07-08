package au.csiro.pathling.fhirpath.operator;

import jakarta.annotation.Nonnull;
import org.apache.spark.sql.Column;

/**
 * An implementation of {@link ColumnComparator} that uses the standard Spark SQL comparison
 * operators.
 */
public class DefaultComparator implements ColumnComparator {

  @Nonnull
  @Override
  public Column equalsTo(@Nonnull final Column left, @Nonnull final Column right) {
    return left.equalTo(right);
  }

  @Nonnull
  @Override
  public Column notEqual(@Nonnull final Column left, @Nonnull final Column right) {
    return left.notEqual(right);
  }

  @Nonnull
  @Override
  public Column lessThan(@Nonnull final Column left, @Nonnull final Column right) {
    return left.lt(right);
  }

  @Nonnull
  @Override
  public Column lessThanOrEqual(@Nonnull final Column left, @Nonnull final Column right) {
    return left.leq(right);
  }

  @Nonnull
  @Override
  public Column greaterThan(@Nonnull final Column left, @Nonnull final Column right) {
    return left.gt(right);
  }

  @Nonnull
  @Override
  public Column greaterThanOrEqual(@Nonnull final Column left, @Nonnull final Column right) {
    return left.geq(right);
  }
}
