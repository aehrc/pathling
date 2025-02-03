package au.csiro.pathling.fhirpath.operator;

import jakarta.annotation.Nonnull;
import org.apache.spark.sql.Column;

/**
 * An implementation of {@link ColumnComparator} that uses the standard Spark SQL comparison
 * operators.
 */
public class DefaultComparator implements ColumnComparator {

  @Override
  public Column equalsTo(@Nonnull final Column left, @Nonnull final Column right) {
    return left.equalTo(right);
  }

  @Override
  public Column notEqual(@Nonnull final Column left, @Nonnull final Column right) {
    return left.notEqual(right);
  }

  @Override
  public Column lessThan(@Nonnull final Column left, @Nonnull final Column right) {
    return left.lt(right);
  }

  @Override
  public Column lessThanOrEqual(@Nonnull final Column left, @Nonnull final Column right) {
    return left.leq(right);
  }

  @Override
  public Column greaterThan(@Nonnull final Column left, @Nonnull final Column right) {
    return left.gt(right);
  }

  @Override
  public Column greaterThanOrEqual(@Nonnull final Column left, @Nonnull final Column right) {
    return left.geq(right);
  }
}
