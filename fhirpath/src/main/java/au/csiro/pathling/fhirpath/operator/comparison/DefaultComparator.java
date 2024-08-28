package au.csiro.pathling.fhirpath.operator.comparison;

import org.apache.spark.sql.Column;
import org.jetbrains.annotations.NotNull;

/**
 * An implementation of {@link ColumnComparator} that uses the standard Spark SQL comparison
 * operators.
 */
public class DefaultComparator implements ColumnComparator {

  @Override
  @NotNull
  public Column equalsTo(@NotNull final Column left, @NotNull final Column right) {
    return left.equalTo(right);
  }

  @Override
  @NotNull
  public Column notEqual(@NotNull final Column left, @NotNull final Column right) {
    return left.notEqual(right);
  }

  @Override
  @NotNull
  public Column lessThan(@NotNull final Column left, @NotNull final Column right) {
    return left.lt(right);
  }

  @Override
  @NotNull
  public Column lessThanOrEqual(@NotNull final Column left, @NotNull final Column right) {
    return left.leq(right);
  }

  @Override
  @NotNull
  public Column greaterThan(@NotNull final Column left, @NotNull final Column right) {
    return left.gt(right);
  }

  @Override
  @NotNull
  public Column greaterThanOrEqual(@NotNull final Column left, @NotNull final Column right) {
    return left.geq(right);
  }

}
