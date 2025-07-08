package au.csiro.pathling.fhirpath.operator;

import jakarta.annotation.Nonnull;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.functions;

/**
 * An interface that defines comparison operation on columns. The actual implementation and the
 * implemented operation depend on the type of value in the column.
 */
public interface ColumnComparator {

  @Nonnull
  Column equalsTo(@Nonnull Column left, @Nonnull Column right);

  @Nonnull
  default Column notEqual(@Nonnull final Column left, @Nonnull final Column right) {
    return functions.not(equalsTo(left, right));
  }

  @Nonnull
  Column lessThan(@Nonnull Column left, @Nonnull Column right);

  @Nonnull
  default Column lessThanOrEqual(@Nonnull final Column left, @Nonnull final Column right) {
    return lessThan(left, right).or(equalsTo(left, right));
  }

  @Nonnull
  Column greaterThan(@Nonnull Column left, @Nonnull Column right);

  @Nonnull
  default Column greaterThanOrEqual(@Nonnull final Column left, @Nonnull final Column right) {
    return greaterThan(left, right).or(equalsTo(left, right));
  }
}
