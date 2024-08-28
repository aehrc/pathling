package au.csiro.pathling.fhirpath.operator.comparison;

import org.apache.spark.sql.Column;
import org.apache.spark.sql.functions;
import org.jetbrains.annotations.NotNull;

/**
 * An interface that defines comparison operation on columns. The actual implementation and the
 * implemented operation depend on the type of value in the column.
 *
 * @author Piotr Szul
 */
public interface ColumnComparator {

  @NotNull
  Column equalsTo(@NotNull Column left, @NotNull Column right);

  @NotNull
  default Column notEqual(@NotNull final Column left, @NotNull final Column right) {
    return functions.not(equalsTo(left, right));
  }

  @NotNull
  Column lessThan(@NotNull Column left, @NotNull Column right);

  @NotNull
  default Column lessThanOrEqual(@NotNull final Column left, @NotNull final Column right) {
    return lessThan(left, right).or(equalsTo(left, right));
  }

  @NotNull
  Column greaterThan(@NotNull Column left, @NotNull Column right);

  @NotNull
  default Column greaterThanOrEqual(@NotNull final Column left, @NotNull final Column right) {
    return greaterThan(left, right).or(equalsTo(left, right));
  }

}
