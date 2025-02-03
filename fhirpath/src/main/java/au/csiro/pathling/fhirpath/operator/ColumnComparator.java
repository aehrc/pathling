package au.csiro.pathling.fhirpath.operator;

import org.apache.spark.sql.Column;
import org.apache.spark.sql.functions;

/**
 * An interface that defines comparison operation on columns. The actual implementation and the
 * implemented operation depend on the type of value in the column.
 */
public interface ColumnComparator {

  Column equalsTo(Column left, Column right);

  default Column notEqual(final Column left, final Column right) {
    return functions.not(equalsTo(left, right));
  }

  Column lessThan(Column left, Column right);

  default Column lessThanOrEqual(final Column left, final Column right) {
    return lessThan(left, right).or(equalsTo(left, right));
  }

  Column greaterThan(Column left, Column right);

  default Column greaterThanOrEqual(final Column left, final Column right) {
    return greaterThan(left, right).or(equalsTo(left, right));
  }
}
