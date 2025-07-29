package au.csiro.pathling.fhirpath.comparison;

import jakarta.annotation.Nonnull;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.functions;

/**
 * An interface that defines comparison operation on columns. The actual implementation and the
 * implemented operation depend on the type of value in the column.
 */
public interface ColumnComparator {

  /**
   * Creates an equality comparison between two columns.
   *
   * @param left the left column
   * @param right the right column
   * @return a column representing the equality comparison
   */
  @Nonnull
  Column equalsTo(@Nonnull Column left, @Nonnull Column right);

  /**
   * Creates an inequality comparison between two columns.
   *
   * @param left the left column
   * @param right the right column
   * @return a column representing the inequality comparison
   */
  @Nonnull
  default Column notEqual(@Nonnull final Column left, @Nonnull final Column right) {
    return functions.not(equalsTo(left, right));
  }

  /**
   * Creates a less-than comparison between two columns.
   *
   * @param left the left column
   * @param right the right column
   * @return a column representing the less-than comparison
   */
  @Nonnull
  Column lessThan(@Nonnull Column left, @Nonnull Column right);

  /**
   * Creates a less-than-or-equal comparison between two columns.
   *
   * @param left the left column
   * @param right the right column
   * @return a column representing the less-than-or-equal comparison
   */
  @Nonnull
  default Column lessThanOrEqual(@Nonnull final Column left, @Nonnull final Column right) {
    return lessThan(left, right).or(equalsTo(left, right));
  }

  /**
   * Creates a greater-than comparison between two columns.
   *
   * @param left the left column
   * @param right the right column
   * @return a column representing the greater-than comparison
   */
  @Nonnull
  Column greaterThan(@Nonnull Column left, @Nonnull Column right);

  /**
   * Creates a greater-than-or-equal comparison between two columns.
   *
   * @param left the left column
   * @param right the right column
   * @return a column representing the greater-than-or-equal comparison
   */
  @Nonnull
  default Column greaterThanOrEqual(@Nonnull final Column left, @Nonnull final Column right) {
    return greaterThan(left, right).or(equalsTo(left, right));
  }
}
