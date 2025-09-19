package au.csiro.pathling.fhirpath.comparison;

import jakarta.annotation.Nonnull;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.functions;

/**
 * An interface that defines methods for comparing two {@link Column} objects for equality and
 * inequality.
 *
 * @author Piotr Szul
 */
public interface ColumnEquality {

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
}
