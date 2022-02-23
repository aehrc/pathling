package au.csiro.pathling.sql;

import javax.annotation.Nonnull;
import org.apache.spark.sql.Column;

/**
 * Pathling specific SQL functions.
 */
public interface PathlingFunctions {

  /**
   * A function that removes all fields starting with '_' (underscore) from struct values. Other
   * types of values are not affected.
   *
   * @param col the column to apply the function to.
   * @return the column tranformed by the function.
   */

  @Nonnull
  static Column pruneSyntheticFields(@Nonnull final Column col) {
    return new Column(new PruneSyntheticFields(col.expr()));
  }
}
