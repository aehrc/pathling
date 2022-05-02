/*
 * Copyright © 2018-2022, Commonwealth Scientific and Industrial Research
 * Organisation (CSIRO) ABN 41 687 119 230. Licensed under the CSIRO Open Source
 * Software Licence Agreement.
 */

package au.csiro.pathling.sql;

import javax.annotation.Nonnull;
import org.apache.spark.sql.Column;

/**
 * Pathling specific SQL functions.
 */
public interface SqlExpressions {

  /**
   * A function that removes all fields starting with '_' (underscore) from struct values. Other
   * types of values are not affected.
   *
   * @param col the column to apply the function to
   * @return the column transformed by the function
   */

  @Nonnull
  static Column pruneSyntheticFields(@Nonnull final Column col) {
    return new Column(new PruneSyntheticFields(col.expr()));
  }
}
