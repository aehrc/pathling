/*
 * Copyright © 2018-2020, Commonwealth Scientific and Industrial Research
 * Organisation (CSIRO) ABN 41 687 119 230. Licensed under the CSIRO Open Source
 * Software Licence Agreement.
 */

package au.csiro.pathling.fhirpath;

import au.csiro.pathling.utilities.Preconditions;
import javax.annotation.Nonnull;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;


/**
 * Describes a path for which elements can deterministically ordered.
 *
 * @author Piotr Szul
 */

public interface Orderable {

  /**
   * The Spark SQL type of the ordering column.
   */
  DataType ORDERING_COLUMN_TYPE = DataTypes.createArrayType(DataTypes.IntegerType);

  /**
   * The typed null value for the ordering column.
   */
  Column ORDERING_NULL_VALUE = functions.lit(null).cast(ORDERING_COLUMN_TYPE);


  /**
   * Returns an indicator of whether this path can be ordered in a deterministic way.
   *
   * @return {@code true} if this path can be ordered
   */
  boolean hasOrder();

  /**
   * Returns an ordered {@link Dataset} that can be used to evaluate this path against data.
   *
   * @return A {@link Dataset}
   * @throws {@link java.lang.IllegalStateException} when the path cannot be ordered.
   */
  @Nonnull
  Dataset<Row> getOrderedDataset();

  /**
   * Returns the column that can be used to order the dataset. This is exposed to allow multi-column
   * ordering with unstable orderBy().
   *
   * @return A {@link Dataset}
   * @throws {@link java.lang.IllegalStateException} when the path cannot be ordered.
   */
  @Nonnull
  Column getOrderingColumn();


  /**
   * @throws java.lang.IllegalStateException if the path cannot be ordered
   */
  default void checkHasOrder() {
    Preconditions.checkState(hasOrder(), "Orderable path expected");
  }

}
