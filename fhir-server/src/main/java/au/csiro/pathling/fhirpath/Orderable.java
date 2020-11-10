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
 * Describes a path that which elements can deterministically ordered.
 *
 * @author Piotr Szul
 */

public interface Orderable {

  /**
   * The Spark SQL type of the ordering column
   */
  static DataType ORDERING_COLUMN_TYPE = DataTypes.createArrayType(DataTypes.IntegerType);
  /**
   * The typed NULL value for the odering column.
   */
  static Column ORDERING_NULL_VALUE = functions.lit(null).cast(ORDERING_COLUMN_TYPE);


  /**
   * Returns an indicator of whether this path can be ordered in deterministic order.
   *
   * @return {@code true} if this path can be ordered in deterministic order
   */
  boolean hasOrder();

  /**
   * Returns ordered {@link Dataset} that can be used to evaluate this path against data.
   *
   * @return A {@link Dataset}
   * @throws {@link java.lang.IllegalStateException} when the path cannot be ordered.
   */
  @Nonnull
  Dataset<Row> getOrderedDataset();

  /**
   * Returns the column that can be used to order the dataset
   *
   * @return A {@link Dataset}
   * @throws {@link java.lang.IllegalStateException} when the path cannot be ordered.
   */
  @Nonnull
  Column getOrderingColumn();


  /**
   * Fails if the path cannot be ordered.
   *
   * @throws {@link java.lang.IllegalStateException}
   */
  default void checkHasOrder() {
    Preconditions.checkState(hasOrder(), "Orderable path expected");
  }


}
