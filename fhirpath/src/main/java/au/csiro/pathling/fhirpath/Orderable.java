/*
 * Copyright 2023 Commonwealth Scientific and Industrial Research
 * Organisation (CSIRO) ABN 41 687 119 230.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package au.csiro.pathling.fhirpath;

import static au.csiro.pathling.utilities.Preconditions.checkState;

import jakarta.annotation.Nonnull;
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
   * @throws IllegalStateException when the path cannot be ordered
   */
  @Nonnull
  Dataset<Row> getOrderedDataset();

  /**
   * Returns the column that can be used to order the dataset. This is exposed to allow multi-column
   * ordering with unstable orderBy().
   *
   * @return A {@link Dataset}
   * @throws IllegalStateException when the path cannot be ordered
   */
  @Nonnull
  Column getOrderingColumn();


  /**
   * @throws IllegalStateException if the path cannot be ordered
   */
  default void checkHasOrder() {
    checkState(hasOrder(), "Orderable path expected");
  }

}
