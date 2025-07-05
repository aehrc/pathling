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

package au.csiro.pathling.sql;

import jakarta.annotation.Nonnull;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.functions;

/**
 * Pathling-specific SQL functions that extend Spark SQL functionality.
 * <p>
 * This interface provides utility functions for working with Spark SQL columns in the context of
 * FHIR data processing. These functions handle common operations like pruning annotations, safely
 * concatenating maps, and collecting maps during aggregation.
 */
public interface SqlFunctions {

  /**
   * Removes all fields starting with '_' (underscore) from struct values.
   * <p>
   * This function is used to clean up internal/synthetic fields from FHIR resources before
   * presenting them to users. Fields that don't start with underscore are preserved. Non-struct
   * values are not affected by this function.
   *
   * @param col The column containing struct values to prune
   * @return A new column with underscore-prefixed fields removed from structs
   */
  @Nonnull
  static Column prune_annotations(@Nonnull final Column col) {
    return new Column(new PruneSyntheticFields(col.expr()));
  }

  /**
   * Safely concatenates two map columns, handling null values appropriately.
   * <p>
   * This function:
   * <ul>
   *   <li>Returns the right map if the left map is null</li>
   *   <li>Returns the left map if the right map is null</li>
   *   <li>Concatenates both maps if neither is null</li>
   * </ul>
   * <p>
   * When maps are concatenated and contain the same keys, the values from the right map
   * take precedence (overwrite values from the left map).
   *
   * @param left The left map column
   * @param right The right map column
   * @return A new column containing the concatenated map
   */
  @Nonnull
  static Column ns_map_concat(@Nonnull final Column left, @Nonnull final Column right) {
    return functions.when(left.isNull(), right)
        .when(right.isNull(), left)
        .otherwise(functions.map_concat(left, right));
  }

  /**
   * Aggregates multiple map columns into a single map by concatenating them.
   * <p>
   * This function is designed to be used in Spark SQL aggregation operations to combine multiple
   * maps into a single map. It handles null values appropriately and ensures that when maps contain
   * the same keys, the last value wins.
   * <p>
   * Note: To work this function requires the Spark SQL configuration
   * {@code spark.sql.mapKeyDedupPolicy=LAST_WIN}.
   *
   * @param mapColumn The map column to aggregate
   * @return A new column containing the aggregated map
   */
  @Nonnull
  static Column collect_map(@Nonnull final Column mapColumn) {
    // TODO: try to implement this more efficiently and in a way
    // that does not require:
    // .config("spark.sql.mapKeyDedupPolicy", "LAST_WIN")
    return functions.reduce(
        functions.collect_list(mapColumn),
        functions.any_value(mapColumn),
        (acc, elem) -> functions.when(acc.isNull(), elem).otherwise(ns_map_concat(acc, elem))
    );
  }
}
