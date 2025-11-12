/*
 * Copyright © 2018-2025 Commonwealth Scientific and Industrial Research
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

package au.csiro.pathling.projection;


import static au.csiro.pathling.encoders.ValueFunctions.ifArray;
import static org.apache.spark.sql.functions.array;
import static org.apache.spark.sql.functions.concat;
import static org.apache.spark.sql.functions.isnull;
import static org.apache.spark.sql.functions.when;

import au.csiro.pathling.encoders.ColumnFunctions;
import au.csiro.pathling.fhirpath.collection.Collection;
import jakarta.annotation.Nonnull;
import java.util.List;
import lombok.Value;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.functions;

/**
 * The result of evaluating a projection, which consists of a list of {@link ProjectedColumn}
 * objects and an intermediate {@link Column} representation that is used to produce the final
 * result.
 * <p>
 * These objects can also be combined to produce higher-level projections, such as the combination
 * of sub-selections and unions.
 */
@Value(staticConstructor = "of")
public class ProjectionResult {

  /**
   * A list of results, each of which contains a {@link Collection} and a {@link RequestedColumn}.
   */
  @Nonnull
  List<ProjectedColumn> results;

  /**
   * An array of structs. The struct has a field for each column name in the projection.
   */
  @Nonnull
  Column resultColumn;


  /**
   * Creates a new ProjectionResult with the specified result column, retaining the existing
   *
   * @param newResultColumn The new result column
   * @return A new ProjectionResult with the updated result column
   */
  @Nonnull
  ProjectionResult withResultColumn(@Nonnull final Column newResultColumn) {
    return of(this.results, newResultColumn);
  }


  /**
   * Combine the results of multiple projections into a single result.
   *
   * @param results The results to combine
   * @return The combined result
   */
  @Nonnull
  public static ProjectionResult combine(@Nonnull final List<ProjectionResult> results) {
    return product(results, false);
  }

  /**
   * Combine the results of multiple projections into a single result, with outer join semantics.
   *
   * @param results The results to combine
   * @param outer Whether to use outer join semantics
   * @return The combined result
   */
  @Nonnull
  public static ProjectionResult combine(@Nonnull final List<ProjectionResult> results,
      final boolean outer) {
    return product(results, outer);
  }

  /**
   * Creates a product (Cartesian product) of multiple projection results.
   * <p>
   * This combines results using struct product semantics, where each result is expanded to include
   * all combinations from the inputs.
   * </p>
   *
   * @param results The results to combine via product
   * @param outer Whether to use outer join semantics
   * @return The product of all results
   */
  @Nonnull
  public static ProjectionResult product(@Nonnull final List<ProjectionResult> results,
      final boolean outer) {
    if (results.size() == 1 && !outer) {
      return results.getFirst();
    } else {
      return of(
          results.stream().flatMap(r -> r.getResults().stream()).toList(),
          structProduct(outer,
              results.stream().map(ProjectionResult::getResultColumn).toArray(Column[]::new))
      );
    }
  }

  /**
   * Creates a concatenation (union) of multiple projection results.
   * <p>
   * This combines results by concatenating their arrays, ensuring each result is converted to an
   * array format before concatenation.
   * </p>
   *
   * @param results The results to concatenate
   * @return The concatenated result
   */
  @Nonnull
  public static ProjectionResult concatenate(@Nonnull final List<ProjectionResult> results) {
    if (results.isEmpty()) {
      throw new IllegalArgumentException("Cannot concatenate empty list of results");
    }

    // Process each result to ensure they are all arrays
    final Column[] converted = results.stream()
        .map(ProjectionResult::getResultColumn)
        // When the result is a singular null, convert it to an empty array
        .map(col -> when(isnull(col), array())
            .otherwise(ifArray(col,
                // If the column is an array, return it as is
                c -> c,
                // If the column is a singular value, convert it to an array
                functions::array
            )))
        .toArray(Column[]::new);

    // Concatenate the converted columns
    final Column combinedResult = concat(converted);

    // Use the schema from the first result
    return results.getFirst().withResultColumn(combinedResult);
  }

  /**
   * Creates a struct product column from the given columns.
   *
   * @param outer whether to use outer join semantics
   * @param columns the columns to include in the struct product
   * @return a new struct product column
   */
  @Nonnull
  public static Column structProduct(final boolean outer, @Nonnull final Column... columns) {
    return outer
           ? ColumnFunctions.structProductOuter(columns)
           : ColumnFunctions.structProduct(columns);
  }

}
