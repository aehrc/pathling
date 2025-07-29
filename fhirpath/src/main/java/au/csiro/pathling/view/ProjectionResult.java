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

package au.csiro.pathling.view;


import au.csiro.pathling.encoders.ColumnFunctions;
import au.csiro.pathling.fhirpath.collection.Collection;
import jakarta.annotation.Nonnull;
import java.util.List;
import lombok.Value;
import org.apache.spark.sql.Column;

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
   * Combine the results of multiple projections into a single result.
   *
   * @param results The results to combine
   * @return The combined result
   */
  @Nonnull
  public static ProjectionResult combine(@Nonnull final List<ProjectionResult> results) {
    return combine(results, false);
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
    if (results.size() == 1 && !outer) {
      return results.get(0);
    } else {
      return of(
          results.stream().flatMap(r -> r.getResults().stream()).toList(),
          structProduct(outer,
              results.stream().map(ProjectionResult::getResultColumn).toArray(Column[]::new))
      );
    }
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
