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

import au.csiro.pathling.fhirpath.FhirPath;
import au.csiro.pathling.fhirpath.collection.Collection;
import au.csiro.pathling.view.DatasetResult.One;
import javax.annotation.Nonnull;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

public interface ProjectionContext {

  @Nonnull
  Dataset<Row> getDataset();

  @Nonnull
  default DatasetResult<CollectionResult> evaluate(@Nonnull final Selection selection) {
    return selection.evaluate(this);
  }

  /**
   * Creates a new execution context that is a sub-context of this one, with the given path.
   *
   * @param parent the path to the sub-context
   * @param unnest whether to unnest the sub-context
   * @return the new sub-context
   */
  @Nonnull
  One<ProjectionContext> subContext(@Nonnull final FhirPath<Collection> parent,
      boolean unnest, boolean withNulls);

  @Nonnull
  default One<ProjectionContext> subContext(
      @Nonnull final FhirPath<Collection> parent,
      final boolean unnest) {
    return subContext(parent, unnest, false);
  }

  @Nonnull
  default One<ProjectionContext> subContext(
      @Nonnull final FhirPath<Collection> parent) {
    return subContext(parent, false);
  }


  /**
   * Evaluates the given FHIRPath path and returns the result as a column.
   *
   * @param path the path to evaluate
   * @return the result as a column
   */
  @Nonnull
  One<Collection> evalExpression(@Nonnull final FhirPath<Collection> path);

}
