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
import au.csiro.pathling.fhirpath.execution.FhirPathExecutor;
import au.csiro.pathling.fhirpath.execution.SingleFhirPathExecutor;
import au.csiro.pathling.fhirpath.function.registry.StaticFunctionRegistry;
import au.csiro.pathling.view.DatasetResult.One;
import javax.annotation.Nonnull;
import lombok.Value;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.hl7.fhir.r4.model.Enumerations.ResourceType;

/**
 * Dependencies and logic relating to the traversal of FHIRPath expressions.
 *
 * @author Piotr Szul
 */
@Value
public class ProjectionContext {

  @Nonnull
  FhirPathExecutor executor;

  @Nonnull
  Collection inputContext;

  @Nonnull
  public Dataset<Row> getDataset() {
    return executor.createInitialDataset();
  }

  @Nonnull
  public ProjectionContext withInputContext(@Nonnull final Collection inputContext) {
    return new ProjectionContext(executor, inputContext);
  }

  @Nonnull
  public DatasetResult<CollectionResult> evaluate(@Nonnull final Selection selection) {
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
  public One<ProjectionContext> subContext(
      @Nonnull final FhirPath parent,
      final boolean unnest, final boolean withNulls) {
    final One<Collection> newInputContextResult = evalExpression(parent);
    if (unnest) {
      return newInputContextResult
          .map(Collection::getColumn)
          .flatMap(cl -> withNulls
                         ? cl.explodeOuter()
                         : cl.explode())
          .map(c -> withInputContext(newInputContextResult.getValue().copyWith(c)));
    } else {
      return newInputContextResult.map(this::withInputContext);
    }
  }

  @Nonnull
  One<ProjectionContext> subContext(@Nonnull final FhirPath parent, final boolean unnest) {
    return subContext(parent, unnest, false);
  }

  @Nonnull
  One<ProjectionContext> subContext(@Nonnull final FhirPath parent) {
    return subContext(parent, false);
  }

  /**
   * Evaluates the given FHIRPath path and returns the result as a column.
   *
   * @param path the path to evaluate
   * @return the result as a column
   */
  @Nonnull
  public One<Collection> evalExpression(@Nonnull final FhirPath path) {
    return DatasetResult.pureOne(executor.evaluate(path, inputContext));
  }

  @Nonnull
  public static ProjectionContext of(@Nonnull final ExecutionContext context,
      @Nonnull final ResourceType subjectResource) {
    final FhirPathExecutor executor = new SingleFhirPathExecutor(
        subjectResource,
        context.getFhirContext(),
        new StaticFunctionRegistry(),
        context.getDataSource());
    return new ProjectionContext(executor, executor.createDefaultInputContext());
  }

}
