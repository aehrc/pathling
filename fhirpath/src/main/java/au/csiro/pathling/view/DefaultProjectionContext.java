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

import au.csiro.pathling.fhirpath.EvaluationContext;
import au.csiro.pathling.fhirpath.FhirPath;
import au.csiro.pathling.fhirpath.collection.Collection;
import au.csiro.pathling.fhirpath.collection.ResourceCollection;
import au.csiro.pathling.fhirpath.function.registry.StaticFunctionRegistry;
import au.csiro.pathling.view.DatasetResult.One;
import java.util.Optional;
import javax.annotation.Nonnull;
import lombok.Value;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.hl7.fhir.r4.model.Enumerations.ResourceType;

@Value
public class DefaultProjectionContext implements ProjectionContext {

  @Nonnull
  EvaluationContext evaluationContext;

  @Nonnull
  @Override
  public Dataset<Row> getDataset() {
    return evaluationContext.getDataset();
  }

  @Nonnull
  @Override
  public One<Collection> evalExpression(@Nonnull final FhirPath<Collection> path) {
    return DatasetResult.pureOne(
        path.apply(evaluationContext.getInputContext(), evaluationContext));
  }

  @Nonnull
  @Override
  public One<ProjectionContext> subContext(
      @Nonnull final FhirPath<Collection> parent,
      final boolean unnest, final boolean withNulls) {
    final One<Collection> newInputContextResult = evalExpression(parent);
    if (unnest) {
      return newInputContextResult
          .map(Collection::getCtx)
          .flatMap(cl -> withNulls
                         ? cl.explode_outer()
                         : cl.explode())
          .map(c -> new DefaultProjectionContext(
              evaluationContext.withInputContext(newInputContextResult.getValue().copyWith(c))));

    } else {
      return newInputContextResult.map(cl ->
          new DefaultProjectionContext(evaluationContext.withInputContext(cl))
      );
    }
  }

  @Nonnull
  public static DefaultProjectionContext of(@Nonnull final ExecutionContext context, @Nonnull final
  ResourceType subjectResource) {
    final Dataset<Row> dataset = context.getDataSource().read(subjectResource);
    final ResourceCollection inputContext = ResourceCollection.build(context.getFhirContext(),
        subjectResource);
    final EvaluationContext evaluationContext = new EvaluationContext(inputContext, inputContext,
        context.getFhirContext(), context.getSpark(), context.getDataSource(), dataset,
        StaticFunctionRegistry.getInstance(), Optional.empty(), Optional.empty());
    return new DefaultProjectionContext(evaluationContext);
  }
}
