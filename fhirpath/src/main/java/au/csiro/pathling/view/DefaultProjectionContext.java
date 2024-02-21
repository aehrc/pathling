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
import au.csiro.pathling.fhirpath.execution.FhirpathExecutor;
import au.csiro.pathling.fhirpath.execution.SingleFhirpathExecutor;
import au.csiro.pathling.fhirpath.function.registry.StaticFunctionRegistry;
import au.csiro.pathling.view.DatasetResult.One;
import javax.annotation.Nonnull;
import lombok.Value;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.hl7.fhir.r4.model.Enumerations.ResourceType;

@Value
public class DefaultProjectionContext implements ProjectionContext {

  @Nonnull
  FhirpathExecutor executor;

  @Nonnull
  Collection inputContext;

  @Nonnull
  @Override
  public Dataset<Row> getDataset() {
    return executor.createInitialDataset();
  }

  @Nonnull
  @Override
  public One<Collection> evalExpression(@Nonnull final FhirPath path) {
    return DatasetResult.pureOne(executor.evaluate(path, inputContext));
  }

  @Nonnull
  @Override
  public One<ProjectionContext> subContext(
      @Nonnull final FhirPath parent,
      final boolean unnest, final boolean withNulls) {
    final One<Collection> newInputContextResult = evalExpression(parent);
    if (unnest) {
      return newInputContextResult
          .map(Collection::getCtx)
          .flatMap(cl -> withNulls
                         ? cl.explodeOuter()
                         : cl.explode())
          .map(c -> withInputContext(newInputContextResult.getValue().copyWith(c)));
    } else {
      return newInputContextResult.map(this::withInputContext);
    }
  }

  @Override
  @Nonnull
  public DefaultProjectionContext withInputContext(@Nonnull final Collection inputContext) {
    return new DefaultProjectionContext(executor, inputContext);
  }

  @Nonnull
  public static DefaultProjectionContext of(@Nonnull final ExecutionContext context, @Nonnull final
  ResourceType subjectResource) {
    final FhirpathExecutor executor = new SingleFhirpathExecutor(
        subjectResource,
        context.getFhirContext(),
        new StaticFunctionRegistry(),
        context.getDataSource());
    return new DefaultProjectionContext(executor, executor.createDefaultInputContext());
  }
}
