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

import static au.csiro.pathling.utilities.Strings.randomAlias;

import au.csiro.pathling.fhirpath.EvaluationContext;
import au.csiro.pathling.fhirpath.FhirPath;
import au.csiro.pathling.fhirpath.collection.Collection;
import au.csiro.pathling.fhirpath.collection.ResourceCollection;
import au.csiro.pathling.fhirpath.function.registry.StaticFunctionRegistry;
import au.csiro.pathling.view.DatasetResult.One;
import java.util.Optional;
import javax.annotation.Nonnull;
import lombok.Value;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.functions;
import org.hl7.fhir.r4.model.Enumerations.ResourceType;

@Value
public class DefaultProjectionContext implements ProjectionContext {

  EvaluationContext evaluationContext;

  @Nonnull
  @Override
  public Column evalExpression(@Nonnull final FhirPath<Collection> path,
      final boolean singularise) {
    return singularise
           ? evaluateInternal(path).getSingleton()
           : evaluateInternal(path).getColumn();
  }

  @Nonnull
  private One<Column> unnestColumn(@Nonnull final Column column, final boolean keepNulls) {

    final String materializedColumnName = randomAlias();

    return new One<>(functions.col(materializedColumnName),
        Optional.of(ds -> {
          final Dataset<Row> newDs = ds.withColumn(
              materializedColumnName, column);
          return !keepNulls
                 ? newDs.filter(newDs.col(materializedColumnName).isNotNull())
                 : newDs;
        }));
  }

  @Nonnull
  @Override
  public Dataset<Row> getDataset() {
    return evaluationContext.getDataset();
  }

  @Nonnull
  @Override
  public Collection evaluateInternal(@Nonnull final FhirPath<Collection> path) {
    return path.apply(evaluationContext.getInputContext(), evaluationContext);
  }

  @Nonnull
  @Override
  public Pair<ProjectionContext, DatasetResult<Column>> subContext(
      @Nonnull final FhirPath<Collection> parent,
      final boolean unnest, final boolean withNulls) {
    final Collection newInputContext = evaluateInternal(parent);

    if (unnest) {
      final One<Column> unnestedColumn = unnestColumn(
          withNulls
          ? newInputContext.getCtx().explode_outer().getValue()
          : newInputContext.getCtx().explode().getValue(),
          withNulls);
      return Pair.of(new DefaultProjectionContext(
              evaluationContext.withInputContext(newInputContext.copyWith(unnestedColumn.getValue())))
          , unnestedColumn.asTransform());
    } else {
      return Pair.of(new DefaultProjectionContext(
          evaluationContext.withInputContext(newInputContext)), DatasetResult.empty());
    }
  }

  @Nonnull
  public static DefaultProjectionContext of(@Nonnull final ViewContext context, @Nonnull final
  ResourceType subjectResource) {
    final Dataset<Row> dataset = context.getDataSource().read(subjectResource);
    final ResourceCollection inputContext = ResourceCollection.build(context.getFhirContext(),
        dataset,
        subjectResource);
    final EvaluationContext evaluationContext = new EvaluationContext(inputContext, inputContext,
        context.getFhirContext(), context.getSpark(), context.getDataSource(), dataset,
        StaticFunctionRegistry.getInstance(), Optional.empty(), Optional.empty());
    return new DefaultProjectionContext(evaluationContext);
  }
}