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
import au.csiro.pathling.fhirpath.collection.ResourceCollection;
import au.csiro.pathling.fhirpath.function.registry.StaticFunctionRegistry;
import au.csiro.pathling.io.source.DataSource;
import ca.uhn.fhir.context.FhirContext;
import java.util.Optional;
import javax.annotation.Nonnull;
import lombok.Value;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.hl7.fhir.r4.model.Enumerations.ResourceType;

@Value
public class View {

  ResourceType subjectResource;
  Selection selection;

  @Value
  public static class Context {

    SparkSession spark;
    FhirContext fhirContext;
    DataSource dataSource;
  }

  public Dataset<Row> evaluate(@Nonnull final Context context) {
    final DefaultProjectionContext projectionContext = newContext(context);
    final DatasetView result = selection.evaluate(projectionContext);
    return result.apply(
        projectionContext.getEvaluationContext().getDataset());
  }

  public void printTree() {
    System.out.println("select:");
    selection.printTree(1);
  }

  DefaultProjectionContext newContext(@Nonnull final Context context) {
    final Dataset<Row> dataset = context.getDataSource().read(subjectResource);
    final ResourceCollection inputContext = ResourceCollection.build(context.getFhirContext(),
        dataset,
        subjectResource);
    final EvaluationContext evaluationContext = new EvaluationContext(inputContext, inputContext,
        context.getFhirContext(), context.getSpark(), dataset, StaticFunctionRegistry.getInstance(),
        Optional.empty(), Optional.empty());
    return new DefaultProjectionContext(evaluationContext);
  }
}
