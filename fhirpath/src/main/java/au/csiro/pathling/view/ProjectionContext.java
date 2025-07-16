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

import static java.util.stream.Collectors.toMap;

import au.csiro.pathling.fhirpath.FhirPath;
import au.csiro.pathling.fhirpath.collection.Collection;
import au.csiro.pathling.fhirpath.execution.FhirpathEvaluator;
import au.csiro.pathling.fhirpath.function.registry.StaticFunctionRegistry;
import au.csiro.pathling.views.ConstantDeclaration;
import jakarta.annotation.Nonnull;
import java.util.List;
import java.util.Map;
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
  FhirpathEvaluator executor;

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

  /**
   * Evaluates the given FHIRPath path and returns the result as a column.
   *
   * @param path the path to evaluate
   * @return the result as a column
   */
  @Nonnull
  public Collection evalExpression(@Nonnull final FhirPath path) {
    return executor.evaluate(path, inputContext);
  }

  @Nonnull
  public static ProjectionContext of(@Nonnull final ExecutionContext context,
      @Nonnull final ResourceType subjectResource,
      @Nonnull final List<ConstantDeclaration> constants) {
    // Create a map of variables from the provided constants.
    final Map<String, Collection> variables = constants.stream()
        .collect(toMap(ConstantDeclaration::getName,
            ProjectionContext::getCollectionForConstantValue));

    // Create a new FhirPathExecutor.
    final FhirpathEvaluator executor = context.getFhirpathEvaluatorFactory()
        .create(subjectResource, StaticFunctionRegistry.getInstance(), variables);

    // Return a new ProjectionContext with the executor and the default input context.
    return new ProjectionContext(executor, executor.createDefaultInputContext());
  }

  @Nonnull
  private static Collection getCollectionForConstantValue(@Nonnull final ConstantDeclaration c) {
    return Collection.fromValue(c.getValue());
  }
}
