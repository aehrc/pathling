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

package au.csiro.pathling.query.view.runner;

import static au.csiro.pathling.utilities.Preconditions.check;
import static java.util.stream.Collectors.toMap;

import au.csiro.pathling.errors.InvalidUserInputError;
import au.csiro.pathling.fhirpath.FhirPath;
import au.csiro.pathling.fhirpath.collection.Collection;
import au.csiro.pathling.fhirpath.execution.FhirPathExecutor;
import au.csiro.pathling.fhirpath.execution.SingleFhirPathExecutor;
import au.csiro.pathling.fhirpath.function.registry.StaticFunctionRegistry;
import au.csiro.pathling.query.view.definition.ConstantDeclaration;
import java.lang.reflect.InvocationTargetException;
import java.util.List;
import java.util.Map;
import lombok.Value;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.hl7.fhir.instance.model.api.IBase;
import org.hl7.fhir.r4.model.Enumerations.FHIRDefinedType;
import org.hl7.fhir.r4.model.Enumerations.ResourceType;
import org.jetbrains.annotations.NotNull;

/**
 * Dependencies and logic relating to the traversal of FHIRPath expressions.
 *
 * @author Piotr Szul
 */
@Value
public class ProjectionContext {

  @NotNull
  FhirPathExecutor executor;

  @NotNull
  Collection inputContext;

  @NotNull
  public Dataset<Row> getDataset() {
    return executor.createInitialDataset();
  }

  @NotNull
  public ProjectionContext withInputContext(@NotNull final Collection inputContext) {
    return new ProjectionContext(executor, inputContext);
  }

  /**
   * Evaluates the given FHIRPath path and returns the result as a column.
   *
   * @param path the path to evaluate
   * @return the result as a column
   */
  @NotNull
  public Collection evalExpression(@NotNull final FhirPath path) {
    return executor.evaluate(path, inputContext);
  }

  @NotNull
  public static ProjectionContext of(@NotNull final ExecutionContext context,
      @NotNull final ResourceType subjectResource,
      @NotNull final List<ConstantDeclaration> constants) {
    // Create a map of variables from the provided constants.
    final Map<String, Collection> variables = constants.stream()
        .collect(toMap(ConstantDeclaration::getName,
            ProjectionContext::getCollectionForConstantValue));

    // Create a new FhirPathExecutor.
    final FhirPathExecutor executor = new SingleFhirPathExecutor(subjectResource,
        context.getFhirContext(), new StaticFunctionRegistry(), variables, context.getDataSource());

    // Return a new ProjectionContext with the executor and the default input context.
    return new ProjectionContext(executor, executor.createDefaultInputContext());
  }

  @NotNull
  private static Collection getCollectionForConstantValue(@NotNull final ConstantDeclaration c) {
    final IBase value = c.getValue();
    final FHIRDefinedType fhirType = FHIRDefinedType.fromCode(value.fhirType());

    // Get the collection class for the FHIR type.
    final Class<? extends Collection> collectionClass = Collection.classForType(fhirType)
        .orElseThrow(() ->
            new InvalidUserInputError("Unsupported constant type: " + fhirType.toCode()));
    try {
      // Invoke the fromValue method on the collection class to get the return value.
      final Object returnValue = collectionClass.getMethod("fromValue", value.getClass())
          .invoke(null, value);
      check(returnValue instanceof Collection);
      return (Collection) returnValue;
    } catch (final IllegalAccessException | InvocationTargetException | NoSuchMethodException e) {
      throw new RuntimeException(e);
    }
  }

}
