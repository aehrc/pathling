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

import static java.util.stream.Collectors.toMap;

import au.csiro.pathling.fhirpath.FhirPath;
import au.csiro.pathling.fhirpath.collection.Collection;
import au.csiro.pathling.fhirpath.collection.EmptyCollection;
import au.csiro.pathling.fhirpath.column.DefaultRepresentation;
import au.csiro.pathling.fhirpath.execution.FhirPathEvaluator;
import au.csiro.pathling.fhirpath.function.registry.StaticFunctionRegistry;
import au.csiro.pathling.views.ConstantDeclaration;
import jakarta.annotation.Nonnull;
import java.util.List;
import java.util.Map;
import java.util.function.UnaryOperator;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

/**
 * Dependencies and logic relating to the traversal of FHIRPath expressions.
 *
 * @param executor an evaluator for FHIRPath expressions
 * @param inputContext the initial context for evaluation
 * @author Piotr Szul
 */
public record ProjectionContext(
    @Nonnull FhirPathEvaluator executor, @Nonnull Collection inputContext) {

  /**
   * Gets the initial dataset for this projection context.
   *
   * @return the initial dataset
   */
  @Nonnull
  public Dataset<Row> getDataset() {
    return executor.createInitialDataset();
  }

  /**
   * Creates a new ProjectionContext with a different input context.
   *
   * @param inputContext the new input context
   * @return a new ProjectionContext with the specified input context
   */
  @Nonnull
  public ProjectionContext withInputContext(@Nonnull final Collection inputContext) {
    return new ProjectionContext(executor, inputContext);
  }

  /**
   * Creates a new ProjectionContext with the current input context collection with the value set to
   * null.
   *
   * <p>This is useful for creating stub contexts when determining result schemas without evaluating
   * actual data, or when no input data is available. This is different from {@link
   * #withEmptyInput()} in that it preserves the type of the input context collection, but replaces
   * the underlying data with an empty representation.
   *
   * @return a new ProjectionContext with an empty input context
   */
  @Nonnull
  public ProjectionContext asStubContext() {
    return withInputContext(inputContext.copyWith(DefaultRepresentation.empty()));
  }

  /**
   * Creates a new ProjectionContext with the input context replaced by a new column.
   *
   * <p>This is a convenience method that wraps the column in a new input context while preserving
   * other context properties. It is particularly useful when transforming input data during
   * projection evaluation.
   *
   * @param inputColumn the new input column to use
   * @return a new ProjectionContext with the specified input column
   */
  @Nonnull
  public ProjectionContext withInputColumn(@Nonnull final Column inputColumn) {
    return withInputContext(inputContext.copyWithColumn(inputColumn));
  }

  /**
   * Creates a new ProjectionContext with an empty input context.
   *
   * <p>This is useful for creating stub contexts when determining result schemas without evaluating
   * actual data, or when no input data is available.
   *
   * @return a new ProjectionContext with an empty input context
   */
  @Nonnull
  public ProjectionContext withEmptyInput() {
    return withInputContext(EmptyCollection.getInstance());
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

  /**
   * Creates a unary operator that evaluates a FHIRPath expression on a given column.
   *
   * <p>This method returns a function that takes a column as input, evaluates the specified
   * FHIRPath expression using that column as the input context, and returns the resulting column
   * value. This is particularly useful for creating reusable transformations that can be applied to
   * multiple columns or used in higher-order operations like tree traversal.
   *
   * <p>Example use case: Creating a traversal operation for recursive tree structures where the
   * same FHIRPath expression needs to be evaluated on each node.
   *
   * @param path the FHIRPath expression to evaluate
   * @return a unary operator that takes a column and returns the result of evaluating the
   *     expression on that column
   */
  @Nonnull
  public UnaryOperator<Column> asColumnOperator(@Nonnull final FhirPath path) {
    return c -> withInputColumn(c).evalExpression(path).getColumnValue();
  }

  /**
   * Creates a new ProjectionContext from the given execution context, subject resource, and
   * constants.
   *
   * @param context the execution context
   * @param subjectResourceCode the subject resource type code (e.g., "Patient", "ViewDefinition")
   * @param constants the list of constant declarations
   * @return a new ProjectionContext
   */
  @Nonnull
  public static ProjectionContext of(
      @Nonnull final ExecutionContext context,
      @Nonnull final String subjectResourceCode,
      @Nonnull final List<ConstantDeclaration> constants) {
    // Create a map of variables from the provided constants.
    final Map<String, Collection> variables =
        constants.stream()
            .collect(
                toMap(
                    ConstantDeclaration::getName,
                    ProjectionContext::getCollectionForConstantValue));

    // Create a new FhirPathExecutor.
    final FhirPathEvaluator executor =
        context
            .fhirpathEvaluatorFactory()
            .create(subjectResourceCode, StaticFunctionRegistry.getInstance(), variables);

    // Return a new ProjectionContext with the executor and the default input context.
    return new ProjectionContext(executor, executor.createDefaultInputContext());
  }

  @Nonnull
  private static Collection getCollectionForConstantValue(@Nonnull final ConstantDeclaration c) {
    return Collection.fromValue(c.getValue());
  }
}
