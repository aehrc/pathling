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

package au.csiro.pathling.fhirpath.evaluation;

import au.csiro.pathling.fhirpath.collection.Collection;
import au.csiro.pathling.fhirpath.function.registry.FunctionRegistry;
import au.csiro.pathling.fhirpath.function.registry.StaticFunctionRegistry;
import ca.uhn.fhir.context.FhirContext;
import jakarta.annotation.Nonnull;
import java.util.Map;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.hl7.fhir.r4.model.Enumerations.ResourceType;

/**
 * Builder for creating {@link DatasetEvaluator} instances.
 * <p>
 * This builder provides a fluent API for configuring and creating evaluators that combine
 * FHIRPath expression evaluation with a Spark Dataset. It uses {@link SingleResourceEvaluatorBuilder}
 * internally to create the underlying evaluator.
 *
 * <h2>Usage Example</h2>
 * <pre>{@code
 * DatasetEvaluator evaluator = DatasetEvaluatorBuilder
 *     .create(ResourceType.PATIENT, fhirContext)
 *     .withDataset(patientDataset)
 *     .withVariables(Map.of("myVar", myCollection))
 *     .build();
 *
 * CollectionDataset result = evaluator.evaluate(fhirPath);
 * }</pre>
 *
 * @see DatasetEvaluator
 * @see SingleResourceEvaluatorBuilder
 */
public class DatasetEvaluatorBuilder {

  @Nonnull
  private final ResourceType subjectResource;

  @Nonnull
  private final FhirContext fhirContext;

  private Dataset<Row> dataset;

  @Nonnull
  private Map<String, Collection> variables = Map.of();

  @Nonnull
  private FunctionRegistry functionRegistry = StaticFunctionRegistry.getInstance();

  @Nonnull
  private CrossResourceStrategy crossResourceStrategy = CrossResourceStrategy.FAIL;

  /**
   * Private constructor. Use factory methods to create instances.
   */
  private DatasetEvaluatorBuilder(
      @Nonnull final ResourceType subjectResource,
      @Nonnull final FhirContext fhirContext) {
    this.subjectResource = subjectResource;
    this.fhirContext = fhirContext;
  }

  /**
   * Creates a builder for the specified resource type.
   *
   * @param subjectResource the subject resource type
   * @param fhirContext the FHIR context
   * @return a new builder
   */
  @Nonnull
  public static DatasetEvaluatorBuilder create(
      @Nonnull final ResourceType subjectResource,
      @Nonnull final FhirContext fhirContext) {
    return new DatasetEvaluatorBuilder(subjectResource, fhirContext);
  }

  /**
   * Sets the dataset to evaluate against.
   * <p>
   * The dataset should have a flat schema where resource fields are top-level columns
   * (e.g., id, name, gender, birthDate, ...).
   *
   * @param dataset the Spark Dataset containing the resource data
   * @return this builder for method chaining
   */
  @Nonnull
  public DatasetEvaluatorBuilder withDataset(@Nonnull final Dataset<Row> dataset) {
    this.dataset = dataset;
    return this;
  }

  /**
   * Sets the variables available during FHIRPath evaluation.
   * <p>
   * Default is an empty map.
   *
   * @param variables the variables map
   * @return this builder for method chaining
   */
  @Nonnull
  public DatasetEvaluatorBuilder withVariables(@Nonnull final Map<String, Collection> variables) {
    this.variables = variables;
    return this;
  }

  /**
   * Sets the function registry for resolving FHIRPath functions.
   * <p>
   * Default is {@link StaticFunctionRegistry#getInstance()}.
   *
   * @param functionRegistry the function registry
   * @return this builder for method chaining
   */
  @Nonnull
  public DatasetEvaluatorBuilder withFunctionRegistry(
      @Nonnull final FunctionRegistry functionRegistry) {
    this.functionRegistry = functionRegistry;
    return this;
  }

  /**
   * Sets the strategy for handling cross-resource references.
   * <p>
   * Default is {@link CrossResourceStrategy#FAIL}.
   *
   * @param crossResourceStrategy the cross-resource strategy
   * @return this builder for method chaining
   */
  @Nonnull
  public DatasetEvaluatorBuilder withCrossResourceStrategy(
      @Nonnull final CrossResourceStrategy crossResourceStrategy) {
    this.crossResourceStrategy = crossResourceStrategy;
    return this;
  }

  /**
   * Builds the {@link DatasetEvaluator} with the configured options.
   *
   * @return a new DatasetEvaluator instance
   * @throws IllegalStateException if the dataset has not been set
   */
  @Nonnull
  public DatasetEvaluator build() {
    if (dataset == null) {
      throw new IllegalStateException("Dataset must be set before building DatasetEvaluator");
    }

    final SingleResourceEvaluator evaluator = SingleResourceEvaluatorBuilder
        .create(subjectResource, fhirContext)
        .withCrossResourceStrategy(crossResourceStrategy)
        .withFunctionRegistry(functionRegistry)
        .withVariables(variables)
        .build();

    return new DatasetEvaluator(evaluator, dataset);
  }
}
