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
import org.hl7.fhir.r4.model.Enumerations.ResourceType;

/**
 * Builder for creating {@link SingleResourceEvaluator} instances.
 * <p>
 * This builder provides a fluent API for configuring and creating evaluators for single-resource
 * FHIRPath evaluation. It uses flat schema representation where columns are accessed directly
 * (e.g., {@code col("name")}).
 *
 * <h2>Usage Examples</h2>
 *
 * <h3>Creating an evaluator for FHIR Search, filter building:</h3>
 * <pre>{@code
 * SingleResourceEvaluator evaluator = SingleResourceEvaluatorBuilder
 *     .create(ResourceType.PATIENT, fhirContext)
 *     .withCrossResourceStrategy(CrossResourceStrategy.EMPTY)
 *     .build();
 *
 * Collection result = evaluator.evaluate(fhirPath);
 * Column filter = result.getColumn().getValue();
 * }</pre>
 *
 * <h3>Creating an evaluator with custom function registry:</h3>
 * <pre>{@code
 * SingleResourceEvaluator evaluator = SingleResourceEvaluatorBuilder
 *     .create(ResourceType.OBSERVATION, fhirContext)
 *     .withFunctionRegistry(customRegistry)
 *     .withVariables(Map.of("myVar", myCollection))
 *     .build();
 * }</pre>
 *
 * @see SingleResourceEvaluator
 */
public class SingleResourceEvaluatorBuilder {

  @Nonnull
  private final ResourceType subjectResource;

  @Nonnull
  private final FhirContext fhirContext;

  @Nonnull
  private CrossResourceStrategy crossResourceStrategy = CrossResourceStrategy.FAIL;

  @Nonnull
  private FunctionRegistry functionRegistry = StaticFunctionRegistry.getInstance();

  @Nonnull
  private Map<String, Collection> variables = Map.of();

  /**
   * Private constructor. Use factory methods to create instances.
   */
  private SingleResourceEvaluatorBuilder(
      @Nonnull final ResourceType subjectResource,
      @Nonnull final FhirContext fhirContext) {
    this.subjectResource = subjectResource;
    this.fhirContext = fhirContext;
  }

  /**
   * Creates a builder for the specified resource type.
   * <p>
   * Column references are generated as direct column access (e.g., {@code col("name")}),
   * suitable for Pathling-encoded flat datasets.
   * <p>
   * This is suitable for:
   * <ul>
   *   <li>Building FHIR Search filter columns</li>
   *   <li>Creating filter expressions for flat datasets</li>
   * </ul>
   *
   * @param subjectResource the subject resource type
   * @param fhirContext the FHIR context
   * @return a new builder
   */
  @Nonnull
  public static SingleResourceEvaluatorBuilder create(
      @Nonnull final ResourceType subjectResource,
      @Nonnull final FhirContext fhirContext) {
    return new SingleResourceEvaluatorBuilder(subjectResource, fhirContext);
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
  public SingleResourceEvaluatorBuilder withCrossResourceStrategy(
      @Nonnull final CrossResourceStrategy crossResourceStrategy) {
    this.crossResourceStrategy = crossResourceStrategy;
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
  public SingleResourceEvaluatorBuilder withFunctionRegistry(
      @Nonnull final FunctionRegistry functionRegistry) {
    this.functionRegistry = functionRegistry;
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
  public SingleResourceEvaluatorBuilder withVariables(
      @Nonnull final Map<String, Collection> variables) {
    this.variables = variables;
    return this;
  }

  /**
   * Builds the {@link SingleResourceEvaluator} with the configured options.
   *
   * @return a new SingleResourceEvaluator instance
   */
  @Nonnull
  public SingleResourceEvaluator build() {
    final ResourceResolver resolver = new FhirResourceResolver(
        subjectResource,
        fhirContext,
        crossResourceStrategy);
    return new SingleResourceEvaluator(resolver, functionRegistry, variables);
  }
}
