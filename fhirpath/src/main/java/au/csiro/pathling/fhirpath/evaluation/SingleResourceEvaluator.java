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

import au.csiro.pathling.fhirpath.EvaluationContext;
import au.csiro.pathling.fhirpath.FhirPath;
import au.csiro.pathling.fhirpath.collection.Collection;
import au.csiro.pathling.fhirpath.collection.ResourceCollection;
import au.csiro.pathling.fhirpath.function.registry.FunctionRegistry;
import au.csiro.pathling.fhirpath.variable.EnvironmentVariableResolver;
import au.csiro.pathling.fhirpath.variable.VariableResolverChain;
import jakarta.annotation.Nonnull;
import java.util.Map;
import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Getter;
import org.hl7.fhir.r4.model.Enumerations.ResourceType;

/**
 * Evaluates FHIRPath expressions against a single FHIR resource type without requiring a
 * DataSource.
 *
 * <p>This evaluator produces {@link Collection} objects containing Spark SQL {@code Column}
 * expressions that can be applied to datasets. Unlike dataset-aware evaluators, this evaluator does
 * not require access to actual data and cannot create dataset views.
 *
 * <p>It uses a {@link ResourceResolver} for resource resolution and handles cross-resource
 * references according to the configured {@link CrossResourceStrategy}.
 *
 * <p>Use this evaluator when:
 *
 * <ul>
 *   <li>Building filter expressions for later application to datasets
 *   <li>Generating Column expressions from FHIRPath without executing queries
 *   <li>Working with single-resource scenarios that don't need cross-resource joins
 * </ul>
 *
 * <p>Use {@link SingleResourceEvaluatorBuilder} to create instances for standard FHIR resources, or
 * use {@link #of(ResourceResolver, FunctionRegistry, Map)} for custom resource resolvers.
 *
 * @see SingleResourceEvaluatorBuilder
 */
@AllArgsConstructor(access = AccessLevel.PACKAGE)
@Getter
public class SingleResourceEvaluator {

  /**
   * Creates a new SingleResourceEvaluator with the specified resolver.
   *
   * <p>This factory method allows creating evaluators with custom resource resolvers, such as
   * {@link DefinitionResourceResolver} for arbitrary data structures.
   *
   * @param resolver the resource resolver to use
   * @param functionRegistry the function registry for resolving FHIRPath functions
   * @param variables additional variables available during evaluation
   * @return a new SingleResourceEvaluator instance
   */
  @Nonnull
  public static SingleResourceEvaluator of(
      @Nonnull final ResourceResolver resolver,
      @Nonnull final FunctionRegistry functionRegistry,
      @Nonnull final Map<String, Collection> variables) {
    return new SingleResourceEvaluator(resolver, functionRegistry, variables);
  }

  /** The resource resolver. */
  @Nonnull private final ResourceResolver resourceResolver;

  /** The function registry for resolving FHIRPath functions. */
  @Nonnull private final FunctionRegistry functionRegistry;

  /** Additional variables available during evaluation. */
  @Nonnull private final Map<String, Collection> variables;

  /**
   * Evaluates a FHIRPath expression with the default input context.
   *
   * <p>The default input context is the subject resource, equivalent to calling {@link
   * #evaluate(FhirPath, Collection)} with the subject resource collection.
   *
   * @param fhirPath the FHIRPath expression to evaluate
   * @return the result of the evaluation as a Collection
   */
  @Nonnull
  public Collection evaluate(@Nonnull final FhirPath fhirPath) {
    return evaluate(fhirPath, getDefaultInputContext());
  }

  /**
   * Evaluates a FHIRPath expression with a custom input context.
   *
   * <p>The input context is the collection that the FHIRPath expression operates on. This is
   * typically used when evaluating expressions in the context of a specific element rather than the
   * resource root.
   *
   * @param fhirPath the FHIRPath expression to evaluate
   * @param inputContext the input context collection to evaluate against
   * @return the result of the evaluation as a Collection
   */
  @Nonnull
  public Collection evaluate(
      @Nonnull final FhirPath fhirPath, @Nonnull final Collection inputContext) {
    final ResourceCollection resource = getDefaultInputContext();
    final EnvironmentVariableResolver variableResolver =
        VariableResolverChain.withDefaults(resource, inputContext, variables);
    final EvaluationContext evalContext =
        new FhirEvaluationContext(
            inputContext, variableResolver, functionRegistry, resourceResolver);
    return fhirPath.apply(inputContext, evalContext);
  }

  /**
   * Returns the subject resource type for this evaluator.
   *
   * @return the subject resource type
   */
  @Nonnull
  public ResourceType getSubjectResource() {
    return resourceResolver.getSubjectResource();
  }

  /**
   * Returns the default input context (subject resource collection).
   *
   * <p>This is the initial input context used when evaluating FHIRPath expressions that start from
   * the resource root. The returned ResourceCollection represents the subject resource type for
   * this evaluator.
   *
   * @return the subject resource collection
   */
  @Nonnull
  public ResourceCollection getDefaultInputContext() {
    return resourceResolver.resolveSubjectResource();
  }
}
