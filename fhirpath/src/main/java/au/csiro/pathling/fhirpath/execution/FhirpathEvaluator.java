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

package au.csiro.pathling.fhirpath.execution;

import au.csiro.pathling.fhirpath.EvaluationContext;
import au.csiro.pathling.fhirpath.FhirPath;
import au.csiro.pathling.fhirpath.collection.Collection;
import au.csiro.pathling.fhirpath.collection.ResourceCollection;
import au.csiro.pathling.fhirpath.context.FhirPathContext;
import au.csiro.pathling.fhirpath.context.ResourceResolver;
import au.csiro.pathling.fhirpath.context.ViewEvaluationContext;
import au.csiro.pathling.fhirpath.function.registry.FunctionRegistry;
import au.csiro.pathling.fhirpath.function.registry.StaticFunctionRegistry;
import au.csiro.pathling.fhirpath.variable.VariableResolverChain;
import jakarta.annotation.Nonnull;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;
import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Value;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.hl7.fhir.r4.model.Enumerations.ResourceType;

/**
 * Evaluates FHIRPath expressions against FHIR resources.
 * <p>
 * This class is the main entry point for FHIRPath evaluation in Pathling. It provides methods to
 * evaluate single expressions or multiple expressions against FHIR resources, handling the creation
 * of appropriate evaluation contexts and resource resolution.
 * <p>
 * The evaluator uses a {@link ResourceResolver} to access FHIR resources, a
 * {@link FunctionRegistry} to resolve FHIRPath functions, and can be configured with variables and
 * evaluation options.
 */
@Value
@AllArgsConstructor(access = AccessLevel.PUBLIC)
@Builder
public class FhirpathEvaluator {

  /**
   * Builder for creating {@link FhirpathEvaluator} instances with customized configuration.
   * <p>
   * This builder allows setting:
   * <ul>
   *   <li>A {@link ResourceResolver} for accessing FHIR resources (required)</li>
   *   <li>A {@link FunctionRegistry} for resolving FHIRPath functions (optional)</li>
   *   <li>A map of variables available during evaluation (optional)</li>
   * </ul>
   */
  public static class FhirpathEvaluatorBuilder {

  }

  /**
   * The resolver used to access FHIR resources during evaluation.
   */
  @Nonnull
  ResourceResolver resourceResolver;

  /**
   * The registry used to resolve FHIRPath functions during evaluation.
   */
  @Nonnull
  @Builder.Default
  FunctionRegistry functionRegistry = StaticFunctionRegistry.getInstance();

  /**
   * Variables available during FHIRPath evaluation.
   */
  @Nonnull
  @Builder.Default
  Map<String, Collection> variables = Map.of();

  /**
   * Creates a builder initialized with the specified resource resolver.
   * <p>
   * This is a convenience method for creating a builder when you only have a resource resolver and
   * want to use default values for other parameters.
   *
   * @param resourceResolver the resolver used to access FHIR resources
   * @return a builder initialized with the specified resource resolver
   */
  @Nonnull
  public static FhirpathEvaluatorBuilder fromResolver(
      @Nonnull final ResourceResolver resourceResolver) {
    return FhirpathEvaluator.builder().resourceResolver(resourceResolver);
  }

  /**
   * Evaluates a FHIRPath expression with the given input context.
   * <p>
   * This method creates an evaluation context with the specified input context and evaluates the
   * FHIRPath expression against it. The input context is the collection that the FHIRPath
   * expression will operate on.
   *
   * @param path the FHIRPath expression to evaluate
   * @param inputContext the input context collection to evaluate against
   * @return the result of the evaluation as a Collection
   */
  @Nonnull
  public Collection evaluate(@Nonnull final FhirPath path, @Nonnull final Collection inputContext) {
    final ResourceCollection resource = createDefaultInputContext();
    final VariableResolverChain variableResolverChain =
        VariableResolverChain.withDefaults(resource, inputContext, variables);
    final FhirPathContext fhirpathContext = FhirPathContext.of(
        resource, inputContext, variableResolverChain);
    final EvaluationContext evalContext = new ViewEvaluationContext(fhirpathContext,
        functionRegistry, resourceResolver);
    return path.apply(inputContext, evalContext);
  }

  /**
   * Evaluates a FHIRPath expression with the default input context.
   * <p>
   * This method uses the subject resource as the input context for evaluation. It's equivalent to
   * calling {@link #evaluate(FhirPath, Collection)} with the result of
   * {@link #createDefaultInputContext()} as the input context.
   *
   * @param path the FHIRPath expression to evaluate
   * @return the result of the evaluation as a Collection
   */
  @Nonnull
  public Collection evaluate(@Nonnull final FhirPath path) {
    return evaluate(path, createDefaultInputContext());
  }

  /**
   * Creates the default input context for evaluation.
   * <p>
   * The default input context is the subject resource of the current evaluation context. This is
   * typically the resource type specified when creating the evaluator.
   *
   * @return the default input context as a ResourceCollection
   */
  @Nonnull
  public ResourceCollection createDefaultInputContext() {
    return resourceResolver.resolveSubjectResource();
  }

  /**
   * Creates the initial dataset for evaluation.
   * <p>
   * This method returns the Spark Dataset that contains the data for the subject resource and any
   * joined resources. It's used as the starting point for evaluating FHIRPath expressions.
   *
   * @return the initial Spark Dataset for evaluation
   */
  @Nonnull
  public Dataset<Row> createInitialDataset() {
    return resourceResolver.createView();
  }

  /**
   * Factory interface for creating FhirpathEvaluator instances.
   * <p>
   * This interface provides methods for creating evaluators with different configurations, allowing
   * for dependency injection and easier testing.
   */
  public interface Factory {

    /**
     * Creates a FhirpathEvaluator with the given parameters.
     * <p>
     * This method creates a fully configured evaluator with the specified subject resource type,
     * function registry, and variables.
     *
     * @param subjectResource the subject resource type to evaluate against
     * @param functionRegistry the registry of FHIRPath functions to use
     * @param variables the variables available during evaluation
     * @return a new FhirpathEvaluator configured with the specified parameters
     */
    @Nonnull
    FhirpathEvaluator create(@Nonnull final ResourceType subjectResource,
        @Nonnull final FunctionRegistry functionRegistry,
        @Nonnull final Map<String, Collection> variables);

    /**
     * Creates a FhirpathEvaluator with the given subject resource and default parameters.
     * <p>
     * This method creates an evaluator with the specified subject resource type and default values
     * for the function registry (StaticFunctionRegistry) and variables (empty map).
     *
     * @param subjectResource the subject resource type to evaluate against
     * @return a new FhirpathEvaluator configured with the specified subject resource and default
     * parameters
     */
    @Nonnull
    default FhirpathEvaluator create(@Nonnull final ResourceType subjectResource) {
      return create(subjectResource, StaticFunctionRegistry.getInstance(), Map.of());
    }
  }

  /**
   * Provider interface for creating FhirpathEvaluator instances with context paths.
   * <p>
   * This interface is similar to Factory but allows for dynamic creation of evaluators based on
   * context paths that are supplied at runtime.
   */
  public interface Provider {

    /**
     * Creates a FhirpathEvaluator with the given parameters.
     * <p>
     * This method creates an evaluator that can handle context paths that are supplied dynamically
     * at runtime through the contextPathsSupplier.
     *
     * @param subjectResource the subject resource type to evaluate against
     * @param contextPathsSupplier a supplier that provides the context paths when needed
     * @return a new FhirpathEvaluator configured with the specified parameters
     */
    @Nonnull
    FhirpathEvaluator create(@Nonnull final ResourceType subjectResource,
        @Nonnull final Supplier<List<FhirPath>> contextPathsSupplier);
  }
}
