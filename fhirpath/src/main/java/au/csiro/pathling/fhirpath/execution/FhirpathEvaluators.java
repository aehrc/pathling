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

import au.csiro.pathling.fhirpath.FhirPath;
import au.csiro.pathling.fhirpath.collection.Collection;
import au.csiro.pathling.fhirpath.collection.ResourceCollection;
import au.csiro.pathling.fhirpath.column.DefaultRepresentation;
import au.csiro.pathling.fhirpath.function.registry.FunctionRegistry;
import au.csiro.pathling.fhirpath.function.registry.StaticFunctionRegistry;
import au.csiro.pathling.io.source.DataSource;
import ca.uhn.fhir.context.FhirContext;
import jakarta.annotation.Nonnull;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Supplier;
import lombok.EqualsAndHashCode;
import lombok.Value;
import lombok.experimental.UtilityClass;
import lombok.extern.slf4j.Slf4j;
import org.hl7.fhir.r4.model.Enumerations.ResourceType;

/**
 * Utility class for creating different types of FhirpathEvaluator instances.
 * <p>
 * This class provides factory methods for creating evaluators with different resource resolution
 * strategies:
 * <ul>
 *   <li>{@link #createNull} - Creates an evaluator that returns empty collections, primarily used
 *       for validating FHIRPath expressions and determining result types without executing queries</li>
 *   <li>{@link #createSingle} - Creates an evaluator for working with a single resource type</li>
 *   <li>{@link #createMulti} - Creates an evaluator that can handle joins between multiple resources</li>
 *   <li>{@link #createMultiFromPaths} - Creates a multi-resource evaluator from context paths</li>
 * </ul>
 * <p>
 * The class also provides factory classes for creating evaluators with specific configurations:
 * <ul>
 *   <li>{@link SingleEvaluatorFactory} - Factory for creating single resource evaluators</li>
 *   <li>{@link MultiEvaluatorFactory} - Factory for creating multi-resource evaluators</li>
 *   <li>{@link MultiEvaluatorProvider} - Provider for creating multi-resource evaluators</li>
 * </ul>
 */
@UtilityClass
@Slf4j
public class FhirpathEvaluators {

  /**
   * Creates a null evaluator that returns empty collections.
   * <p>
   * The null evaluator is primarily used for:
   * <ul>
   *   <li>Validating FHIRPath expressions without executing actual queries</li>
   *   <li>Determining the result type of a FHIRPath expression</li>
   *   <li>Testing FHIRPath functionality without requiring actual data</li>
   * </ul>
   * <p>
   * This evaluator is efficient for syntax checking and type analysis since it doesn't
   * load or process any actual resource data.
   *
   * @param subjectResource the subject resource type
   * @param fhirContext the FHIR context
   * @return a new FhirpathEvaluator that returns empty collections
   */
  @Nonnull
  public static FhirpathEvaluator createNull(
      @Nonnull final ResourceType subjectResource,
      @Nonnull final FhirContext fhirContext) {
    return new FhirpathEvaluator(
        new NullResourceResolver(subjectResource, fhirContext),
        StaticFunctionRegistry.getInstance(),
        Map.of());
  }

  /**
   * Creates a single resource evaluator.
   * <p>
   * This evaluator is designed for evaluating FHIRPath expressions against a single resource type
   * without complex joins. It's the most common evaluator for simple queries.
   * <p>
   * Use this evaluator when:
   * <ul>
   *   <li>Working with a single resource type (e.g., Patient)</li>
   *   <li>Not requiring complex joins between different resource types</li>
   *   <li>Performing simple attribute access and filtering operations</li>
   * </ul>
   *
   * @param subjectResource the subject resource type (e.g., Patient, Observation)
   * @param fhirContext the FHIR context for FHIR model operations
   * @param functionRegistry the registry of FHIRPath functions to use during evaluation
   * @param variables the variables available during FHIRPath evaluation
   * @param dataSource the data source containing the resources to query
   * @return a new FhirpathEvaluator configured for single resource evaluation
   */
  @Nonnull
  public static FhirpathEvaluator createSingle(
      @Nonnull final ResourceType subjectResource,
      @Nonnull final FhirContext fhirContext,
      @Nonnull final FunctionRegistry<?> functionRegistry,
      @Nonnull final Map<String, Collection> variables,
      @Nonnull final DataSource dataSource) {
    return new FhirpathEvaluator(
        new SingleResourceResolver(subjectResource, fhirContext, dataSource),
        functionRegistry,
        variables
    );
  }

  /**
   * Creates a multi-resource evaluator that can handle joins between different resource types.
   * <p>
   * This evaluator supports complex FHIRPath expressions that traverse resource boundaries,
   * such as following references from one resource to another or performing reverse lookups.
   * <p>
   * Use this evaluator when:
   * <ul>
   *   <li>Working with expressions that traverse multiple resource types</li>
   *   <li>Performing joins between resources (e.g., Patient.managingOrganization.resolve())</li>
   *   <li>Executing complex queries that require data from related resources</li>
   * </ul>
   *
   * @param subjectResource the primary resource type being queried
   * @param fhirContext the FHIR context for FHIR model operations
   * @param functionRegistry the registry of FHIRPath functions to use during evaluation
   * @param variables the variables available during FHIRPath evaluation
   * @param dataSource the data source containing the resources to query
   * @param joinSets the join sets defining the relationships between resources
   * @return a new FhirpathEvaluator configured for multi-resource evaluation with joins
   */
  @Nonnull
  public static FhirpathEvaluator createMulti(
      @Nonnull final ResourceType subjectResource,
      @Nonnull final FhirContext fhirContext,
      @Nonnull final FunctionRegistry<?> functionRegistry,
      @Nonnull final Map<String, Collection> variables,
      @Nonnull final DataSource dataSource,
      @Nonnull final List<JoinSet> joinSets) {
    return new FhirpathEvaluator(
        new ManyResourceResolver(subjectResource, fhirContext, dataSource, joinSets),
        functionRegistry,
        variables
    );
  }

  /**
   * Creates a multi-resource evaluator from context paths.
   * <p>
   * This method simplifies the creation of a multi-resource evaluator by automatically
   * deriving the join sets from a list of FHIRPath expressions (context paths).
   * <p>
   * The context paths are analyzed to determine which resources need to be joined,
   * and appropriate join sets are created automatically.
   * <p>
   * Use this method when:
   * <ul>
   *   <li>You have a list of FHIRPath expressions that may traverse resource boundaries</li>
   *   <li>You want the system to automatically determine the necessary joins</li>
   *   <li>You prefer working with FHIRPath expressions rather than explicit join sets</li>
   * </ul>
   *
   * @param subjectResource the primary resource type being queried
   * @param fhirContext the FHIR context for FHIR model operations
   * @param functionRegistry the registry of FHIRPath functions to use during evaluation
   * @param variables the variables available during FHIRPath evaluation
   * @param dataSource the data source containing the resources to query
   * @param contextPaths the FHIRPath expressions that define the context for evaluation
   * @return a new FhirpathEvaluator configured for multi-resource evaluation based on context paths
   */
  @Nonnull
  public static FhirpathEvaluator createMultiFromPaths(
      @Nonnull final ResourceType subjectResource,
      @Nonnull final FhirContext fhirContext,
      @Nonnull final FunctionRegistry<?> functionRegistry,
      @Nonnull final Map<String, Collection> variables,
      @Nonnull final DataSource dataSource,
      @Nonnull final List<FhirPath> contextPaths) {
    final List<JoinSet> joinSets = fromContextPaths(subjectResource, fhirContext, contextPaths);
    return createMulti(subjectResource, fhirContext, functionRegistry, variables, dataSource,
        joinSets);
  }

  /**
   * Creates join sets from context paths.
   * <p>
   * This method analyzes a list of FHIRPath expressions to determine which resources
   * need to be joined and how they should be connected. It:
   * <ol>
   *   <li>Identifies all resource types referenced in the expressions</li>
   *   <li>Determines the relationships between these resources</li>
   *   <li>Creates appropriate join sets to represent these relationships</li>
   * </ol>
   * <p>
   * The resulting join sets are optimized to minimize the number of joins while
   * ensuring all required data is available for evaluation.
   *
   * @param subjectResource the primary resource type being queried
   * @param fhirContext the FHIR context for FHIR model operations
   * @param contextPaths the FHIRPath expressions to analyze
   * @return a list of join sets representing the resource relationships
   */
  @Nonnull
  private static List<JoinSet> fromContextPaths(
      @Nonnull final ResourceType subjectResource,
      @Nonnull final FhirContext fhirContext,
      @Nonnull final List<FhirPath> contextPaths) {
    final DataRootResolver dataRootResolver = new DataRootResolver(subjectResource, fhirContext);
    final Set<DataRoot> joinRoots = dataRootResolver.findDataRoots(contextPaths);
    log.debug("Join roots: {}", joinRoots);
    final List<JoinSet> joinSets = JoinSet.mergeRoots(joinRoots);
    joinSets.forEach(joinSet -> log.debug("Join set:\n{}", joinSet.toTreeString()));
    return joinSets;
  }

  /**
   * A resource resolver that returns empty collections for all resource types.
   * <p>
   * This resolver is primarily used for:
   * <ul>
   *   <li>Validating FHIRPath expressions without executing actual queries</li>
   *   <li>Determining the result type of a FHIRPath expression</li>
   *   <li>Testing FHIRPath functionality without requiring actual data</li>
   * </ul>
   * <p>
   * When using this resolver, all resource collections will be empty, but they will
   * have the correct structure and type information, allowing for type checking and
   * validation of FHIRPath expressions without the overhead of data processing.
   */
  @EqualsAndHashCode(callSuper = true)
  @Value
  private static class NullResourceResolver extends BaseResourceResolver {

    @Nonnull
    ResourceType subjectResource;

    @Nonnull
    FhirContext fhirContext;

    @Nonnull
    protected ResourceCollection createResource(@Nonnull final ResourceType resourceType) {
      return ResourceCollection.build(
          DefaultRepresentation.empty(),
          getFhirContext(), resourceType);
    }
  }

  /**
   * Factory for creating single resource evaluators.
   * <p>
   * This factory creates evaluators that work with a single resource type without complex joins.
   * It's suitable for simple queries that don't traverse resource boundaries.
   * <p>
   * The factory is configured with a FHIR context and data source, which are used for all
   * evaluators created by this factory. Each evaluator can be created with a different
   * subject resource type, function registry, and variables.
   */
  @Value(staticConstructor = "of")
  public static class SingleEvaluatorFactory implements FhirpathEvaluator.Factory {

    @Nonnull
    FhirContext fhirContext;

    @Nonnull
    DataSource dataSource;

    @Override
    @Nonnull
    public FhirpathEvaluator create(@Nonnull final ResourceType subjectResource,
        @Nonnull final FunctionRegistry<?> functionRegistry,
        @Nonnull final Map<String, Collection> variables) {
      return FhirpathEvaluators.createSingle(
          subjectResource,
          fhirContext,
          functionRegistry,
          variables,
          dataSource
      );
    }
  }

  /**
   * Factory for creating multi-resource evaluators.
   * <p>
   * This factory creates evaluators that can handle joins between different resource types.
   * It's suitable for complex queries that traverse resource boundaries.
   * <p>
   * The factory is configured with a subject resource type, FHIR context, data source,
   * and join sets, which define the relationships between resources. All evaluators created
   * by this factory will use the same subject resource type and join configuration.
   * <p>
   * Use the {@link #fromPaths} method to create a factory from context paths instead of
   * explicit join sets.
   */
  @Value(staticConstructor = "of")
  public static class MultiEvaluatorFactory implements FhirpathEvaluator.Factory {

    @Nonnull
    ResourceType subjectResource;

    @Nonnull
    FhirContext fhirContext;

    @Nonnull
    DataSource dataSource;

    @Nonnull
    List<JoinSet> joinSets;

    @Override
    @Nonnull
    public FhirpathEvaluator create(
        @Nonnull final ResourceType subjectResource,
        @Nonnull final FunctionRegistry<?> functionRegistry,
        @Nonnull final Map<String, Collection> variables) {

      if (!subjectResource.equals(this.subjectResource)) {
        throw new IllegalArgumentException(
            "subjectResource must be the same as the one used to create the factory");
      }
      return FhirpathEvaluators.createMulti(
          subjectResource,
          fhirContext,
          functionRegistry,
          variables,
          dataSource,
          joinSets
      );
    }

    /**
     * Creates a MultiEvaluatorFactory from context paths.
     * <p>
     * This method simplifies the creation of a multi-resource evaluator factory by automatically
     * deriving the join sets from a list of FHIRPath expressions (context paths).
     * <p>
     * The context paths are analyzed to determine which resources need to be joined,
     * and appropriate join sets are created automatically.
     *
     * @param subjectResource the primary resource type being queried
     * @param fhirContext the FHIR context for FHIR model operations
     * @param dataSource the data source containing the resources to query
     * @param contextPaths the FHIRPath expressions that define the context for evaluation
     * @return a new MultiEvaluatorFactory configured based on the context paths
     */
    @Nonnull
    public static MultiEvaluatorFactory fromPaths(@Nonnull ResourceType subjectResource,
        @Nonnull final FhirContext fhirContext,
        @Nonnull final DataSource dataSource,
        @Nonnull final List<FhirPath> contextPaths) {

      final List<JoinSet> joinSets = FhirpathEvaluators.fromContextPaths(
          subjectResource, fhirContext, contextPaths);
      return of(
          subjectResource,
          fhirContext,
          dataSource,
          joinSets
      );
    }
  }

  /**
   * Provider for creating multi-resource evaluators.
   * <p>
   * This provider creates evaluators that can handle joins between different resource types.
   * Unlike the {@link MultiEvaluatorFactory}, this provider allows for dynamic creation of
   * evaluators based on context paths that are supplied at runtime.
   * <p>
   * The provider is configured with a FHIR context, function registry, variables, and data source,
   * which are used for all evaluators created by this provider. Each evaluator can be created
   * with a different subject resource type and context paths.
   * <p>
   * This is particularly useful when the context paths are not known at the time the provider
   * is created, such as in dynamic query scenarios.
   */
  @Value
  public static class MultiEvaluatorProvider implements FhirpathEvaluator.Provider {

    @Nonnull
    FhirContext fhirContext;

    @Nonnull
    FunctionRegistry<?> functionRegistry;

    @Nonnull
    Map<String, Collection> variables;

    @Nonnull
    DataSource dataSource;

    @Nonnull
    @Override
    public FhirpathEvaluator create(@Nonnull final ResourceType subjectResource,
        @Nonnull final Supplier<List<FhirPath>> contextPathsSupplier) {
      final List<FhirPath> contextPaths = contextPathsSupplier.get();
      return FhirpathEvaluators.createMultiFromPaths(
          subjectResource,
          fhirContext,
          functionRegistry,
          variables,
          dataSource,
          contextPaths
      );
    }
  }
}
