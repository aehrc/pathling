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
import au.csiro.pathling.fhirpath.function.registry.FunctionRegistry;
import au.csiro.pathling.io.source.DataSource;
import ca.uhn.fhir.context.FhirContext;
import jakarta.annotation.Nonnull;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;
import lombok.Value;
import lombok.experimental.UtilityClass;
import lombok.extern.slf4j.Slf4j;
import org.hl7.fhir.r4.model.Enumerations.ResourceType;

/**
 * Utility class for creating FhirpathEvaluator instances.
 * <p>
 * This class provides factory methods and factory classes for creating evaluators that work with
 * single resource types. The {@link #createSingle} method creates an evaluator for working with a
 * single resource type, whilst the {@link SingleEvaluatorFactory} and
 * {@link SingleEvaluatorProvider} classes provide reusable factory instances for creating
 * evaluators with specific configurations.
 */
@UtilityClass
@Slf4j
public class FhirpathEvaluators {

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
   * Factory for creating single resource evaluators.
   * <p>
   * This factory creates evaluators that work with a single resource type without complex joins.
   * It's suitable for simple queries that don't traverse resource boundaries.
   * <p>
   * The factory is configured with a FHIR context and data source, which are used for all
   * evaluators created by this factory. Each evaluator can be created with a different subject
   * resource type, function registry, and variables.
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
   * A provider for creating single resource evaluators in FHIRPath contexts.
   * <p>
   * This class implements the FhirpathEvaluator.Provider interface to create FhirPath evaluators
   * configured for evaluating expressions on a single resource type. This approach is suitable for
   * straightforward scenarios involving evaluation against a specific resource type without
   * requiring complex joins across multiple resources.
   * <p>
   * The SingleEvaluatorProvider uses the following dependencies for evaluation: - `FhirContext`:
   * Provides FHIR context information for FHIR model operations. - `FunctionRegistry`: Manages
   * available FHIRPath functions during evaluation. - `variables`: Represents variables that can be
   * used within the FHIRPath expressions. - `DataSource`: The source containing the resource data
   * to be queried.
   * <p>
   * The `create` method initializes a single resource evaluator by utilizing the provided subject
   * resource and dynamically supplied context paths, which guide the evaluation process for
   * FHIRPath expressions.
   */
  public record SingleEvaluatorProvider(@Nonnull FhirContext fhirContext,
                                        @Nonnull FunctionRegistry<?> functionRegistry,
                                        @Nonnull Map<String, Collection> variables,
                                        @Nonnull DataSource dataSource) implements
      FhirpathEvaluator.Provider {

    @Nonnull
    @Override
    public FhirpathEvaluator create(@Nonnull final ResourceType subjectResource,
        @Nonnull final Supplier<List<FhirPath>> contextPathsSupplier) {
      return FhirpathEvaluators.createSingle(
          subjectResource,
          fhirContext,
          functionRegistry,
          variables,
          dataSource
      );
    }
  }
}
