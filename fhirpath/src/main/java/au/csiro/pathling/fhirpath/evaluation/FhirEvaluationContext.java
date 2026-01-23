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
import au.csiro.pathling.fhirpath.collection.Collection;
import au.csiro.pathling.fhirpath.collection.ResourceCollection;
import au.csiro.pathling.fhirpath.function.NamedFunction;
import au.csiro.pathling.fhirpath.function.registry.FunctionRegistry;
import au.csiro.pathling.fhirpath.function.registry.NoSuchFunctionError;
import au.csiro.pathling.fhirpath.variable.EnvironmentVariableResolver;
import jakarta.annotation.Nonnull;
import java.util.Optional;

/**
 * An implementation of {@link EvaluationContext} for FHIRPath evaluation.
 * <p>
 * This context provides all the components needed for FHIRPath expression evaluation:
 * <ul>
 *   <li>Resource and input context for the current evaluation</li>
 *   <li>Variable resolution through an {@link EnvironmentVariableResolver}</li>
 *   <li>Function resolution through a {@link FunctionRegistry}</li>
 *   <li>Resource resolution through a {@link ResourceResolver}</li>
 * </ul>
 *
 * @param inputContext the current input context (focus) for the evaluation
 * @param variableResolver the resolver for environment variables
 * @param functionRegistry the registry for resolving FHIRPath functions
 * @param resourceResolver the resolver for FHIR resources
 */
public record FhirEvaluationContext(
    @Nonnull Collection inputContext,
    @Nonnull EnvironmentVariableResolver variableResolver,
    @Nonnull FunctionRegistry functionRegistry,
    @Nonnull ResourceResolver resourceResolver
) implements EvaluationContext {

  /**
   * {@inheritDoc}
   * <p>
   * Delegates to the underlying {@link ResourceResolver}.
   */
  @Nonnull
  @Override
  public Optional<ResourceCollection> resolveResource(@Nonnull final String resourceCode) {
    return resourceResolver.resolveResource(resourceCode);
  }

  /**
   * {@inheritDoc}
   * <p>
   * Delegates to the underlying {@link FunctionRegistry}.
   */
  @Nonnull
  @Override
  public NamedFunction resolveFunction(@Nonnull final String name) throws NoSuchFunctionError {
    return functionRegistry.getInstance(name);
  }

  /**
   * {@inheritDoc}
   * <p>
   * Delegates to the underlying {@link EnvironmentVariableResolver}.
   */
  @Nonnull
  @Override
  public Collection resolveVariable(@Nonnull final String name) {
    return variableResolver.get(name)
        .orElseThrow(() -> new IllegalArgumentException("Unknown variable: " + name));
  }

  /**
   * {@inheritDoc}
   * <p>
   * Returns the input context directly.
   */
  @Override
  @Nonnull
  public Collection getInputContext() {
    return inputContext;
  }
}
