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

package au.csiro.pathling.fhirpath.context;

import au.csiro.pathling.fhirpath.EvaluationContext;
import au.csiro.pathling.fhirpath.collection.Collection;
import au.csiro.pathling.fhirpath.collection.ResourceCollection;
import au.csiro.pathling.fhirpath.function.NamedFunction;
import au.csiro.pathling.fhirpath.function.registry.FunctionRegistry;
import au.csiro.pathling.fhirpath.function.registry.NoSuchFunctionError;
import jakarta.annotation.Nonnull;
import java.util.Optional;

/**
 * <p>
 * An implementation of {@link EvaluationContext} used for evaluating FHIRPath expressions within
 * FHIR views.
 * </p>
 * <p>
 * This class combines three key components needed for FHIRPath evaluation:
 * </p>
 * <ul>
 *   <li>A {@link FhirPathContext} that provides access to variables and the input context</li>
 *   <li>A {@link FunctionRegistry} that provides access to FHIRPath functions</li>
 *   <li>A {@link ResourceResolver} that provides access to FHIR resources</li>
 * </ul>
 *
 * @param fhirPathContext The FHIRPath context that provides access to variables and the input
 * context.
 * @param functionRegistry The function registry that provides access to FHIRPath functions.
 * @param resourceResolver The resource resolver that provides access to FHIR resources.
 */
public record ViewEvaluationContext(
    @Nonnull FhirPathContext fhirPathContext,
    @Nonnull FunctionRegistry functionRegistry,
    @Nonnull ResourceResolver resourceResolver
) implements
    EvaluationContext {

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
  public NamedFunction resolveFunction(@Nonnull final String name)
      throws NoSuchFunctionError {
    return functionRegistry.getInstance(name);
  }

  /**
   * {@inheritDoc}
   * <p>
   * Delegates to the underlying {@link FhirPathContext}.
   */
  @Nonnull
  @Override
  public Collection resolveVariable(@Nonnull final String name) {
    return fhirPathContext.resolveVariable(name);
  }

  /**
   * {@inheritDoc}
   * <p>
   * Delegates to the underlying {@link FhirPathContext}.
   */
  @Override
  @Nonnull
  public Collection getInputContext() {
    return fhirPathContext.getInputContext();
  }

}
