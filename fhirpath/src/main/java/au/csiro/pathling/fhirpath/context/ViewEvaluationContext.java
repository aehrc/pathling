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
import au.csiro.pathling.fhirpath.function.registry.NoSuchFunctionException;
import jakarta.annotation.Nonnull;
import java.util.Optional;
import lombok.AllArgsConstructor;
import lombok.Value;

/**
 * An implementation of {@link EvaluationContext} used for evaluating FHIRPath expressions within
 * FHIR views.
 * <p>
 * This class combines three key components needed for FHIRPath evaluation:
 * <ul>
 *   <li>A {@link FhirPathContext} that provides access to variables and the input context</li>
 *   <li>A {@link FunctionRegistry} that provides access to FHIRPath functions</li>
 *   <li>A {@link ResourceResolver} that provides access to FHIR resources</li>
 * </ul>
 * <p>
 */
@Value
@AllArgsConstructor
public class ViewEvaluationContext implements EvaluationContext {

  /**
   * The FHIRPath context that provides access to variables and the input context.
   */
  @Nonnull
  FhirPathContext fhirPathContext;

  /**
   * The function registry that provides access to FHIRPath functions.
   */
  @Nonnull
  FunctionRegistry<?> functionRegistry;

  /**
   * The resource resolver that provides access to FHIR resources.
   */
  @Nonnull
  ResourceResolver resourceResolver;

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
  public NamedFunction<Collection> resolveFunction(@Nonnull final String name)
      throws NoSuchFunctionException {
    //noinspection unchecked
    return (NamedFunction<Collection>) functionRegistry.getInstance(name);
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
