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

import au.csiro.pathling.fhirpath.EvalOptions;
import au.csiro.pathling.fhirpath.EvaluationContext;
import au.csiro.pathling.fhirpath.collection.Collection;
import au.csiro.pathling.fhirpath.collection.ReferenceCollection;
import au.csiro.pathling.fhirpath.collection.ResourceCollection;
import au.csiro.pathling.fhirpath.function.NamedFunction;
import au.csiro.pathling.fhirpath.function.registry.FunctionRegistry;
import au.csiro.pathling.fhirpath.function.registry.NoSuchFunctionException;
import jakarta.annotation.Nonnull;
import lombok.AllArgsConstructor;
import lombok.Value;
import java.util.Optional;

@Value
@AllArgsConstructor
public class ViewEvaluationContext implements EvaluationContext {

  @Nonnull
  FhirPathContext fhirPathContext;

  @Nonnull
  FunctionRegistry<?> functionRegistry;

  @Nonnull
  ResourceResolver resourceResolver;

  @Nonnull
  EvalOptions evalOptions;

  public ViewEvaluationContext(@Nonnull final FhirPathContext fhirPathContext,
      @Nonnull final FunctionRegistry<?> functionRegistry,
      @Nonnull final ResourceResolver resourceResolver) {
    this(fhirPathContext, functionRegistry, resourceResolver, EvalOptions.getDefaults());
  }

  @Nonnull
  @Override
  public Optional<ResourceCollection> resolveResource(@Nonnull final String resourceCode) {
    return resourceResolver.resolveResource(resourceCode);
  }

  @Override
  @Nonnull
  public Collection resolveJoin(
      @Nonnull final ReferenceCollection referenceCollection) {
    return resourceResolver.resolveJoin(referenceCollection);
  }

  @Nonnull
  @Override
  public ResourceCollection resolveReverseJoin(@Nonnull final ResourceCollection parentResource,
      @Nonnull final String expression) {
    return resourceResolver.resolveReverseJoin(parentResource, expression);
  }

  @Nonnull
  @Override
  public NamedFunction<Collection> resolveFunction(@Nonnull final String name)
      throws NoSuchFunctionException {
    return (NamedFunction<Collection>) functionRegistry.getInstance(name);
  }

  @Nonnull
  @Override
  public Collection resolveVariable(@Nonnull final String name) {
    return fhirPathContext.resolveVariable(name);
  }

}
