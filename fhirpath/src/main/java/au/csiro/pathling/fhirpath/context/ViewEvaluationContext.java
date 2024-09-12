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
import lombok.Value;
import org.hl7.fhir.r4.model.Enumerations.ResourceType;

@Value
public class ViewEvaluationContext implements EvaluationContext {

  @Nonnull
  FhirPathContext fhirPathContext;

  @Nonnull
  FunctionRegistry<?> functionRegistry;

  @Nonnull
  ResourceResolver resourceResolver;

  @Nonnull
  @Override
  public ResourceCollection resolveResource(@Nonnull final ResourceType resourceType) {
    return resourceResolver.resolveResource(resourceType);
  }

  @Nonnull
  @Override
  public ResourceCollection resolveReverseJoin(@Nonnull final ResourceType resourceType,
      @Nonnull final String expression) {
    return resourceResolver.resolveReverseJoin(resourceType, expression);
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
