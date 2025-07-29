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

import au.csiro.pathling.fhirpath.collection.Collection;
import au.csiro.pathling.fhirpath.collection.ResourceCollection;
import au.csiro.pathling.fhirpath.variable.EnvironmentVariableResolver;
import jakarta.annotation.Nonnull;
import lombok.Value;

/**
 * Represents the context in which a FHIRPath expression is evaluated. It contains the resources
 * being processed, the input context, and a resolver for environment variables.
 */
@Value(staticConstructor = "of")
public class FhirPathContext {

  /**
   * The name of the variable representing the resource being processed.
   */
  public static final String RESOURCE_VARIABLE_NAME = "resource";

  /**
   * The name of the variable representing the root resource in the context.
   */
  public static final String ROOT_RESOURCE_VARIABLE_NAME = "rootResource";

  /**
   * The name of the variable representing the input context.
   */
  public static final String CONTEXT_VARIABLE_NAME = "context";

  /**
   * The collection of resources being processed.
   */
  @Nonnull
  ResourceCollection resource;

  /**
   * The input context for the evaluation.
   */
  @Nonnull
  Collection inputContext;

  /**
   * A resolver for environment variables.
   */
  @Nonnull
  EnvironmentVariableResolver variables;

  /**
   * Resolves a variable by its name.
   *
   * @param name the name of the variable to resolve
   * @return the collection associated with the variable name
   */
  @Nonnull
  public Collection resolveVariable(@Nonnull final String name) {
    if (name.equals(CONTEXT_VARIABLE_NAME)) {
      return inputContext;
    } else if (name.equals(RESOURCE_VARIABLE_NAME) || name.equals(ROOT_RESOURCE_VARIABLE_NAME)) {
      return resource;
    } else {
      return variables.get(name)
          .orElseThrow(() -> new IllegalArgumentException("Unknown variable: " + name));
    }
  }
}
