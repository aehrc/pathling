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

package au.csiro.pathling.fhirpath;

import au.csiro.pathling.fhirpath.FhirPathConstants.PredefinedVariables;
import au.csiro.pathling.fhirpath.collection.Collection;
import au.csiro.pathling.fhirpath.collection.ResourceCollection;
import au.csiro.pathling.fhirpath.function.NamedFunction;
import au.csiro.pathling.fhirpath.function.registry.NoSuchFunctionException;
import jakarta.annotation.Nonnull;
import java.util.Optional;

/**
 * Provides the context for evaluating FHIRPath expressions. This interface defines methods for
 * resolving resources, functions, and variables during FHIRPath evaluation.
 * <p>
 * The evaluation context is responsible for:
 * <ul>
 *   <li>Resolving resource references to actual resource collections</li>
 *   <li>Resolving function names to function implementations</li>
 *   <li>Resolving variable references to their values</li>
 *   <li>Providing evaluation options that control the behavior of the evaluation</li>
 *   <li>Providing access to the current input context (the focus of the evaluation)</li>
 * </ul>
 */
public interface EvaluationContext {

  /**
   * Resolves a resource type code to a collection of resources of that type.
   *
   * @param resourceCode The code of the resource type to resolve
   * @return An optional containing the resource collection if the resource type exists,
   *         or an empty optional if it does not
   */
  @Nonnull
  Optional<ResourceCollection> resolveResource(@Nonnull final String resourceCode);

  /**
   * Resolves a function name to its implementation.
   *
   * @param name The name of the function to resolve
   * @return The function implementation
   * @throws NoSuchFunctionException If no function with the given name exists
   */
  @Nonnull
  NamedFunction<Collection> resolveFunction(@Nonnull final String name)
      throws NoSuchFunctionException;

  /**
   * Resolves a variable name to its value.
   *
   * @param name The name of the variable to resolve
   * @return The value of the variable
   */
  @Nonnull
  Collection resolveVariable(@Nonnull final String name);

  /**
   * Returns the evaluation options that control the behavior of the FHIRPath evaluation.
   *
   * @return The evaluation options
   */
  @Nonnull
  default EvalOptions getEvalOptions() {
    return EvalOptions.getDefaults();
  }

  /**
   * Returns the current input context (the focus of the evaluation).
   * <p>
   * By default, this is the value of the predefined variable "%context".
   *
   * @return The current input context
   */
  @Nonnull
  default Collection getInputContext() {
    return resolveVariable(PredefinedVariables.CONTEXT);
  }
}
