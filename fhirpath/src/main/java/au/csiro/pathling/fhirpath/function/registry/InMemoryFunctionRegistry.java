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

package au.csiro.pathling.fhirpath.function.registry;

import au.csiro.pathling.fhirpath.function.NamedFunction;
import jakarta.annotation.Nonnull;
import java.util.Map;

/**
 * An implementation of {@link FunctionRegistry} that stores function instances in an in-memory map
 * structure.
 *
 * @author John Grimes
 */
public class InMemoryFunctionRegistry implements FunctionRegistry {

  @Nonnull
  private final Map<String, NamedFunction> functions;

  /**
   * @param functions The map of functions to store
   */
  public InMemoryFunctionRegistry(@Nonnull final Map<String, NamedFunction> functions) {
    this.functions = functions;
  }

  @Nonnull
  @Override
  public NamedFunction getInstance(@Nonnull final String name)
      throws NoSuchFunctionError {
    final NamedFunction function = functions.get(name);
    if (function == null) {
      throw new NoSuchFunctionError("Unsupported function: " + name);
    }
    return function;
  }

}
