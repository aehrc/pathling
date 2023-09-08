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

package au.csiro.pathling.fhirpath.function;

import static au.csiro.pathling.utilities.Preconditions.checkUserInput;
import static java.util.Objects.requireNonNull;

import au.csiro.pathling.fhirpath.FunctionInput;
import au.csiro.pathling.fhirpath.annotations.Name;
import au.csiro.pathling.fhirpath.collection.Collection;
import javax.annotation.Nonnull;

/**
 * Represents a function in FHIRPath.
 *
 * @author John Grimes
 */
public interface NamedFunction<O extends Collection> {

  /**
   * @return the name of this function
   */
  @Nonnull
  default String getName() {
    return requireNonNull(this.getClass().getAnnotation(Name.class)).value();
  }

  /**
   * Invokes this function with the specified inputs.
   *
   * @param input a {@link FunctionInput} object
   * @return a {@link Collection} object representing the resulting expression
   */
  @Nonnull
  default O invoke(@Nonnull final FunctionInput input) {
    throw new UnsupportedOperationException("Not implemented: " + getName());
  }

  /**
   * Check that no arguments have been passed within the supplied {@link FunctionInput}.
   *
   * @param functionName The name of the function, used for error reporting purposes
   * @param input The {@link FunctionInput} to check for arguments
   */
  static void checkNoArguments(@Nonnull final String functionName,
      @Nonnull final FunctionInput input) {
    checkUserInput(input.getArguments().isEmpty(),
        "Arguments can not be passed to " + functionName + " function");
  }

}
