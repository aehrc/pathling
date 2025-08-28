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

package au.csiro.pathling.fhirpath.function;

import static java.util.Objects.requireNonNull;

import au.csiro.pathling.fhirpath.annotations.Name;
import au.csiro.pathling.fhirpath.collection.Collection;
import jakarta.annotation.Nonnull;

/**
 * Represents a function in FHIRPath.
 *
 * @author John Grimes
 */
public interface NamedFunction {

  /**
   * @return the name of this function
   */
  @Nonnull
  default String name() {
    return requireNonNull(this.getClass().getAnnotation(Name.class)).value();
  }

  /**
   * Invokes this function with the specified inputs.
   *
   * @param input a {@link FunctionInput} object
   * @return a {@link Collection} object representing the resulting expression
   */
  @Nonnull
  default Collection invoke(@Nonnull final FunctionInput input) {
    throw new UnsupportedOperationException("Not implemented: " + name());
  }

}
