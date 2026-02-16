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

package au.csiro.pathling.search;

import au.csiro.pathling.errors.InvalidUserInputError;
import jakarta.annotation.Nonnull;

/**
 * Thrown when an invalid or unsupported modifier is used with a search parameter.
 *
 * <p>Per FHIR specification, servers SHALL reject any search request that contains a search
 * parameter with an unsupported modifier.
 *
 * @see <a href="https://hl7.org/fhir/search.html#modifiers">FHIR Search Modifiers</a>
 */
public class InvalidModifierException extends InvalidUserInputError {

  /**
   * Creates a new exception for an invalid modifier.
   *
   * @param modifier the invalid modifier
   * @param parameterType the search parameter type
   */
  public InvalidModifierException(
      @Nonnull final String modifier, @Nonnull final SearchParameterType parameterType) {
    super(
        String.format(
            "Modifier '%s' is not supported for search parameter type %s",
            modifier, parameterType));
  }

  /**
   * Creates a new exception for an invalid modifier with a custom message.
   *
   * @param message the error message
   */
  @SuppressWarnings("unused")
  public InvalidModifierException(@Nonnull final String message) {
    super(message);
  }
}
