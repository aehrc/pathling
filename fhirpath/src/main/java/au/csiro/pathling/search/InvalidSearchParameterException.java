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

import jakarta.annotation.Nonnull;

/**
 * Thrown when a search parameter configuration is invalid.
 *
 * <p>This can occur when:
 *
 * <ul>
 *   <li>The FHIR type of the element does not match the allowed types for the search parameter type
 *   <li>The FHIR type cannot be determined from the FHIRPath expression result
 * </ul>
 *
 * @see <a href="https://hl7.org/fhir/search.html">FHIR Search</a>
 */
public class InvalidSearchParameterException extends RuntimeException {

  /**
   * Creates a new exception with a custom message.
   *
   * @param message the error message
   */
  public InvalidSearchParameterException(@Nonnull final String message) {
    super(message);
  }
}
