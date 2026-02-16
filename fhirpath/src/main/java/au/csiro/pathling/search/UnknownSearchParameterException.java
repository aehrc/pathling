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
import org.hl7.fhir.r4.model.Enumerations.ResourceType;

/** Exception thrown when a search parameter is not found in the registry. */
public class UnknownSearchParameterException extends InvalidUserInputError {

  /**
   * Creates a new exception for an unknown search parameter.
   *
   * @param parameterCode the parameter code that was not found
   * @param resourceType the resource type being searched
   */
  public UnknownSearchParameterException(
      @Nonnull final String parameterCode, @Nonnull final ResourceType resourceType) {
    super(
        String.format(
            "Unknown search parameter '%s' for resource type '%s'",
            parameterCode, resourceType.toCode()));
  }
}
