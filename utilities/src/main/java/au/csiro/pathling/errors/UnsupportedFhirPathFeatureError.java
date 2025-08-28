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

package au.csiro.pathling.errors;

import java.io.Serial;

/**
 * Thrown when the request references a FHIRPath feature that is not supported. This is specifically
 * reserved for things that are defined within FHIRPath but not supported by Pathling.
 *
 * @author John Grimes
 */
public class UnsupportedFhirPathFeatureError extends InvalidUserInputError {

  @Serial
  private static final long serialVersionUID = 3463869194525010650L;

  /**
   * Creates a new UnsupportedFhirPathFeatureError with the specified message.
   *
   * @param message the error message describing the unsupported feature
   */
  public UnsupportedFhirPathFeatureError(final String message) {
    super(message);
  }

}
