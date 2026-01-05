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

package au.csiro.pathling.terminology;

import ca.uhn.fhir.rest.gclient.IOperationUntypedWithInput;
import jakarta.annotation.Nonnull;
import java.util.Optional;

/**
 * An abstraction of a terminology operation that describes how to validate, execute and extract a
 * result from the response.
 *
 * @param <R> The type of the response returned by the terminology client
 * @param <T> The type of the final result that is extracted from the response
 * @author John Grimes
 */
public interface TerminologyOperation<R, T> {

  /**
   * Validates the parameters for this operation.
   *
   * @return An empty {@link Optional} if the parameters are valid, or an {@link Optional}
   *     containing the result to be returned to the user if the parameters are invalid.
   */
  @Nonnull
  Optional<T> validate();

  /**
   * Builds a request for this operation, ready to send to the terminology server.
   *
   * @return A request for this operation that has not been executed yet, and can still be modified
   *     (e.g. headers)
   */
  @Nonnull
  IOperationUntypedWithInput<R> buildRequest();

  /**
   * Extracts the result from the response from the terminology server.
   *
   * @param response the response from the terminology server
   * @return the extracted result
   */
  @Nonnull
  T extractResult(@Nonnull final R response);

  /**
   * @return the result that should be returned to the user if the terminology server returns a 400
   *     series response
   */
  @Nonnull
  T invalidRequestFallback();
}
