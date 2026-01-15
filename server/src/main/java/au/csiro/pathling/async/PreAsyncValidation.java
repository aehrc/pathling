/*
 * Copyright © 2018-2026 Commonwealth Scientific and Industrial Research
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

package au.csiro.pathling.async;

import ca.uhn.fhir.rest.server.exceptions.InvalidRequestException;
import ca.uhn.fhir.rest.server.servlet.ServletRequestDetails;
import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import java.util.List;
import org.hl7.fhir.r4.model.OperationOutcome;

/**
 * Interface for operations that need to perform validation before async execution.
 *
 * @param <R> the type of the validation result
 * @author Felix Naumann
 */
public interface PreAsyncValidation<R> {

  /**
   * Run some validation before the async request kicks off. If the code in this method determines
   * that the request is invalid, then instead of a 202, an error is returned immediately.
   *
   * @param servletRequestDetails The details from the initial client request.
   * @param params The operation parameters.
   * @return A result object with the parsed structure and potential warnings.
   * @throws InvalidRequestException When the request is invalid.
   */
  @Nonnull
  PreAsyncValidationResult<R> preAsyncValidate(
      @Nonnull ServletRequestDetails servletRequestDetails, @Nonnull Object[] params)
      throws InvalidRequestException;

  /**
   * Contains the result of pre-async validation along with any warnings.
   *
   * @param result the parsed request result
   * @param warnings any warnings generated during validation
   * @param <R> the type of the result
   */
  record PreAsyncValidationResult<R>(
      @Nullable R result,
      @Nonnull List<OperationOutcome.OperationOutcomeIssueComponent> warnings) {}

  /**
   * Computes a cache key component from the parsed request. Override this method to include request
   * body parameters in job deduplication for POST operations.
   *
   * <p>The returned string should be deterministic and include all parameters that make this
   * request unique. It should NOT include:
   *
   * <ul>
   *   <li>The original request URL (already in RequestTag.requestUrl)
   *   <li>Server base URL (infrastructure detail, not request-specific)
   * </ul>
   *
   * @param request the parsed request object from preAsyncValidate
   * @return a cache key component, or empty string if no additional keying is needed
   */
  @Nonnull
  default String computeCacheKeyComponent(@Nonnull R request) {
    return "";
  }
}
