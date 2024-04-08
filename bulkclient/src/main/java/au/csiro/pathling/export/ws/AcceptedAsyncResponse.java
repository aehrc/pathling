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

package au.csiro.pathling.export.ws;

import java.util.Optional;
import javax.annotation.Nonnull;

import lombok.Builder;
import lombok.Value;

/**
 * Represents an accepted but incomplete status response for the FHIR asynchronous request pattern.
 */
@Value
@Builder
public class AcceptedAsyncResponse implements AsyncResponse {

  /**
   * Optional URL to the content of the response. This should be present in the initial response to
   * the kick-off request.
   */
  @Nonnull
  @Builder.Default
  Optional<String> contentLocation = Optional.empty();

  /**
   * Optional information on the progress of the request. The value of the 'X-Progress' header if
   * present.
   */
  @Nonnull
  @Builder.Default
  Optional<String> progress = Optional.empty();

  /**
   * Optional information on the retry-after value. The value of the 'Retry-After' header if
   * present.
   */
  @Nonnull
  @Builder.Default
  Optional<RetryValue> retryAfter = Optional.empty();
}
