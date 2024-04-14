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

import lombok.Builder;
import lombok.Value;

import java.time.Duration;

/**
 * Configuration relating to the FHIR asynchronous request pattern.
 *
 * @see <a href="https://build.fhir.org/async.html">Async Request Pattern</a>
 */
@Value
@Builder
public class AsyncConfig {

  /**
   * The minimum delay between two consecutive status requests. This may override the value returned
   * from the server in the 'retry-after' header.
   */
  @Builder.Default
  Duration minPoolingDelay = Duration.ofSeconds(1);

  /**
   * The maxium delay between two consecutive status requests. This may override the value returned
   * from the server in the 'retry-after' header.
   */
  @Builder.Default
  Duration maxPoolingDelay = Duration.ofSeconds(60);

  /**
   * The delay to retry after a transient error in a status request.
   */
  @Builder.Default
  Duration transientErrorDelay = Duration.ofSeconds(2);

  /**
   * The delay to retry after the HTTP 429 'Too many requests' response.
   */
  @Builder.Default
  Duration tooManyRequestsDelay = Duration.ofSeconds(10);

  /**
   * The maximum number of consecutive transient errors to retry before giving up.
   */
  @Builder.Default
  int maxTransientErrors = 3;
}
