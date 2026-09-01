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

/**
 * The asynchronous wire contract that an operation follows. The two patterns differ in the kick-off
 * acknowledgement body returned with the {@code 202 Accepted} response and in how a completed job
 * is delivered when polled via {@code $job}.
 *
 * <p>The redirect-based contract is the HL7 Asynchronous Interaction Request Pattern, defined by
 * the HL7 R6 API Incubator: <a
 * href="https://build.fhir.org/ig/HL7/api-incubator-ig/branches/simplified-async-interaction/async-interaction.html">Asynchronous
 * Interaction Request Pattern</a>.
 *
 * @author John Grimes
 */
public enum AsyncPattern {

  /**
   * The HL7 Asynchronous Interaction Request Pattern. Kick-off returns a {@code Parameters}
   * acknowledgement (with {@code status=accepted} and an {@code exportId}), and a completed job
   * returns {@code 303 See Other} pointing at a separate {@code $job-result} endpoint.
   */
  STANDARD_ASYNC_PATTERN,

  /**
   * The FHIR Bulk Data pattern used by {@code $export}/{@code $import}. Kick-off returns an {@code
   * OperationOutcome}, and a completed job returns the result manifest inline with {@code 200 OK}.
   * This is the default.
   */
  BULK_DATA
}
