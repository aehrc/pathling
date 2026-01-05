/*
 * Copyright 2025 Commonwealth Scientific and Industrial Research
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

package au.csiro.pathling.operations.bulksubmit;

import lombok.Getter;

/**
 * Represents the state of a bulk submission throughout its lifecycle.
 *
 * @author John Grimes
 * @see <a href="https://hackmd.io/@argonaut/rJoqHZrPle">Argonaut $bulk-submit Specification</a>
 */
@Getter
public enum SubmissionState {

  /** Submission received with "in-progress" status, awaiting completion notification. */
  PENDING("pending"),

  /** Submission marked as complete, files are being processed. */
  PROCESSING("processing"),

  /** All files have been processed successfully. */
  COMPLETED("completed"),

  /** Files have been processed but some errors occurred. */
  COMPLETED_WITH_ERRORS("completed-with-errors"),

  /** Submission was explicitly aborted by the submitter. */
  ABORTED("aborted");

  private final String code;

  SubmissionState(final String code) {
    this.code = code;
  }

  /**
   * Resolve a SubmissionState enum from its code.
   *
   * @param code The code to resolve.
   * @return A SubmissionState if a match is found.
   * @throws IllegalArgumentException if no match can be found.
   */
  public static SubmissionState fromCode(final String code) {
    for (final SubmissionState state : SubmissionState.values()) {
      if (state.getCode().equalsIgnoreCase(code)) {
        return state;
      }
    }
    throw new IllegalArgumentException("Unknown submission state: " + code);
  }
}
