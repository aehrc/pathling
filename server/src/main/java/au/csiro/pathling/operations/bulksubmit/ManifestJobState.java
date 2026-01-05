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

package au.csiro.pathling.operations.bulksubmit;

/**
 * Represents the processing state of a manifest job within a bulk submission.
 *
 * @author John Grimes
 * @see <a href="https://hackmd.io/@argonaut/rJoqHZrPle">Argonaut $bulk-submit Specification</a>
 */
public enum ManifestJobState {

  /** The manifest job has been created but processing has not yet started. */
  PENDING,

  /** The manifest is being processed (files are being downloaded). */
  PROCESSING,

  /** The files have been downloaded successfully and are awaiting import. */
  DOWNLOADED,

  /** The manifest was imported successfully. */
  COMPLETED,

  /** The manifest processing failed with an error. */
  FAILED,

  /** The manifest job was explicitly aborted. */
  ABORTED;

  /**
   * Returns whether this state is a terminal state (processing has finished).
   *
   * @return true if the state is terminal, false otherwise.
   */
  public boolean isTerminal() {
    return this == COMPLETED || this == FAILED || this == ABORTED;
  }
}
