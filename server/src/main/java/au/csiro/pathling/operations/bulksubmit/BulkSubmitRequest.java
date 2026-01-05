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

import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import java.util.List;

/**
 * Represents a request to the $bulk-submit operation.
 *
 * @param originalRequest The original request URL.
 * @param submissionId The unique identifier for this submission.
 * @param submitter The identifier of the submitting system.
 * @param submissionStatus The status of the submission: "in-progress", "complete", or "aborted".
 * @param manifestUrl The URL of the manifest file (required when status is "complete").
 * @param fhirBaseUrl The base URL of the FHIR server that produced the manifest.
 * @param replacesManifestUrl The URL of a previous manifest that this submission replaces.
 * @param oauthMetadataUrl URL to OAuth 2.0 metadata for token acquisition.
 * @param metadata Optional metadata associated with the submission.
 * @param fileRequestHeaders Custom HTTP headers to include when downloading files.
 * @author John Grimes
 * @see <a href="https://hackmd.io/@argonaut/rJoqHZrPle">Argonaut $bulk-submit Specification</a>
 */
public record BulkSubmitRequest(
    @Nonnull String originalRequest,
    @Nonnull String submissionId,
    @Nonnull SubmitterIdentifier submitter,
    @Nonnull String submissionStatus,
    @Nullable String manifestUrl,
    @Nullable String fhirBaseUrl,
    @Nullable String replacesManifestUrl,
    @Nullable String oauthMetadataUrl,
    @Nullable SubmissionMetadata metadata,
    @Nonnull List<FileRequestHeader> fileRequestHeaders) {

  /** Submission status indicating the submission is still in progress. */
  public static final String STATUS_IN_PROGRESS = "in-progress";

  /** Submission status indicating the submission is complete and ready for processing. */
  public static final String STATUS_COMPLETE = "complete";

  /** Submission status indicating the submission has been aborted. */
  public static final String STATUS_ABORTED = "aborted";

  /**
   * Returns true if the submission status is "in-progress".
   *
   * @return true if in-progress status.
   */
  public boolean isInProgress() {
    return STATUS_IN_PROGRESS.equals(submissionStatus);
  }

  /**
   * Returns true if the submission status is "complete".
   *
   * @return true if complete status.
   */
  public boolean isComplete() {
    return STATUS_COMPLETE.equals(submissionStatus);
  }

  /**
   * Returns true if the submission status is "aborted".
   *
   * @return true if aborted status.
   */
  public boolean isAborted() {
    return STATUS_ABORTED.equals(submissionStatus);
  }
}
