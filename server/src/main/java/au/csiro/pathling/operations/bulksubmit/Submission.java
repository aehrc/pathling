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
import java.time.Instant;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.Optional;

/**
 * Represents a bulk submission throughout its lifecycle.
 * <p>
 * Timestamps are stored as ISO-8601 strings for JSON serialisation compatibility with the shaded
 * Jackson library.
 *
 * @param submissionId The unique identifier for this submission.
 * @param submitter The identifier of the submitting system.
 * @param state The current state of the submission.
 * @param manifestUrl The URL of the manifest file containing the data to import.
 * @param replacesManifestUrl The URL of a previous manifest that this submission replaces.
 * @param fhirBaseUrl The base URL of the FHIR server that produced the manifest.
 * @param createdAt The timestamp when the submission was created (ISO-8601 format).
 * @param completedAt The timestamp when the submission completed processing (ISO-8601 format).
 * @param ownerId The identifier of the user who owns this submission, or null if not applicable.
 * @param fileRequestHeaders Custom HTTP headers to include when downloading files.
 * @param metadata Optional metadata associated with the submission.
 * @author John Grimes
 * @see <a href="https://hackmd.io/@argonaut/rJoqHZrPle">Argonaut $bulk-submit Specification</a>
 */
public record Submission(
    @Nonnull String submissionId,
    @Nonnull SubmitterIdentifier submitter,
    @Nonnull SubmissionState state,
    @Nullable String manifestUrl,
    @Nullable String replacesManifestUrl,
    @Nullable String fhirBaseUrl,
    @Nonnull String createdAt,
    @Nullable String completedAt,
    @Nullable String ownerId,
    @Nonnull List<FileRequestHeader> fileRequestHeaders,
    @Nullable SubmissionMetadata metadata
) {

  private static final DateTimeFormatter ISO_FORMATTER = DateTimeFormatter.ISO_INSTANT;

  /**
   * Returns the current timestamp as an ISO-8601 string.
   *
   * @return The current time formatted as ISO-8601.
   */
  @Nonnull
  private static String now() {
    return ISO_FORMATTER.format(Instant.now());
  }

  /**
   * Creates a new submission in PENDING state.
   *
   * @param submissionId The unique identifier for this submission.
   * @param submitter The identifier of the submitting system.
   * @param ownerId The optional identifier of the user who owns this submission.
   * @return A new submission in PENDING state.
   */
  @Nonnull
  public static Submission createPending(
      @Nonnull final String submissionId,
      @Nonnull final SubmitterIdentifier submitter,
      @Nonnull final Optional<String> ownerId
  ) {
    return new Submission(
        submissionId,
        submitter,
        SubmissionState.PENDING,
        null,
        null,
        null,
        now(),
        null,
        ownerId.orElse(null),
        List.of(),
        null
    );
  }

  /**
   * Creates a copy of this submission with a new state.
   *
   * @param newState The new state for the submission.
   * @return A new submission with the updated state.
   */
  @Nonnull
  public Submission withState(@Nonnull final SubmissionState newState) {
    final String newCompletedAt = (newState == SubmissionState.COMPLETED
        || newState == SubmissionState.COMPLETED_WITH_ERRORS
        || newState == SubmissionState.ABORTED)
                                  ? now()
                                  : this.completedAt;
    return new Submission(
        submissionId,
        submitter,
        newState,
        manifestUrl,
        replacesManifestUrl,
        fhirBaseUrl,
        createdAt,
        newCompletedAt,
        ownerId,
        fileRequestHeaders,
        metadata
    );
  }

  /**
   * Creates a copy of this submission with manifest details for processing.
   *
   * @param manifestUrl The URL of the manifest file.
   * @param fhirBaseUrl The base URL of the FHIR server.
   * @param fileRequestHeaders Custom headers for file downloads.
   * @param metadata Optional submission metadata.
   * @return A new submission ready for processing.
   */
  @Nonnull
  public Submission withManifestDetails(
      @Nonnull final String manifestUrl,
      @Nullable final String fhirBaseUrl,
      @Nonnull final List<FileRequestHeader> fileRequestHeaders,
      @Nullable final SubmissionMetadata metadata
  ) {
    return new Submission(
        submissionId,
        submitter,
        SubmissionState.PROCESSING,
        manifestUrl,
        replacesManifestUrl,
        fhirBaseUrl,
        createdAt,
        completedAt,
        ownerId,
        fileRequestHeaders,
        metadata
    );
  }

  /**
   * Returns a unique key for this submission based on submitter and submission ID.
   *
   * @return A composite key for registry storage.
   */
  @Nonnull
  public String getKey() {
    return submitter.toKey() + "/" + submissionId;
  }

}
