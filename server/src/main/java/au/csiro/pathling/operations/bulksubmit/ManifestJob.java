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

/**
 * Represents a single manifest import job within a bulk submission.
 *
 * <p>Each manifest submitted to the bulk submit operation creates a ManifestJob that tracks the
 * processing state of that specific manifest independently from other manifests in the same
 * submission.
 *
 * @param manifestJobId Unique identifier for this manifest job.
 * @param manifestUrl The URL of the manifest file.
 * @param fhirBaseUrl The base URL of the FHIR server that produced the manifest.
 * @param oauthMetadataUrl URL to OAuth 2.0 metadata for token acquisition.
 * @param state Current processing state.
 * @param jobId The async Job ID for progress tracking.
 * @param createdAt ISO-8601 timestamp when created.
 * @param completedAt ISO-8601 timestamp when finished.
 * @param errorMessage Error message if failed.
 * @param errorFileName Name of the NDJSON file containing error OperationOutcome resources.
 * @param downloadedFiles List of files downloaded from the manifest.
 * @author John Grimes
 * @see <a href="https://hackmd.io/@argonaut/rJoqHZrPle">Argonaut $bulk-submit Specification</a>
 */
public record ManifestJob(
    @Nonnull String manifestJobId,
    @Nonnull String manifestUrl,
    @Nullable String fhirBaseUrl,
    @Nullable String oauthMetadataUrl,
    @Nonnull ManifestJobState state,
    @Nullable String jobId,
    @Nonnull String createdAt,
    @Nullable String completedAt,
    @Nullable String errorMessage,
    @Nullable String errorFileName,
    @Nullable List<DownloadedFile> downloadedFiles) {

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
   * Creates a new manifest job in PENDING state.
   *
   * @param manifestJobId Unique identifier for this manifest job.
   * @param manifestUrl The URL of the manifest file.
   * @param fhirBaseUrl The base URL of the FHIR server that produced the manifest.
   * @param oauthMetadataUrl URL to OAuth 2.0 metadata for token acquisition.
   * @return A new manifest job in PENDING state.
   */
  @Nonnull
  public static ManifestJob createPending(
      @Nonnull final String manifestJobId,
      @Nonnull final String manifestUrl,
      @Nullable final String fhirBaseUrl,
      @Nullable final String oauthMetadataUrl) {
    return new ManifestJob(
        manifestJobId,
        manifestUrl,
        fhirBaseUrl,
        oauthMetadataUrl,
        ManifestJobState.PENDING,
        null,
        now(),
        null,
        null,
        null,
        null);
  }

  /**
   * Creates a copy of this manifest job with a new state.
   *
   * @param newState The new state for the manifest job.
   * @return A new manifest job with the updated state.
   */
  @Nonnull
  public ManifestJob withState(@Nonnull final ManifestJobState newState) {
    final String newCompletedAt = newState.isTerminal() ? now() : this.completedAt;
    return new ManifestJob(
        manifestJobId,
        manifestUrl,
        fhirBaseUrl,
        oauthMetadataUrl,
        newState,
        jobId,
        createdAt,
        newCompletedAt,
        errorMessage,
        errorFileName,
        downloadedFiles);
  }

  /**
   * Creates a copy of this manifest job with a job ID for progress tracking.
   *
   * @param jobId The ID of the async Job processing this manifest.
   * @return A new manifest job with the job ID set.
   */
  @Nonnull
  public ManifestJob withJobId(@Nonnull final String jobId) {
    return new ManifestJob(
        manifestJobId,
        manifestUrl,
        fhirBaseUrl,
        oauthMetadataUrl,
        state,
        jobId,
        createdAt,
        completedAt,
        errorMessage,
        errorFileName,
        downloadedFiles);
  }

  /**
   * Creates a copy of this manifest job with an error message, error file name, and FAILED state.
   *
   * @param errorMessage The error message describing what went wrong.
   * @param errorFileName The name of the NDJSON file containing error OperationOutcome resources.
   * @return A new manifest job with the error details and FAILED state.
   */
  @Nonnull
  public ManifestJob withError(
      @Nonnull final String errorMessage, @Nullable final String errorFileName) {
    return new ManifestJob(
        manifestJobId,
        manifestUrl,
        fhirBaseUrl,
        oauthMetadataUrl,
        ManifestJobState.FAILED,
        jobId,
        createdAt,
        now(),
        errorMessage,
        errorFileName,
        downloadedFiles);
  }

  /**
   * Creates a copy of this manifest job with the list of downloaded files.
   *
   * @param downloadedFiles The list of files downloaded from the manifest.
   * @return A new manifest job with the downloaded files set.
   */
  @Nonnull
  public ManifestJob withDownloadedFiles(@Nonnull final List<DownloadedFile> downloadedFiles) {
    return new ManifestJob(
        manifestJobId,
        manifestUrl,
        fhirBaseUrl,
        oauthMetadataUrl,
        state,
        jobId,
        createdAt,
        completedAt,
        errorMessage,
        errorFileName,
        downloadedFiles);
  }

  /**
   * Returns whether this manifest job is in a terminal state.
   *
   * @return true if the job has completed or failed, false otherwise.
   */
  public boolean isTerminal() {
    return state.isTerminal();
  }
}
