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
import java.util.ArrayList;
import java.util.List;

/**
 * Represents a single manifest import job within a bulk submission.
 * <p>
 * Each manifest submitted to the bulk submit operation creates a ManifestJob that tracks the
 * processing state of that specific manifest independently from other manifests in the same
 * submission.
 *
 * @param manifestJobId Unique identifier for this manifest job.
 * @param manifestUrl The URL of the manifest file.
 * @param fhirBaseUrl The base URL of the FHIR server that produced the manifest.
 * @param state Current processing state.
 * @param jobId The async Job ID for progress tracking.
 * @param createdAt ISO-8601 timestamp when created.
 * @param completedAt ISO-8601 timestamp when finished.
 * @param errorMessage Error message if failed.
 * @param outputFiles Output files from this manifest.
 * @author John Grimes
 * @see <a href="https://hackmd.io/@argonaut/rJoqHZrPle">Argonaut $bulk-submit Specification</a>
 */
public record ManifestJob(
    @Nonnull String manifestJobId,
    @Nonnull String manifestUrl,
    @Nullable String fhirBaseUrl,
    @Nonnull ManifestJobState state,
    @Nullable String jobId,
    @Nonnull String createdAt,
    @Nullable String completedAt,
    @Nullable String errorMessage,
    @Nonnull List<OutputFile> outputFiles
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
   * Creates a new manifest job in PENDING state.
   *
   * @param manifestJobId Unique identifier for this manifest job.
   * @param manifestUrl The URL of the manifest file.
   * @param fhirBaseUrl The base URL of the FHIR server that produced the manifest.
   * @return A new manifest job in PENDING state.
   */
  @Nonnull
  public static ManifestJob createPending(
      @Nonnull final String manifestJobId,
      @Nonnull final String manifestUrl,
      @Nullable final String fhirBaseUrl
  ) {
    return new ManifestJob(
        manifestJobId,
        manifestUrl,
        fhirBaseUrl,
        ManifestJobState.PENDING,
        null,
        now(),
        null,
        null,
        new ArrayList<>()
    );
  }

  /**
   * Creates a copy of this manifest job with a new state.
   *
   * @param newState The new state for the manifest job.
   * @return A new manifest job with the updated state.
   */
  @Nonnull
  public ManifestJob withState(@Nonnull final ManifestJobState newState) {
    final String newCompletedAt = newState.isTerminal()
                                  ? now()
                                  : this.completedAt;
    return new ManifestJob(
        manifestJobId,
        manifestUrl,
        fhirBaseUrl,
        newState,
        jobId,
        createdAt,
        newCompletedAt,
        errorMessage,
        outputFiles
    );
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
        state,
        jobId,
        createdAt,
        completedAt,
        errorMessage,
        outputFiles
    );
  }

  /**
   * Creates a copy of this manifest job with an error message and FAILED state.
   *
   * @param errorMessage The error message describing what went wrong.
   * @return A new manifest job with the error message and FAILED state.
   */
  @Nonnull
  public ManifestJob withError(@Nonnull final String errorMessage) {
    return new ManifestJob(
        manifestJobId,
        manifestUrl,
        fhirBaseUrl,
        ManifestJobState.FAILED,
        jobId,
        createdAt,
        now(),
        errorMessage,
        outputFiles
    );
  }

  /**
   * Creates a copy of this manifest job with output files.
   *
   * @param outputFiles The output files from processing this manifest.
   * @return A new manifest job with the output files set.
   */
  @Nonnull
  public ManifestJob withOutputFiles(@Nonnull final List<OutputFile> outputFiles) {
    return new ManifestJob(
        manifestJobId,
        manifestUrl,
        fhirBaseUrl,
        state,
        jobId,
        createdAt,
        completedAt,
        errorMessage,
        outputFiles
    );
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
