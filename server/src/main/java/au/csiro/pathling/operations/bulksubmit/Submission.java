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
import java.util.Optional;

/**
 * Represents a bulk submission throughout its lifecycle.
 *
 * <p>A submission can contain multiple manifest jobs, each representing a manifest URL submitted
 * via an in-progress request. All manifest jobs within a submission share the same submitter and
 * submission ID.
 *
 * <p>Timestamps are stored as ISO-8601 strings for JSON serialisation compatibility with the shaded
 * Jackson library.
 *
 * @param submissionId The unique identifier for this submission.
 * @param submitter The identifier of the submitting system.
 * @param state The current state of the submission.
 * @param manifestJobs The list of manifest jobs associated with this submission.
 * @param createdAt The timestamp when the submission was created (ISO-8601 format).
 * @param completedAt The timestamp when the submission completed processing (ISO-8601 format).
 * @param ownerId The identifier of the user who owns this submission, or null if not applicable.
 * @param metadata Optional metadata associated with the submission.
 * @param errorMessage Error message if the submission failed.
 * @author John Grimes
 * @see <a href="https://hackmd.io/@argonaut/rJoqHZrPle">Argonaut $bulk-submit Specification</a>
 */
public record Submission(
    @Nonnull String submissionId,
    @Nonnull SubmitterIdentifier submitter,
    @Nonnull SubmissionState state,
    @Nonnull List<ManifestJob> manifestJobs,
    @Nonnull String createdAt,
    @Nullable String completedAt,
    @Nullable String ownerId,
    @Nullable SubmissionMetadata metadata,
    @Nullable String errorMessage) {

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
      @Nonnull final Optional<String> ownerId) {
    return new Submission(
        submissionId,
        submitter,
        SubmissionState.PENDING,
        new ArrayList<>(),
        now(),
        null,
        ownerId.orElse(null),
        null,
        null);
  }

  /**
   * Creates a copy of this submission with a new state.
   *
   * @param newState The new state for the submission.
   * @return A new submission with the updated state.
   */
  @Nonnull
  public Submission withState(@Nonnull final SubmissionState newState) {
    final String newCompletedAt =
        (newState == SubmissionState.COMPLETED
                || newState == SubmissionState.COMPLETED_WITH_ERRORS
                || newState == SubmissionState.ABORTED)
            ? now()
            : this.completedAt;
    return new Submission(
        submissionId,
        submitter,
        newState,
        manifestJobs,
        createdAt,
        newCompletedAt,
        ownerId,
        metadata,
        errorMessage);
  }

  /**
   * Creates a copy of this submission with an additional manifest job.
   *
   * @param manifestJob The manifest job to add.
   * @return A new submission with the manifest job added.
   */
  @Nonnull
  public Submission withManifestJob(@Nonnull final ManifestJob manifestJob) {
    final List<ManifestJob> newManifestJobs = new ArrayList<>(this.manifestJobs);
    newManifestJobs.add(manifestJob);
    return new Submission(
        submissionId,
        submitter,
        state,
        newManifestJobs,
        createdAt,
        completedAt,
        ownerId,
        metadata,
        errorMessage);
  }

  /**
   * Creates a copy of this submission with an updated manifest job.
   *
   * @param manifestJobId The ID of the manifest job to update.
   * @param updatedJob The updated manifest job.
   * @return A new submission with the manifest job updated.
   */
  @Nonnull
  public Submission withUpdatedManifestJob(
      @Nonnull final String manifestJobId, @Nonnull final ManifestJob updatedJob) {
    final List<ManifestJob> newManifestJobs = new ArrayList<>();
    for (final ManifestJob job : this.manifestJobs) {
      if (job.manifestJobId().equals(manifestJobId)) {
        newManifestJobs.add(updatedJob);
      } else {
        newManifestJobs.add(job);
      }
    }
    return new Submission(
        submissionId,
        submitter,
        state,
        newManifestJobs,
        createdAt,
        completedAt,
        ownerId,
        metadata,
        errorMessage);
  }

  /**
   * Creates a copy of this submission with metadata.
   *
   * @param metadata The metadata to set.
   * @return A new submission with the metadata set.
   */
  @Nonnull
  public Submission withMetadata(@Nullable final SubmissionMetadata metadata) {
    return new Submission(
        submissionId,
        submitter,
        state,
        manifestJobs,
        createdAt,
        completedAt,
        ownerId,
        metadata,
        errorMessage);
  }

  /**
   * Creates a copy of this submission with an error message and COMPLETED_WITH_ERRORS state.
   *
   * @param errorMessage The error message describing what went wrong.
   * @return A new submission with the error message and updated state.
   */
  @Nonnull
  public Submission withError(@Nonnull final String errorMessage) {
    return new Submission(
        submissionId,
        submitter,
        SubmissionState.COMPLETED_WITH_ERRORS,
        manifestJobs,
        createdAt,
        now(),
        ownerId,
        metadata,
        errorMessage);
  }

  /**
   * Returns whether this submission has any manifest jobs that are not in a terminal state.
   *
   * @return true if there are outstanding jobs, false otherwise.
   */
  public boolean hasOutstandingJobs() {
    return manifestJobs.stream().anyMatch(job -> !job.isTerminal());
  }

  /**
   * Returns whether any manifest jobs in this submission have failed.
   *
   * @return true if any job has failed, false otherwise.
   */
  public boolean hasFailedJobs() {
    return manifestJobs.stream().anyMatch(job -> job.state() == ManifestJobState.FAILED);
  }

  /**
   * Returns whether all manifest jobs have completed downloading their files. Jobs in DOWNLOADED,
   * COMPLETED, or FAILED states are considered to have finished downloading.
   *
   * @return true if all jobs have finished downloading, false otherwise.
   */
  public boolean allDownloadsComplete() {
    return manifestJobs.stream()
        .allMatch(
            job ->
                job.state() == ManifestJobState.DOWNLOADED
                    || job.state() == ManifestJobState.COMPLETED
                    || job.state() == ManifestJobState.FAILED);
  }

  /**
   * Returns whether all manifest jobs are in the DOWNLOADED state and ready for import.
   *
   * @return true if all jobs are downloaded and ready, false otherwise.
   */
  public boolean allJobsReadyForImport() {
    return !manifestJobs.isEmpty()
        && manifestJobs.stream().allMatch(job -> job.state() == ManifestJobState.DOWNLOADED);
  }

  /**
   * Returns all job IDs from manifest jobs that have an associated async job.
   *
   * @return A list of job IDs.
   */
  @Nonnull
  public List<String> getAllJobIds() {
    return manifestJobs.stream().map(ManifestJob::jobId).filter(jobId -> jobId != null).toList();
  }

  /**
   * Finds a manifest job by its ID.
   *
   * @param manifestJobId The ID of the manifest job to find.
   * @return The manifest job if found, or empty if not found.
   */
  @Nonnull
  public Optional<ManifestJob> findManifestJob(@Nonnull final String manifestJobId) {
    return manifestJobs.stream()
        .filter(job -> job.manifestJobId().equals(manifestJobId))
        .findFirst();
  }

  /**
   * Finds a manifest job by its manifest URL.
   *
   * @param manifestUrl The URL of the manifest.
   * @return The manifest job if found, or empty if not found.
   */
  @Nonnull
  public Optional<ManifestJob> findManifestJobByUrl(@Nonnull final String manifestUrl) {
    return manifestJobs.stream().filter(job -> job.manifestUrl().equals(manifestUrl)).findFirst();
  }

  /**
   * Creates a copy of this submission without the specified manifest job.
   *
   * @param manifestJobId The ID of the manifest job to remove.
   * @return A new submission without the manifest job.
   */
  @Nonnull
  public Submission withoutManifestJob(@Nonnull final String manifestJobId) {
    final List<ManifestJob> newManifestJobs =
        manifestJobs.stream().filter(job -> !job.manifestJobId().equals(manifestJobId)).toList();
    return new Submission(
        submissionId,
        submitter,
        state,
        new ArrayList<>(newManifestJobs),
        createdAt,
        completedAt,
        ownerId,
        metadata,
        errorMessage);
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
