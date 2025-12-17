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
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Function;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

/**
 * Registry for tracking bulk submissions throughout their lifecycle.
 * <p>
 * Provides in-memory storage for submissions. Submissions are not persisted across server
 * restarts.
 *
 * @author John Grimes
 * @see <a href="https://hackmd.io/@argonaut/rJoqHZrPle">Argonaut $bulk-submit Specification</a>
 */
@Slf4j
@Component
public class SubmissionRegistry {

  @Nonnull
  private final ConcurrentMap<String, Submission> submissions = new ConcurrentHashMap<>();

  @Nonnull
  private final ConcurrentMap<String, SubmissionResult> results = new ConcurrentHashMap<>();

  /**
   * Creates or updates a submission in the registry.
   *
   * @param submission The submission to store.
   */
  public void put(@Nonnull final Submission submission) {
    final String key = submission.getKey();
    submissions.put(key, submission);
    log.debug("Stored submission: {}", key);
  }

  /**
   * Retrieves a submission by submitter and submission ID.
   *
   * @param submitter The submitter identifier.
   * @param submissionId The submission ID.
   * @return The submission if found, or empty if not found.
   */
  @Nonnull
  public Optional<Submission> get(
      @Nonnull final SubmitterIdentifier submitter,
      @Nonnull final String submissionId
  ) {
    final String key = submitter.toKey() + "/" + submissionId;
    return Optional.ofNullable(submissions.get(key));
  }

  /**
   * Retrieves a submission by its composite key.
   *
   * @param key The composite key (submitter|value/submissionId).
   * @return The submission if found, or empty if not found.
   */
  @Nonnull
  public Optional<Submission> getByKey(@Nonnull final String key) {
    return Optional.ofNullable(submissions.get(key));
  }

  /**
   * Updates the state of a submission.
   *
   * @param submitter The submitter identifier.
   * @param submissionId The submission ID.
   * @param newState The new state.
   * @return The updated submission, or empty if the submission was not found.
   */
  @Nonnull
  public Optional<Submission> updateState(
      @Nonnull final SubmitterIdentifier submitter,
      @Nonnull final String submissionId,
      @Nonnull final SubmissionState newState
  ) {
    return get(submitter, submissionId).map(submission -> {
      final Submission updated = submission.withState(newState);
      put(updated);
      return updated;
    });
  }

  /**
   * Adds a manifest job to a submission.
   *
   * @param submitter The submitter identifier.
   * @param submissionId The submission ID.
   * @param manifestJob The manifest job to add.
   * @return The updated submission, or empty if the submission was not found.
   */
  @Nonnull
  public Optional<Submission> addManifestJob(
      @Nonnull final SubmitterIdentifier submitter,
      @Nonnull final String submissionId,
      @Nonnull final ManifestJob manifestJob
  ) {
    return get(submitter, submissionId).map(submission -> {
      final Submission updated = submission.withManifestJob(manifestJob);
      put(updated);
      log.debug("Added manifest job {} to submission {}", manifestJob.manifestJobId(),
          submissionId);
      return updated;
    });
  }

  /**
   * Updates a manifest job within a submission.
   *
   * @param submitter The submitter identifier.
   * @param submissionId The submission ID.
   * @param manifestJobId The ID of the manifest job to update.
   * @param updater A function that transforms the manifest job.
   * @return The updated submission, or empty if the submission or manifest job was not found.
   */
  @Nonnull
  public Optional<Submission> updateManifestJob(
      @Nonnull final SubmitterIdentifier submitter,
      @Nonnull final String submissionId,
      @Nonnull final String manifestJobId,
      @Nonnull final Function<ManifestJob, ManifestJob> updater
  ) {
    return get(submitter, submissionId).flatMap(submission -> {
      final Optional<ManifestJob> existingJob = submission.findManifestJob(manifestJobId);
      if (existingJob.isEmpty()) {
        log.warn("Manifest job {} not found in submission {}", manifestJobId, submissionId);
        return Optional.empty();
      }
      final ManifestJob updatedJob = updater.apply(existingJob.get());
      final Submission updated = submission.withUpdatedManifestJob(manifestJobId, updatedJob);
      put(updated);
      log.debug("Updated manifest job {} in submission {}", manifestJobId, submissionId);
      return Optional.of(updated);
    });
  }

  /**
   * Lists all submissions for a given submitter.
   *
   * @param submitter The submitter identifier.
   * @return A list of submissions for the submitter.
   */
  @Nonnull
  public List<Submission> getBySubmitter(@Nonnull final SubmitterIdentifier submitter) {
    final String prefix = submitter.toKey() + "/";
    return submissions.entrySet().stream()
        .filter(entry -> entry.getKey().startsWith(prefix))
        .map(java.util.Map.Entry::getValue)
        .toList();
  }

  /**
   * Stores a result for a completed submission.
   *
   * @param result The submission result to store.
   */
  public void putResult(@Nonnull final SubmissionResult result) {
    results.put(result.submissionId(), result);
    log.debug("Stored result for submission: {}", result.submissionId());
  }

  /**
   * Retrieves the result for a submission.
   *
   * @param submissionId The submission ID.
   * @return The result if found, or empty if not found.
   */
  @Nonnull
  public Optional<SubmissionResult> getResult(@Nonnull final String submissionId) {
    return Optional.ofNullable(results.get(submissionId));
  }

  /**
   * Removes a submission and its result from the registry.
   *
   * @param submitter The submitter identifier.
   * @param submissionId The submission ID.
   * @return true if the submission was removed, false if it was not found.
   */
  public boolean remove(
      @Nonnull final SubmitterIdentifier submitter,
      @Nonnull final String submissionId
  ) {
    final String key = submitter.toKey() + "/" + submissionId;
    final Submission removed = submissions.remove(key);
    results.remove(submissionId);
    if (removed != null) {
      log.debug("Removed submission: {}", key);
      return true;
    }
    return false;
  }

}
