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

import au.csiro.pathling.library.io.FileSystemPersistence;
import au.csiro.pathling.shaded.com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.annotation.Nonnull;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.sql.SparkSession;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;

/**
 * Registry for tracking bulk submissions throughout their lifecycle.
 * <p>
 * Provides in-memory storage with file-based persistence via Hadoop FileSystem API for durability
 * across server restarts.
 *
 * @author John Grimes
 * @see <a href="https://hackmd.io/@argonaut/rJoqHZrPle">Argonaut $bulk-submit Specification</a>
 */
@Slf4j
@Component
@ConditionalOnProperty(prefix = "pathling.bulk-submit", name = "enabled", havingValue = "true")
public class SubmissionRegistry {

  private static final String SUBMISSIONS_DIR = "submissions";
  private static final String SUBMISSION_FILE = "submission.json";
  private static final String RESULT_FILE = "result.json";

  @Nonnull
  private final ConcurrentMap<String, Submission> submissions = new ConcurrentHashMap<>();

  @Nonnull
  private final ConcurrentMap<String, SubmissionResult> results = new ConcurrentHashMap<>();

  @Nonnull
  private final ObjectMapper objectMapper;

  @Nonnull
  private final SparkSession spark;

  @Nonnull
  private final String storageLocation;

  /**
   * Creates a new SubmissionRegistry.
   *
   * @param spark The Spark session for FileSystem access.
   * @param warehouseUrl The base warehouse URL for storage.
   */
  public SubmissionRegistry(
      @Nonnull final SparkSession spark,
      @Value("${pathling.storage.warehouseUrl}") final String warehouseUrl
  ) {
    this.spark = spark;
    this.storageLocation = FileSystemPersistence.safelyJoinPaths(warehouseUrl, SUBMISSIONS_DIR);
    this.objectMapper = createObjectMapper();
    loadFromStorage();
  }

  @Nonnull
  private static ObjectMapper createObjectMapper() {
    return new ObjectMapper();
  }

  /**
   * Creates or updates a submission in the registry.
   *
   * @param submission The submission to store.
   */
  public void put(@Nonnull final Submission submission) {
    final String key = submission.getKey();
    submissions.put(key, submission);
    persistSubmission(submission);
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
    persistResult(result);
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
      deleteFromStorage(removed);
      log.debug("Removed submission: {}", key);
      return true;
    }
    return false;
  }

  private void persistSubmission(@Nonnull final Submission submission) {
    final String path = getSubmissionPath(submission);
    try {
      final FileSystem fs = FileSystemPersistence.getFileSystem(spark, storageLocation);
      final Path filePath = new Path(path);
      fs.mkdirs(filePath.getParent());
      try (final OutputStream out = fs.create(filePath, true)) {
        final String json = objectMapper.writeValueAsString(submission);
        out.write(json.getBytes(StandardCharsets.UTF_8));
      }
    } catch (final IOException e) {
      log.warn("Failed to persist submission {}: {}", submission.getKey(), e.getMessage());
    }
  }

  private void persistResult(@Nonnull final SubmissionResult result) {
    // Results are stored alongside submissions - we need to find the submission first.
    submissions.values().stream()
        .filter(s -> s.submissionId().equals(result.submissionId()))
        .findFirst()
        .ifPresent(submission -> {
          final String path = getResultPath(submission);
          try {
            final FileSystem fs = FileSystemPersistence.getFileSystem(spark, storageLocation);
            final Path filePath = new Path(path);
            try (final OutputStream out = fs.create(filePath, true)) {
              final String json = objectMapper.writeValueAsString(result);
              out.write(json.getBytes(StandardCharsets.UTF_8));
            }
          } catch (final IOException e) {
            log.warn("Failed to persist result for {}: {}", result.submissionId(), e.getMessage());
          }
        });
  }

  private void deleteFromStorage(@Nonnull final Submission submission) {
    final String dirPath = getSubmissionDir(submission);
    try {
      final FileSystem fs = FileSystemPersistence.getFileSystem(spark, storageLocation);
      fs.delete(new Path(dirPath), true);
    } catch (final Exception e) {
      log.warn("Failed to delete submission from storage {}: {}", submission.getKey(),
          e.getMessage());
    }
  }

  private void loadFromStorage() {
    try {
      final FileSystem fs = FileSystemPersistence.getFileSystem(spark, storageLocation);
      final Path basePath = new Path(storageLocation);

      if (!fs.exists(basePath)) {
        log.debug("No submissions directory found at {}", storageLocation);
        return;
      }

      // Iterate through submitter directories.
      final FileStatus[] submitterDirs = fs.listStatus(basePath);
      for (final FileStatus submitterDir : submitterDirs) {
        if (!submitterDir.isDirectory()) {
          continue;
        }

        // Iterate through submission directories.
        final FileStatus[] submissionDirs = fs.listStatus(submitterDir.getPath());
        for (final FileStatus submissionDir : submissionDirs) {
          if (!submissionDir.isDirectory()) {
            continue;
          }

          loadSubmission(fs, submissionDir.getPath());
        }
      }

      log.info("Loaded {} submissions from storage", submissions.size());
    } catch (final Exception e) {
      log.warn("Failed to load submissions from storage: {}", e.getMessage());
    }
  }

  private void loadSubmission(@Nonnull final FileSystem fs, @Nonnull final Path submissionDir) {
    final Path submissionFile = new Path(submissionDir, SUBMISSION_FILE);
    try {
      if (!fs.exists(submissionFile)) {
        return;
      }

      try (final InputStream in = fs.open(submissionFile)) {
        final String json = new String(in.readAllBytes(), StandardCharsets.UTF_8);
        final Submission submission = objectMapper.readValue(json, Submission.class);
        submissions.put(submission.getKey(), submission);

        // Load result if present.
        final Path resultFile = new Path(submissionDir, RESULT_FILE);
        if (fs.exists(resultFile)) {
          try (final InputStream resultIn = fs.open(resultFile)) {
            final String resultJson = new String(resultIn.readAllBytes(), StandardCharsets.UTF_8);
            final SubmissionResult result = objectMapper.readValue(resultJson,
                SubmissionResult.class);
            results.put(result.submissionId(), result);
          }
        }
      }
    } catch (final IOException e) {
      log.warn("Failed to load submission from {}: {}", submissionDir, e.getMessage());
    }
  }

  @Nonnull
  private String getSubmissionDir(@Nonnull final Submission submission) {
    return FileSystemPersistence.safelyJoinPaths(
        FileSystemPersistence.safelyJoinPaths(storageLocation,
            sanitizePath(submission.submitter())),
        submission.submissionId()
    );
  }

  @Nonnull
  private String getSubmissionPath(@Nonnull final Submission submission) {
    return FileSystemPersistence.safelyJoinPaths(getSubmissionDir(submission), SUBMISSION_FILE);
  }

  @Nonnull
  private String getResultPath(@Nonnull final Submission submission) {
    return FileSystemPersistence.safelyJoinPaths(getSubmissionDir(submission), RESULT_FILE);
  }

  @Nonnull
  private String sanitizePath(@Nonnull final SubmitterIdentifier submitter) {
    // Replace characters that might be problematic in file paths.
    return submitter.toKey()
        .replace(":", "_")
        .replace("|", "_")
        .replace("/", "_");
  }

}
