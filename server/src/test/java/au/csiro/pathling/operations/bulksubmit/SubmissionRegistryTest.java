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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import java.util.Optional;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

/**
 * Tests for {@link SubmissionRegistry} covering put, get, updateState, remove, getBySubmitter,
 * getByJobId, addManifestJob, and updateManifestJob operations.
 *
 * @author John Grimes
 */
@Tag("UnitTest")
class SubmissionRegistryTest {

  private SubmissionRegistry registry;
  private SubmitterIdentifier submitter;

  @BeforeEach
  void setUp() {
    registry = new SubmissionRegistry();
    submitter = new SubmitterIdentifier("http://example.org", "submitter1");
  }

  // -- put and get --

  @Test
  void putAndGetRetrievesSubmission() {
    // A submission that has been stored should be retrievable by submitter and ID.
    final Submission submission = Submission.createPending("sub-1", submitter, Optional.empty());
    registry.put(submission);

    final Optional<Submission> result = registry.get(submitter, "sub-1");
    assertTrue(result.isPresent());
    assertEquals("sub-1", result.get().submissionId());
  }

  @Test
  void getReturnsEmptyForMissingSubmission() {
    // Getting a non-existent submission should return empty.
    final Optional<Submission> result = registry.get(submitter, "nonexistent");
    assertTrue(result.isEmpty());
  }

  @Test
  void getByKeyRetrievesSubmission() {
    // A submission should also be retrievable via its composite key.
    final Submission submission = Submission.createPending("sub-1", submitter, Optional.empty());
    registry.put(submission);

    final String key = submitter.toKey() + "/sub-1";
    final Optional<Submission> result = registry.getByKey(key);
    assertTrue(result.isPresent());
    assertEquals("sub-1", result.get().submissionId());
  }

  @Test
  void getByKeyReturnsEmptyForMissingKey() {
    // Getting by a non-existent key should return empty.
    assertTrue(registry.getByKey("nonexistent/key").isEmpty());
  }

  // -- updateState --

  @Test
  void updateStateChangesSubmissionState() {
    // Updating the state of a submission should be reflected in subsequent retrievals.
    final Submission submission = Submission.createPending("sub-1", submitter, Optional.empty());
    registry.put(submission);

    final Optional<Submission> updated =
        registry.updateState(submitter, "sub-1", SubmissionState.PROCESSING);
    assertTrue(updated.isPresent());
    assertEquals(SubmissionState.PROCESSING, updated.get().state());

    // Verify the registry itself was updated.
    final Optional<Submission> retrieved = registry.get(submitter, "sub-1");
    assertTrue(retrieved.isPresent());
    assertEquals(SubmissionState.PROCESSING, retrieved.get().state());
  }

  @Test
  void updateStateReturnsEmptyForMissingSubmission() {
    // Updating state for a non-existent submission should return empty.
    final Optional<Submission> result =
        registry.updateState(submitter, "nonexistent", SubmissionState.COMPLETED);
    assertTrue(result.isEmpty());
  }

  // -- remove --

  @Test
  void removeDeletesSubmission() {
    // Removing a submission should make it no longer retrievable.
    final Submission submission = Submission.createPending("sub-1", submitter, Optional.empty());
    registry.put(submission);

    final boolean removed = registry.remove(submitter, "sub-1");
    assertTrue(removed);
    assertTrue(registry.get(submitter, "sub-1").isEmpty());
  }

  @Test
  void removeReturnsFalseForMissingSubmission() {
    // Removing a non-existent submission should return false.
    assertFalse(registry.remove(submitter, "nonexistent"));
  }

  // -- getBySubmitter --

  @Test
  void getBySubmitterReturnsAllSubmissionsForSubmitter() {
    // All submissions for a given submitter should be returned.
    registry.put(Submission.createPending("sub-1", submitter, Optional.empty()));
    registry.put(Submission.createPending("sub-2", submitter, Optional.empty()));

    // A different submitter should not be included.
    final SubmitterIdentifier otherSubmitter = new SubmitterIdentifier("http://other.org", "other");
    registry.put(Submission.createPending("sub-3", otherSubmitter, Optional.empty()));

    final List<Submission> results = registry.getBySubmitter(submitter);
    assertEquals(2, results.size());
  }

  @Test
  void getBySubmitterReturnsEmptyForUnknownSubmitter() {
    // Getting submissions for a submitter with none stored should return an empty list.
    final SubmitterIdentifier unknown = new SubmitterIdentifier("http://unknown.org", "nobody");
    assertTrue(registry.getBySubmitter(unknown).isEmpty());
  }

  // -- getByJobId --

  @Test
  void getByJobIdFindsSubmissionByManifestJobId() {
    // A submission should be findable by the job ID of one of its manifest jobs.
    final Submission submission = Submission.createPending("sub-1", submitter, Optional.empty());
    registry.put(submission);

    final ManifestJob manifestJob =
        ManifestJob.createPending("mj-1", "http://manifest.example.com", null, null)
            .withJobId("job-123");
    registry.addManifestJob(submitter, "sub-1", manifestJob);

    final Optional<Submission> result = registry.getByJobId("job-123");
    assertTrue(result.isPresent());
    assertEquals("sub-1", result.get().submissionId());
  }

  @Test
  void getByJobIdReturnsEmptyForUnknownJobId() {
    // Searching for a non-existent job ID should return empty.
    assertTrue(registry.getByJobId("nonexistent-job").isEmpty());
  }

  // -- addManifestJob --

  @Test
  void addManifestJobAddsJobToSubmission() {
    // Adding a manifest job should be reflected in subsequent retrievals.
    final Submission submission = Submission.createPending("sub-1", submitter, Optional.empty());
    registry.put(submission);

    final ManifestJob manifestJob =
        ManifestJob.createPending("mj-1", "http://manifest.example.com", null, null);
    final Optional<Submission> updated = registry.addManifestJob(submitter, "sub-1", manifestJob);

    assertTrue(updated.isPresent());
    assertEquals(1, updated.get().manifestJobs().size());
    assertEquals("mj-1", updated.get().manifestJobs().get(0).manifestJobId());
  }

  @Test
  void addManifestJobReturnsEmptyForMissingSubmission() {
    // Adding a manifest job to a non-existent submission should return empty.
    final ManifestJob manifestJob =
        ManifestJob.createPending("mj-1", "http://manifest.example.com", null, null);
    assertTrue(registry.addManifestJob(submitter, "nonexistent", manifestJob).isEmpty());
  }

  // -- updateManifestJob --

  @Test
  void updateManifestJobUpdatesExistingJob() {
    // Updating a manifest job should modify the specified job within the submission.
    final Submission submission = Submission.createPending("sub-1", submitter, Optional.empty());
    registry.put(submission);

    final ManifestJob manifestJob =
        ManifestJob.createPending("mj-1", "http://manifest.example.com", null, null);
    registry.addManifestJob(submitter, "sub-1", manifestJob);

    final Optional<Submission> updated =
        registry.updateManifestJob(
            submitter, "sub-1", "mj-1", job -> job.withState(ManifestJobState.PROCESSING));

    assertTrue(updated.isPresent());
    assertEquals(ManifestJobState.PROCESSING, updated.get().manifestJobs().get(0).state());
  }

  @Test
  void updateManifestJobReturnsEmptyForMissingJob() {
    // Updating a non-existent manifest job should return empty.
    final Submission submission = Submission.createPending("sub-1", submitter, Optional.empty());
    registry.put(submission);

    assertTrue(
        registry
            .updateManifestJob(
                submitter,
                "sub-1",
                "nonexistent-mj",
                job -> job.withState(ManifestJobState.PROCESSING))
            .isEmpty());
  }

  @Test
  void updateManifestJobReturnsEmptyForMissingSubmission() {
    // Updating a manifest job in a non-existent submission should return empty.
    assertTrue(
        registry
            .updateManifestJob(
                submitter, "nonexistent", "mj-1", job -> job.withState(ManifestJobState.PROCESSING))
            .isEmpty());
  }

  // -- overwrite --

  @Test
  void putOverwritesExistingSubmission() {
    // Putting a submission with the same key should overwrite the existing one.
    final Submission original = Submission.createPending("sub-1", submitter, Optional.empty());
    registry.put(original);

    final Submission updated = original.withState(SubmissionState.PROCESSING);
    registry.put(updated);

    final Optional<Submission> retrieved = registry.get(submitter, "sub-1");
    assertTrue(retrieved.isPresent());
    assertEquals(SubmissionState.PROCESSING, retrieved.get().state());
  }
}
