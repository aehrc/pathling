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

package au.csiro.pathling.async;

import static org.assertj.core.api.Assertions.assertThat;

import jakarta.annotation.Nonnull;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import org.hl7.fhir.instance.model.api.IBaseResource;
import org.hl7.fhir.r4.model.Parameters;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for {@link Job}, covering the creation-time start timestamp used by the SQL on FHIR
 * export manifest's timing fields, and the state machine that decides which party removes a deleted
 * job's output directory.
 *
 * @author John Grimes
 */
@Tag("UnitTest")
class JobTest {

  private static final int CONCURRENCY_ITERATIONS = 2000;

  @Test
  void recordsNonNullStartTimeAtCreation() {
    final Instant before = Instant.now();
    final Future<IBaseResource> result = CompletableFuture.completedFuture(new Parameters());
    final Job<Object> job = new Job<>("job-1", "viewdefinition-export", result, Optional.empty());
    final Instant after = Instant.now();

    // The job records a creation timestamp that falls within the window in which it was created.
    assertThat(job.getStartTime()).isNotNull();
    assertThat(job.getStartTime()).isBetween(before, after);
  }

  // Deletion claim state machine.

  @Test
  void newJobIsNeitherTerminatedNorMarkedDeleted() {
    // A job starts out with its work still to run and no deletion requested.
    final Job<Object> job = newJob();

    assertThat(job.isTerminated()).isFalse();
    assertThat(job.isMarkedAsDeleted()).isFalse();
  }

  @Test
  void deletionBeforeTerminationLeavesTheClaimToTheJobThread() {
    // The delete request arrives while the work is still running, so it must not take the claim.
    // The job's own thread takes it when it exits.
    final Job<Object> job = newJob();

    assertThat(job.markDeletedAndClaim()).isFalse();
    assertThat(job.isMarkedAsDeleted()).isTrue();
    assertThat(job.isTerminated()).isFalse();

    assertThat(job.markTerminatedAndClaim()).isTrue();
    assertThat(job.isTerminated()).isTrue();
  }

  @Test
  void terminationBeforeDeletionLeavesTheClaimToTheDeleteRequest() {
    // The work finishes before any delete arrives, so the job's thread must not take the claim. A
    // delete that follows takes it and removes the directory inline.
    final Job<Object> job = newJob();

    assertThat(job.markTerminatedAndClaim()).isFalse();
    assertThat(job.isTerminated()).isTrue();
    assertThat(job.isMarkedAsDeleted()).isFalse();

    assertThat(job.markDeletedAndClaim()).isTrue();
    assertThat(job.isMarkedAsDeleted()).isTrue();
  }

  @Test
  void secondClaimFromTheDeleteRequestReturnsFalse() {
    // The claim is single use: once the job's thread has taken it, a later delete does not.
    final Job<Object> job = newJob();
    assertThat(job.markDeletedAndClaim()).isFalse();
    assertThat(job.markTerminatedAndClaim()).isTrue();

    assertThat(job.markDeletedAndClaim()).isFalse();
  }

  @Test
  void secondClaimFromTheJobThreadReturnsFalse() {
    // The claim is single use: once the delete request has taken it, the job's thread does not.
    final Job<Object> job = newJob();
    assertThat(job.markTerminatedAndClaim()).isFalse();
    assertThat(job.markDeletedAndClaim()).isTrue();

    assertThat(job.markTerminatedAndClaim()).isFalse();
  }

  @Test
  void terminationWithoutDeletionNeverClaims() {
    // A job that runs to completion and is never deleted leaves the claim untaken, so its result
    // stays available for download.
    final Job<Object> job = newJob();

    assertThat(job.markTerminatedAndClaim()).isFalse();
    assertThat(job.markTerminatedAndClaim()).isFalse();
  }

  @Test
  void markTerminatedRecordsTerminationWithoutTakingTheClaim() {
    // Jobs registered outside the asynchronous request machinery are marked terminated up front, so
    // that a delete request handles their cleanup itself. Doing so must not consume the claim.
    final Job<Object> job = newJob();

    job.markTerminated();

    assertThat(job.isTerminated()).isTrue();
    assertThat(job.markDeletedAndClaim()).isTrue();
  }

  @Test
  void exactlyOnePartyClaimsWhenBothArriveConcurrently() throws Exception {
    // The delete request and the job's own thread may reach the decision point at the same time.
    // Whichever enters the monitor second observes the other's flag, so exactly one claims: never
    // both (which would remove the directory twice) and never neither (which would leak it).
    final ExecutorService executor = Executors.newFixedThreadPool(2);
    try {
      for (int iteration = 0; iteration < CONCURRENCY_ITERATIONS; iteration++) {
        final Job<Object> job = newJob();
        final CyclicBarrier barrier = new CyclicBarrier(2);

        final Future<Boolean> deleteClaim =
            executor.submit(
                () -> {
                  barrier.await();
                  return job.markDeletedAndClaim();
                });
        final Future<Boolean> threadClaim =
            executor.submit(
                () -> {
                  barrier.await();
                  return job.markTerminatedAndClaim();
                });

        final List<Boolean> claims = List.of(deleteClaim.get(), threadClaim.get());
        assertThat(claims)
            .as("iteration %d: exactly one party must claim the deletion", iteration)
            .containsOnlyOnce(true);
      }
    } finally {
      executor.shutdownNow();
      assertThat(executor.awaitTermination(10, TimeUnit.SECONDS)).isTrue();
    }
  }

  @Nonnull
  private static Job<Object> newJob() {
    return new Job<>("job-1", "export", new CompletableFuture<>(), Optional.empty());
  }
}
