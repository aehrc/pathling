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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

/**
 * Tests for {@link AsyncJobContext}.
 *
 * @author John Grimes
 */
class AsyncJobContextTest {

  @AfterEach
  void tearDown() {
    // Ensure the context is cleared after each test.
    AsyncJobContext.clear();
  }

  @Test
  void getCurrentJobReturnsEmptyWhenNotSet() {
    // When no job has been set, getCurrentJob should return empty.
    final Optional<Job<?>> result = AsyncJobContext.getCurrentJob();

    assertTrue(result.isEmpty());
  }

  @Test
  void getCurrentJobReturnsJobAfterSet() {
    // Given a mock job.
    final Job<?> mockJob = createMockJob("test-job-id");

    // When the job is set in the context.
    AsyncJobContext.setCurrentJob(mockJob);

    // Then getCurrentJob should return the same job.
    final Optional<Job<?>> result = AsyncJobContext.getCurrentJob();

    assertTrue(result.isPresent());
    assertEquals(mockJob, result.get());
    assertEquals("test-job-id", result.get().getId());
  }

  @Test
  void clearRemovesCurrentJob() {
    // Given a job has been set.
    final Job<?> mockJob = createMockJob("test-job-id");
    AsyncJobContext.setCurrentJob(mockJob);

    // Verify it's set.
    assertTrue(AsyncJobContext.getCurrentJob().isPresent());

    // When clear is called.
    AsyncJobContext.clear();

    // Then getCurrentJob should return empty.
    assertTrue(AsyncJobContext.getCurrentJob().isEmpty());
  }

  @Test
  void jobContextIsIsolatedPerThread() throws InterruptedException {
    // Given a job set in the main thread.
    final Job<?> mainThreadJob = createMockJob("main-thread-job");
    AsyncJobContext.setCurrentJob(mainThreadJob);

    // Set up a latch to synchronise thread execution.
    final CountDownLatch latch = new CountDownLatch(1);
    final AtomicReference<Optional<Job<?>>> otherThreadResult = new AtomicReference<>();

    // When we check the context from another thread.
    final Thread otherThread =
        new Thread(
            () -> {
              otherThreadResult.set(AsyncJobContext.getCurrentJob());
              latch.countDown();
            });
    otherThread.start();

    // Wait for the other thread to complete.
    final boolean completed = latch.await(5, TimeUnit.SECONDS);
    assertTrue(completed, "Thread should complete within timeout");

    // Then the other thread should see an empty context (ThreadLocal is per-thread).
    assertTrue(otherThreadResult.get().isEmpty(), "Other thread should not see main thread's job");

    // And the main thread should still see its job.
    assertTrue(AsyncJobContext.getCurrentJob().isPresent());
    assertEquals(mainThreadJob, AsyncJobContext.getCurrentJob().get());
  }

  @Test
  void setCurrentJobOverwritesPreviousJob() {
    // Given an initial job is set.
    final Job<?> firstJob = createMockJob("first-job");
    AsyncJobContext.setCurrentJob(firstJob);

    // When a second job is set.
    final Job<?> secondJob = createMockJob("second-job");
    AsyncJobContext.setCurrentJob(secondJob);

    // Then the context should contain the second job.
    final Optional<Job<?>> result = AsyncJobContext.getCurrentJob();
    assertTrue(result.isPresent());
    assertEquals("second-job", result.get().getId());
  }

  @SuppressWarnings("unchecked")
  private Job<?> createMockJob(final String jobId) {
    final Job<?> mockJob = mock(Job.class);
    when(mockJob.getId()).thenReturn(jobId);
    return mockJob;
  }
}
