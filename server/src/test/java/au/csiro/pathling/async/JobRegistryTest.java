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
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.mockito.Mockito.mock;

import au.csiro.pathling.async.Job.JobTag;
import jakarta.annotation.Nonnull;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.Future;
import org.hl7.fhir.instance.model.api.IBaseResource;
import org.junit.jupiter.api.Test;

class JobRegistryTest {

  private static final Future<IBaseResource> MOCK_FUTURE = mock(FutureResource.class);
  private static final JobTag JOB_TAG_1 = new JobTag() {};
  private static final JobTag JOB_TAG_2 = new JobTag() {};

  @Nonnull private final JobRegistry registry = new JobRegistry();

  @Test
  void testNewJobCanBeRetrievedById() {
    final Job<?> newJob =
        registry.getOrCreate(
            JOB_TAG_1, id -> new Job<>(id, "operation", MOCK_FUTURE, Optional.empty()));

    assertEquals(newJob, registry.get(newJob.getId()));
  }

  @Test
  void testReusesJobWhenTagsAreIdentical() {
    final Job<?> firstJob =
        registry.getOrCreate(
            JOB_TAG_1, id -> new Job<>(id, "operation", MOCK_FUTURE, Optional.empty()));
    final Job<?> otherJob =
        registry.getOrCreate(
            JOB_TAG_1, id -> new Job<>(id, "operation", MOCK_FUTURE, Optional.empty()));

    assertEquals(firstJob, otherJob);
    assertEquals(firstJob, registry.get(firstJob.getId()));
  }

  @Test
  void testCreatesNewJobIfTagsDiffer() {

    assertNotEquals(JOB_TAG_1, JOB_TAG_2);

    final Job<?> firstJob =
        registry.getOrCreate(
            JOB_TAG_1, id -> new Job<>(id, "operation", MOCK_FUTURE, Optional.empty()));

    final Job<?> otherJob =
        registry.getOrCreate(
            JOB_TAG_2, id -> new Job<>(id, "operation", MOCK_FUTURE, Optional.empty()));

    assertNotEquals(firstJob, otherJob);
    assertNotEquals(firstJob.getId(), otherJob.getId());
  }

  @Test
  void snapshotReturnsAllRegisteredJobs() {
    final Job<?> firstJob =
        registry.getOrCreate(
            JOB_TAG_1, id -> new Job<>(id, "export", MOCK_FUTURE, Optional.empty()));
    final Job<?> secondJob =
        registry.getOrCreate(
            JOB_TAG_2, id -> new Job<>(id, "import", MOCK_FUTURE, Optional.empty()));

    assertThat(registry.allJobs()).containsExactlyInAnyOrder(firstJob, secondJob);
  }

  @Test
  void snapshotIsEmptyWhenNoJobsRegistered() {
    assertThat(registry.allJobs()).isEmpty();
  }

  @Test
  void snapshotReflectsRemovals() {
    final Job<?> firstJob =
        registry.getOrCreate(
            JOB_TAG_1, id -> new Job<>(id, "export", MOCK_FUTURE, Optional.empty()));
    final Job<?> secondJob =
        registry.getOrCreate(
            JOB_TAG_2, id -> new Job<>(id, "import", MOCK_FUTURE, Optional.empty()));

    registry.remove(firstJob);

    assertThat(registry.allJobs()).containsExactly(secondJob);
  }

  @Test
  void snapshotIsASafeCopy() {
    final Job<?> firstJob =
        registry.getOrCreate(
            JOB_TAG_1, id -> new Job<>(id, "export", MOCK_FUTURE, Optional.empty()));

    // The snapshot taken before a later registration must not observe the new job.
    final List<Job<?>> snapshot = registry.allJobs();
    registry.getOrCreate(JOB_TAG_2, id -> new Job<>(id, "import", MOCK_FUTURE, Optional.empty()));
    assertThat(snapshot).containsExactly(firstJob);

    // The returned list must be immutable so callers cannot mutate the registry through it.
    assertThatThrownBy(() -> snapshot.add(firstJob))
        .isInstanceOf(UnsupportedOperationException.class);
  }

  interface FutureResource extends Future<IBaseResource> {}
}
