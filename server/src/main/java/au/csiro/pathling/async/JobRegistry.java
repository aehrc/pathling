/*
 * Copyright 2023 Commonwealth Scientific and Industrial Research
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

import au.csiro.pathling.async.Job.JobTag;
import ca.uhn.fhir.rest.server.exceptions.InternalErrorException;
import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.function.Function;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;

/**
 * Used for storing information about running asynchronous tasks.
 *
 * @author John Grimes
 */
@Slf4j
@Component
@ConditionalOnProperty(prefix = "pathling", name = "async.enabled", havingValue = "true")
public class JobRegistry {

  private final Map<String, Job<?>> jobsById = new HashMap<>();
  private final Map<JobTag, Job<?>> jobsByTags = new HashMap<>();

  private final Set<String> removedFromRegistryButStillWithSparkJob = new HashSet<>();

  /**
   * Gets the job with the given tag if it exists, or creates a new one using the given factory
   * function that accepts the job id.
   *
   * @param tag the tag of the job.
   * @param jobFactory the factory function that accepts the job id and returns a new job.
   * @return the job.
   */
  @Nonnull
  @SuppressWarnings("unchecked")
  public synchronized <T> Job<T> getOrCreate(@Nonnull final JobTag tag,
      @Nonnull final Function<String, Job<T>> jobFactory) {

    final Job<T> existingJob = (Job<T>) jobsByTags.get(tag);
    if (existingJob != null) {
      log.debug("Returning existing job: {} for tag: {}", existingJob.getId(), tag);
      return existingJob;
    } else {
      final String jobId = UUID.randomUUID().toString();
      final Job<T> newJob = jobFactory.apply(jobId);
      log.debug("Created new job: {} for tag: {}", newJob.getId(), tag);
      assert jobId.equals(newJob.getId());
      final Job<T> replacedJob = (Job<T>) jobsById.put(newJob.getId(), newJob);
      assert replacedJob == null;
      jobsByTags.put(tag, newJob);
      return newJob;
    }
  }

  @SuppressWarnings("unchecked")
  public synchronized <T> Job<T> get(JobTag jobTag) {
    return (Job<T>) jobsByTags.get(jobTag);
  }

  /**
   * Gets the jobs of the given id if exits or returns null otherwise.
   *
   * @param id the id of the job
   * @return the job or null
   */
  @Nullable
  @SuppressWarnings("unchecked")
  public synchronized <T> Job<T> get(@Nonnull final String id) {
    return (Job<T>) jobsById.get(id);
  }

  /**
   * Registers a job directly in the registry without using tags. This is useful for jobs created
   * outside the normal async request flow, such as bulk submit processing jobs.
   *
   * @param job The job to register.
   * @param <T> The type of the job's pre-async validation result.
   */
  public synchronized <T> void register(@Nonnull final Job<T> job) {
    final Job<?> existing = jobsById.get(job.getId());
    if (existing != null) {
      log.warn("Replacing existing job with id: {}", job.getId());
    }
    jobsById.put(job.getId(), job);
    log.debug("Registered job: {}", job.getId());
  }

  public synchronized <T> boolean remove(@Nonnull Job<T> job) {
    boolean removed = jobsById.remove(job.getId(), job);
    if (!removed) {
      log.warn("Failed to remove job {} from registry.", job.getId());
      return false;
    }
    boolean removedFromTags = jobsByTags.values().removeIf(otherJob -> otherJob.equals(job));
    if (!removedFromTags) {
      throw new InternalErrorException(
          "Removed job %s from id map but failed to remove it from tag map.".formatted(
              job.getId()));
    }
    removedFromRegistryButStillWithSparkJob.add(job.getId());
    return true;
  }

  public boolean removedFromRegistryButStillWithSparkJobContains(String jobId) {
    return removedFromRegistryButStillWithSparkJob.contains(jobId);
  }

  public boolean removeCompletelyAfterSparkCleanup(String jobId) {
    return removedFromRegistryButStillWithSparkJob.remove(jobId);
  }
}
