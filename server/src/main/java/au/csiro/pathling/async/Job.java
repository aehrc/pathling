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

import jakarta.annotation.Nonnull;
import jakarta.servlet.http.HttpServletResponse;
import java.time.Instant;
import java.util.Optional;
import java.util.concurrent.Future;
import java.util.function.Consumer;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;
import org.hl7.fhir.instance.model.api.IBaseResource;

/**
 * Represents a background job that is in progress or complete.
 *
 * @param <T> the type of the pre-async validation result
 * @author John Grimes
 * @author Felix Naumann
 */
@Getter
@ToString
public class Job<T> {

  /**
   * A marker interface for job tags. These are used to identify input parameters that for which an
   * existing job can be used to provide the result.
   */
  public interface JobTag {}

  /** The unique identifier for this job. */
  @Nonnull final String id;

  /** The name of the operation that initiated this job, used for enforcing authorisation. */
  @Nonnull private final String operation;

  /** The future representing the asynchronous computation of the job result. */
  @Nonnull private final Future<IBaseResource> result;

  /** The identifier of the user who owns this job, if authenticated. */
  @Nonnull private final Optional<String> ownerId;

  /**
   * The time at which this job was created (kick-off time). Used to populate the SQL on FHIR export
   * manifest's {@code exportStartTime} and to compute {@code exportDuration}.
   */
  @Nonnull private final Instant startTime;

  /** The total number of stages in this job, used to calculate progress percentage. */
  private int totalStages;

  /** The number of completed stages in this job, used to calculate progress percentage. */
  private int completedStages;

  /** The result of pre-async validation, stored to be used when the job executes. */
  private T preAsyncValidationResult;

  /** A consumer that modifies the HTTP response for this job, such as adding headers. */
  @Setter private Consumer<HttpServletResponse> responseModification;

  /**
   * Indicates whether a client has asked for this job to be deleted. Guarded by this instance's
   * monitor and only ever set through {@link #markDeletedAndClaim()}.
   */
  @Getter(AccessLevel.NONE)
  private boolean markedAsDeleted;

  /**
   * Indicates whether the thread executing this job's work has finished unwinding. This is not the
   * same as the job's future being done: cancelling a future whose task has already started reports
   * the future as done immediately, while the work carries on. Guarded by this instance's monitor.
   */
  @Getter(AccessLevel.NONE)
  private boolean terminated;

  /**
   * Indicates whether some party has taken responsibility for removing this job's output directory.
   * Once set it never clears. Guarded by this instance's monitor.
   */
  @Getter(AccessLevel.NONE)
  private boolean deletionClaimed;

  /**
   * The asynchronous wire contract this job follows. Under {@link
   * AsyncPattern#STANDARD_ASYNC_PATTERN} (the HL7 Asynchronous Interaction Request Pattern, <a
   * href="https://build.fhir.org/ig/HL7/api-incubator-ig/branches/simplified-async-interaction/async-interaction.html">spec</a>)
   * a completed job returns 303 See Other with a redirect to the result endpoint, rather than
   * returning the result inline. Defaults to {@link AsyncPattern#BULK_DATA} and is never null.
   */
  @Setter private AsyncPattern pattern = AsyncPattern.BULK_DATA;

  /**
   * The last calculated progress percentage. When a job is at 100% that does not always indicate
   * that the job is actually finished. Most of the time, this indicates that a new stage has not
   * been submitted while the current stage is already completed. In that case just show the last
   * calculated percentage again.
   */
  @Setter private int lastProgress;

  /**
   * Creates a new Job.
   *
   * @param id the unique identifier for the job
   * @param operation the operation that initiated the job, used for enforcing authorisation
   * @param result the {@link Future} result
   * @param ownerId the identifier of the owner of the job, if authenticated
   */
  public Job(
      @Nonnull final String id,
      @Nonnull final String operation,
      @Nonnull final Future<IBaseResource> result,
      @Nonnull final Optional<String> ownerId) {
    this.id = id;
    this.operation = operation;
    this.result = result;
    this.ownerId = ownerId;
    this.startTime = Instant.now();
    this.responseModification = httpServletResponse -> {};
  }

  /** Increment the number of total stages within the job, used to calculate progress. */
  public void incrementTotalStages() {
    totalStages++;
  }

  /** Increment the number of completed stages within the job, used to calculate progress. */
  public void incrementCompletedStages() {
    completedStages++;
  }

  /**
   * Calculates the progress percentage based on completed and total stages.
   *
   * @return the progress percentage (0-100)
   */
  public int getProgressPercentage() {
    return (completedStages * 100) / totalStages;
  }

  /**
   * Sets the pre-async validation result for this job.
   *
   * @param preAsyncValidationResult the validation result to store
   */
  @SuppressWarnings("unchecked")
  public void setPreAsyncValidationResult(final Object preAsyncValidationResult) {
    try {
      this.preAsyncValidationResult = (T) preAsyncValidationResult;
    } catch (final ClassCastException e) {
      throw new InternalError("PreAsyncValidationResult casting failed.", e);
    }
  }

  /**
   * Checks whether this job has been cancelled.
   *
   * @return true if the job was cancelled, false otherwise
   */
  public boolean isCancelled() {
    return result.isCancelled();
  }

  /**
   * Checks whether a client has asked for this job to be deleted.
   *
   * @return true if the job has been marked for deletion, false otherwise
   */
  public synchronized boolean isMarkedAsDeleted() {
    return markedAsDeleted;
  }

  /**
   * Checks whether the thread executing this job's work has finished unwinding.
   *
   * @return true if the work has terminated, false otherwise
   */
  public synchronized boolean isTerminated() {
    return terminated;
  }

  /**
   * Records that a client has asked for this job to be deleted, and determines whether the caller
   * owns the removal of the job's output directory.
   *
   * <p>Called by the request handling the deletion. A {@code true} return obliges the caller to
   * remove the directory; a {@code false} return means the job's own thread has not finished yet
   * and will perform the removal as it exits.
   *
   * @return true if the caller has taken responsibility for removing the job's output directory
   */
  public synchronized boolean markDeletedAndClaim() {
    markedAsDeleted = true;
    return terminated && claim();
  }

  /**
   * Records that the thread executing this job's work has finished unwinding, and determines
   * whether that thread owns the removal of the job's output directory.
   *
   * <p>Called by the job's own thread as it exits. A {@code true} return obliges the caller to
   * remove the directory; a {@code false} return means either that no client has asked for the job
   * to be deleted, or that the request handling the deletion has already removed it.
   *
   * @return true if the caller has taken responsibility for removing the job's output directory
   */
  public synchronized boolean markTerminatedAndClaim() {
    markTerminated();
    return markedAsDeleted && claim();
  }

  /**
   * Records that the thread executing this job's work has finished unwinding, without contending
   * for the removal of the job's output directory.
   *
   * <p>Called at registration time for jobs whose work is not run by the asynchronous request
   * machinery. No thread will ever signal termination for such a job, so marking it terminated up
   * front is what allows a deletion request to perform its own cleanup rather than deferring it to
   * a thread that never arrives.
   */
  public synchronized void markTerminated() {
    terminated = true;
  }

  /**
   * Takes the single-use claim on removing this job's output directory, if it is still free.
   *
   * <p>Both entry points call this from inside the instance monitor, so their bodies are totally
   * ordered. Whichever runs second observes the flag written by the first, which is what guarantees
   * that at least one party claims once both have been called; this flag is what guarantees that at
   * most one does.
   *
   * @return true if the claim was free and has now been taken by the caller
   */
  private synchronized boolean claim() {
    if (deletionClaimed) {
      return false;
    }
    deletionClaimed = true;
    return true;
  }
}
