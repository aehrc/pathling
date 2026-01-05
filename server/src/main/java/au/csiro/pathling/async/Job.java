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

import jakarta.annotation.Nonnull;
import jakarta.servlet.http.HttpServletResponse;
import java.util.Optional;
import java.util.concurrent.Future;
import java.util.function.Consumer;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;
import org.hl7.fhir.instance.model.api.IBaseResource;

/**
 * Represents a background job that is in progress or complete.
 *
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

  /** The total number of stages in this job, used to calculate progress percentage. */
  private int totalStages;

  /** The number of completed stages in this job, used to calculate progress percentage. */
  private int completedStages;

  /** The result of pre-async validation, stored to be used when the job executes. */
  private T preAsyncValidationResult;

  /** A consumer that modifies the HTTP response for this job, such as adding headers. */
  @Setter private Consumer<HttpServletResponse> responseModification;

  /** Indicates whether this job has been marked for deletion. */
  @Setter private boolean markedAsDeleted;

  /**
   * The last calculated progress percentage. When a job is at 100% that does not always indicate
   * that the job is actually finished. Most of the time, this indicates that a new stage has not
   * been submitted while the current stage is already completed. In that case just show the last
   * calculated percentage again.
   */
  @Setter private int lastProgress;

  /**
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

  public int getProgressPercentage() {
    return (completedStages * 100) / totalStages;
  }

  @SuppressWarnings("unchecked")
  public void setPreAsyncValidationResult(final Object preAsyncValidationResult) {
    try {
      this.preAsyncValidationResult = (T) preAsyncValidationResult;
    } catch (final ClassCastException e) {
      throw new InternalError("PreAsyncValidationResult casting failed.", e);
    }
  }

  public boolean isCancelled() {
    return result.isCancelled();
  }
}
