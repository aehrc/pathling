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
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

/**
 * The externally visible status of an asynchronous job, derived from the job's {@link Future}
 * rather than stored. The {@link Future} is the single source of truth for a job's state.
 *
 * @author John Grimes
 */
public enum JobStatus {

  /** The job's background work has not yet finished. */
  IN_PROGRESS("in-progress"),

  /** The job's background work finished normally. */
  COMPLETED("completed"),

  /** The job's background work finished by throwing an exception. */
  FAILED("failed"),

  /** The job was cancelled before its background work finished. */
  CANCELLED("cancelled");

  @Nonnull private final String code;

  JobStatus(@Nonnull final String code) {
    this.code = code;
  }

  /**
   * Returns the stable wire code for this status, used in the {@code $jobs} operation response.
   *
   * @return the wire code (for example {@code in-progress})
   */
  @Nonnull
  public String getCode() {
    return code;
  }

  /**
   * Derives the status of a job from its result {@link Future}. Cancellation is checked first, then
   * completion; {@code get()} is only invoked once the future is done, so derivation never blocks.
   *
   * @param result the job's result future
   * @return the derived {@link JobStatus}
   */
  @Nonnull
  public static JobStatus fromResult(@Nonnull final Future<?> result) {
    if (result.isCancelled()) {
      return CANCELLED;
    }
    if (!result.isDone()) {
      return IN_PROGRESS;
    }
    try {
      // The future is done and not cancelled, so this returns immediately without blocking.
      result.get();
      return COMPLETED;
    } catch (final CancellationException e) {
      // A cancellation that raced with completion is still a cancellation.
      return CANCELLED;
    } catch (final ExecutionException e) {
      return FAILED;
    } catch (final InterruptedException e) {
      // Restore the interrupt flag and treat the job as failed rather than swallowing the signal.
      Thread.currentThread().interrupt();
      return FAILED;
    }
  }
}
