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
import java.util.Optional;

/**
 * Provides thread-local storage for the current async job context. This allows async operations to
 * access their associated job without needing to look it up from the servlet request, which may
 * have been recycled by the time the async task executes.
 *
 * @author John Grimes
 */
public final class AsyncJobContext {

  private static final ThreadLocal<Job<?>> CURRENT_JOB = new ThreadLocal<>();

  private AsyncJobContext() {
    // Utility class - prevent instantiation.
  }

  /**
   * Sets the current job in the thread-local context.
   *
   * @param job the job to set as the current job
   */
  public static void setCurrentJob(@Nonnull final Job<?> job) {
    CURRENT_JOB.set(job);
  }

  /**
   * Gets the current job from the thread-local context.
   *
   * @return an Optional containing the current job, or empty if no job is set
   */
  @Nonnull
  public static Optional<Job<?>> getCurrentJob() {
    return Optional.ofNullable(CURRENT_JOB.get());
  }

  /**
   * Clears the current job from the thread-local context. This should be called in a finally block
   * after async execution completes to prevent memory leaks.
   */
  public static void clear() {
    CURRENT_JOB.remove();
  }
}
