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

package au.csiro.pathling.export.utils;


import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nonnull;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;


/**
 * A resource that manages the lifecycle of an {@link ExecutorService}. This is to be uses to wrap
 * an {@link ExecutorService}. in a try-with-resources block to ensure that it is shut down
 * correctly.
 */
@Slf4j
@Getter
public class ExecutorServiceResource implements AutoCloseable {

  /**
   * The default number of milliseconds to wait for the executor service to terminate its tasks.
   */
  public static final int DEFAULT_WAIT_FOR_SHUTDOWN_MILLIS = 1000;

  @Nonnull
  private final ExecutorService executorService;

  private final int waitForShutdownMillis;

  /**
   * Creates a new {@link ExecutorServiceResource} that will wait for the given number of
   * milliseconds for the executor service to shut down.
   *
   * @param executorService the executor service to manage
   * @param waitForShutdownMillis the number of milliseconds to wait for the executor service to to
   * terminate its tasks.
   */
  private ExecutorServiceResource(@Nonnull final ExecutorService executorService,
      final int waitForShutdownMillis) {
    this.executorService = executorService;
    this.waitForShutdownMillis = waitForShutdownMillis;
  }

  /**
   * Creates a new {@link ExecutorServiceResource} with the default wait time.
   *
   * @param executorService the executor service to manage
   * @return a new {@link ExecutorServiceResource}
   */
  @Nonnull
  public static ExecutorServiceResource of(@Nonnull final ExecutorService executorService) {
    return of(executorService, DEFAULT_WAIT_FOR_SHUTDOWN_MILLIS);
  }

  /**
   * Creates a new {@link ExecutorServiceResource} that will wait for the given number of
   * milliseconds for the executor service task to terminate.
   *
   * @param executorService the executor service to manage
   * @param waitForShutdownMillis the number of milliseconds to wait for the executor service to
   * shut down
   * @return a new {@link ExecutorServiceResource}
   */
  public static ExecutorServiceResource of(@Nonnull final ExecutorService executorService,
      final int waitForShutdownMillis) {
    return new ExecutorServiceResource(executorService, waitForShutdownMillis);
  }

  /**
   * Shuts down the executor service in the safe manner, attempting to terminate any running tasks
   * first and then forcibly shutting down the executor service.
   */
  @Override
  public void close() {
    log.debug("Shutting down ExecutorService {}", executorService);
    executorService.shutdownNow();
    try {
      if (!executorService.awaitTermination(waitForShutdownMillis, TimeUnit.MILLISECONDS)) {
        log.warn("ExecutorService {} did not terminate within {} ms", executorService,
            waitForShutdownMillis);
      }
    } catch (final InterruptedException __) {
      executorService.shutdownNow();
      Thread.currentThread().interrupt();
    }
  }
}
