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


@Slf4j
@Getter
public class ExecutorServiceResource implements AutoCloseable {

  public static final int DEFAULT_WAIT_FOR_SHUTDOWN_MILLIS = 1000;

  @Nonnull
  private final ExecutorService executorService;

  private final int waitForShutdownMillis;

  private ExecutorServiceResource(@Nonnull final ExecutorService executorService,
      final int waitForShutdownMillis) {
    this.executorService = executorService;
    this.waitForShutdownMillis = waitForShutdownMillis;
  }

  @Nonnull
  public static ExecutorServiceResource of(@Nonnull final ExecutorService executorService) {
    return of(executorService, DEFAULT_WAIT_FOR_SHUTDOWN_MILLIS);
  }

  public static ExecutorServiceResource of(@Nonnull final ExecutorService executorService,
      final int waitForShutdownMillis) {
    return new ExecutorServiceResource(executorService, waitForShutdownMillis);
  }

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
