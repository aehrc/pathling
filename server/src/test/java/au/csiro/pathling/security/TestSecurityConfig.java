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

package au.csiro.pathling.security;

import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Primary;
import org.springframework.core.task.SyncTaskExecutor;
import org.springframework.core.task.TaskExecutor;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;

/**
 * @author Felix Naumann
 */
@TestConfiguration
public class TestSecurityConfig {

  @Bean
  @Primary
  public TaskExecutor syncExecutor() {
    return new SyncTaskExecutor();
  }

  @Bean
  @Primary
  public ThreadPoolTaskExecutor threadPoolTaskExecutor() {
    // Create a wrapper that delegates to SyncTaskExecutor
    return new ThreadPoolTaskExecutor() {
      private final SyncTaskExecutor sync = new SyncTaskExecutor();

      @Override
      public <T> Future<T> submit(Callable<T> task) {
        try {
          return CompletableFuture.completedFuture(task.call());
        } catch (Exception e) {
          throw new RuntimeException(e);
        }
      }

      @Override
      public Future<?> submit(Runnable task) {
        sync.execute(task);
        return CompletableFuture.completedFuture(null);
      }

      @Override
      public void execute(Runnable task) {
        sync.execute(task);
      }
    };
  }
}
