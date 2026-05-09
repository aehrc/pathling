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

package au.csiro.pathling.operations.sqlquery;

import au.csiro.pathling.config.ServerConfiguration;
import au.csiro.pathling.config.SqlQueryConfiguration;
import jakarta.annotation.Nonnull;
import jakarta.annotation.PreDestroy;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.SparkSession;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

/**
 * Schedules a wall-clock timeout against each {@code $sqlquery-run} query and cancels the
 * associated Spark job group when the timeout fires. Used to defend the synchronous {@code
 * $sqlquery-run} surface against queries that consume an unbounded share of compute resources - for
 * example, generators chained with array-builder functions that produce few output rows but
 * traverse very large intermediate sets, which the row cap cannot short-circuit.
 *
 * <p>Mirrors the cancellation pattern used by {@code AsyncAspect}: {@code setJobGroup(...,
 * interruptOnCancel=true)} pairs with {@code cancelJobGroup}, which propagates cancellation to
 * in-flight Spark task threads as a thrown exception.
 *
 * @author John Grimes
 */
@Slf4j
@Component
public class SqlQueryWatchdog {

  @Nonnull private final SparkSession sparkSession;

  @Nonnull private final SqlQueryConfiguration config;

  @Nonnull private final ScheduledExecutorService scheduler;

  /**
   * Constructs a new SqlQueryWatchdog.
   *
   * @param sparkSession the Spark session whose job groups will be cancelled on timeout
   * @param serverConfiguration the server configuration, used to resolve the timeout value
   */
  @Autowired
  public SqlQueryWatchdog(
      @Nonnull final SparkSession sparkSession,
      @Nonnull final ServerConfiguration serverConfiguration) {
    this.sparkSession = sparkSession;
    this.config = serverConfiguration.getSqlQuery();
    this.scheduler =
        Executors.newSingleThreadScheduledExecutor(
            r -> {
              final Thread t = new Thread(r, "sqlquery-watchdog");
              t.setDaemon(true);
              return t;
            });
  }

  /**
   * Starts a watchdog for the given Spark job group. The returned {@link Watch} must be completed
   * by the caller in a {@code finally} block.
   *
   * @param jobGroupId the Spark job group id to cancel if the timeout fires
   * @return a handle that exposes whether the timeout fired and lets the caller cancel the
   *     scheduled cancellation when the query completes normally
   */
  @Nonnull
  public Watch start(@Nonnull final String jobGroupId) {
    final long timeoutSeconds = config.getTimeoutSeconds();
    final AtomicBoolean timedOut = new AtomicBoolean(false);
    final ScheduledFuture<?> task =
        scheduler.schedule(
            () -> {
              timedOut.set(true);
              log.warn(
                  "$sqlquery-run timeout fired for jobGroupId={} after {} seconds; cancelling.",
                  jobGroupId,
                  timeoutSeconds);
              try {
                sparkSession.sparkContext().cancelJobGroup(jobGroupId);
              } catch (final RuntimeException e) {
                log.warn("Failed to cancel job group {}: {}", jobGroupId, e.getMessage());
              }
            },
            timeoutSeconds,
            TimeUnit.SECONDS);
    return new Watch(task, timedOut);
  }

  /** Shuts down the scheduler when the bean is destroyed. */
  @PreDestroy
  public void shutdown() {
    scheduler.shutdownNow();
  }

  /** Handle returned by {@link #start(String)}. */
  public static final class Watch {

    @Nonnull private final ScheduledFuture<?> task;

    @Nonnull private final AtomicBoolean timedOut;

    Watch(@Nonnull final ScheduledFuture<?> task, @Nonnull final AtomicBoolean timedOut) {
      this.task = task;
      this.timedOut = timedOut;
    }

    /**
     * Cancels the scheduled cancellation. Idempotent. Safe to call after the timeout has already
     * fired.
     */
    public void complete() {
      task.cancel(false);
    }

    /** Returns true if the timeout fired before {@link #complete()} was called. */
    public boolean timedOut() {
      return timedOut.get();
    }
  }
}
