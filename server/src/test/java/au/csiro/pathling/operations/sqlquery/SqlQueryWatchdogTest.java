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

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import au.csiro.pathling.config.ServerConfiguration;
import au.csiro.pathling.config.SqlQueryConfiguration;
import jakarta.annotation.Nonnull;
import java.time.Duration;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for {@link SqlQueryWatchdog}. The Spark session is mocked so the test does not need a
 * running cluster - the watchdog's contract is to call {@code cancelJobGroup} once the timeout
 * elapses and to suppress that call when the watch is completed first.
 */
class SqlQueryWatchdogTest {

  private static final String JOB_GROUP_ID = "sqlquery-test-job";

  private SqlQueryWatchdog watchdog;

  @AfterEach
  void tearDown() {
    if (watchdog != null) {
      watchdog.shutdown();
    }
  }

  @Test
  void cancelsJobGroupAfterTimeout() {
    final SparkSession spark = mock(SparkSession.class);
    final SparkContext context = mock(SparkContext.class);
    when(spark.sparkContext()).thenReturn(context);
    watchdog = newWatchdog(spark, /* timeoutSeconds= */ 1);

    final SqlQueryWatchdog.Watch watch = watchdog.start(JOB_GROUP_ID);

    // Allow time for the schedule to fire; verify the cancellation lands on the job group.
    verify(context, timeout(5000)).cancelJobGroup(eq(JOB_GROUP_ID));
    await().atMost(Duration.ofSeconds(5)).until(watch::timedOut);
    assertThat(watch.timedOut()).isTrue();
  }

  @Test
  void cancellationIsCancelledOnNormalCompletion() throws InterruptedException {
    final SparkSession spark = mock(SparkSession.class);
    final SparkContext context = mock(SparkContext.class);
    when(spark.sparkContext()).thenReturn(context);
    watchdog = newWatchdog(spark, /* timeoutSeconds= */ 60);

    final SqlQueryWatchdog.Watch watch = watchdog.start(JOB_GROUP_ID);
    watch.complete();

    // Wait long enough that an unscheduled fire would have occurred if complete() did not cancel.
    Thread.sleep(500);
    verify(context, never()).cancelJobGroup(eq(JOB_GROUP_ID));
    assertThat(watch.timedOut()).isFalse();
  }

  @Nonnull
  private static SqlQueryWatchdog newWatchdog(
      @Nonnull final SparkSession spark, final long timeoutSeconds) {
    final SqlQueryConfiguration sqlQueryConfig = new SqlQueryConfiguration();
    sqlQueryConfig.setTimeoutSeconds(timeoutSeconds);
    final ServerConfiguration serverConfiguration = new ServerConfiguration();
    serverConfiguration.setSqlQuery(sqlQueryConfig);
    return new SqlQueryWatchdog(spark, serverConfiguration);
  }
}
