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

package au.csiro.pathling.test;

import jakarta.annotation.Nonnull;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.spark.SparkContext;
import org.apache.spark.scheduler.SparkListener;
import org.apache.spark.scheduler.SparkListenerJobEnd;

/**
 * A Spark listener that counts the jobs that end while it is registered, for tests that an action
 * reads no data.
 */
public final class JobCountingListener extends SparkListener {

  @Nonnull private final AtomicInteger jobs = new AtomicInteger();

  private JobCountingListener() {}

  @Override
  public void onJobEnd(@Nonnull final SparkListenerJobEnd jobEnd) {
    jobs.incrementAndGet();
  }

  /**
   * Runs an action and returns the number of Spark jobs that ended while it ran.
   *
   * <p>The listener bus is drained before the listener is added and again after the action, so that
   * a job from earlier work is not counted and a job of the action is not missed.
   *
   * @param context the Spark context the action runs in
   * @param action the action to run
   * @return the number of jobs that ended while the action ran
   * @throws TimeoutException if the listener bus is not drained in time
   */
  public static int jobsRunBy(@Nonnull final SparkContext context, @Nonnull final Runnable action)
      throws TimeoutException {
    final JobCountingListener listener = new JobCountingListener();
    context.listenerBus().waitUntilEmpty();
    context.addSparkListener(listener);
    try {
      action.run();
      context.listenerBus().waitUntilEmpty();
    } finally {
      context.removeSparkListener(listener);
    }
    return listener.jobs.get();
  }
}
