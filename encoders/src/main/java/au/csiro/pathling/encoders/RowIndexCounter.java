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

package au.csiro.pathling.encoders;

import java.io.Serializable;

/**
 * A thread-safe counter for tracking element positions within recursive tree traversals. Each
 * thread gets its own independent counter via {@link ThreadLocal}, ensuring that Spark tasks
 * running in parallel on different partitions do not interfere with each other.
 *
 * <p>This class is {@link Serializable} so that it survives Spark plan serialization to executors.
 * The {@link ThreadLocal} state is transient and lazily re-initialized after deserialization.
 *
 * @author Piotr Szul
 */
public class RowIndexCounter implements Serializable {

  private static final long serialVersionUID = 1L;

  @SuppressWarnings("TransientFieldNotInitialized")
  private transient ThreadLocal<int[]> counter;

  private ThreadLocal<int[]> getCounter() {
    if (counter == null) {
      counter = ThreadLocal.withInitial(() -> new int[] {0});
    }
    return counter;
  }

  /**
   * Returns the current counter value and increments it. The first call after a {@link #reset()}
   * returns 0.
   *
   * @return the current counter value before incrementing
   */
  public int getAndIncrement() {
    return getCounter().get()[0]++;
  }

  /**
   * Resets the counter to zero for the current thread. This should be called before evaluating each
   * top-level row to ensure the index sequence starts fresh.
   */
  public void reset() {
    getCounter().get()[0] = 0;
  }
}
