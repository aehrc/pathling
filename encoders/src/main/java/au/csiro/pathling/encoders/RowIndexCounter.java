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

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.Serializable;

/**
 * A thread-safe counter for tracking element positions within recursive tree traversals. Each
 * thread gets its own independent counter via {@link ThreadLocal}, ensuring that Spark tasks
 * running in parallel on different partitions do not interfere with each other.
 *
 * <p>This class is shared across partitions via Spark's {@code addReferenceObj()} mechanism in
 * codegen mode. Since reference objects are shared within an executor, {@link ThreadLocal} is
 * required to isolate mutable state per task thread.
 *
 * <p>This class is {@link Serializable} so that it survives Spark plan serialization to executors.
 * The {@link ThreadLocal} is eagerly initialized and re-initialized after deserialization via
 * {@link #readObject(ObjectInputStream)}.
 *
 * <p>Note: {@link ThreadLocal#remove()} is intentionally not called. The stored value is a single
 * {@code int[1]} (16 bytes) that is reset to zero each row via {@link #reset()}. When this object
 * becomes unreachable, the {@link ThreadLocal}'s weak-reference key is collected and the stale
 * entry is cleaned up lazily by subsequent {@link ThreadLocal} operations on the same thread.
 *
 * @author Piotr Szul
 */
@SuppressWarnings("java:S5164") // ThreadLocal.remove() not needed — see class Javadoc.
public class RowIndexCounter implements Serializable {

  private static final long serialVersionUID = 1L;

  private transient ThreadLocal<int[]> counter = ThreadLocal.withInitial(() -> new int[] {0});

  private void readObject(final ObjectInputStream in) throws IOException, ClassNotFoundException {
    in.defaultReadObject();
    counter = ThreadLocal.withInitial(() -> new int[] {0});
  }

  /**
   * Returns the current counter value without modifying it. Multiple calls between increments
   * return the same value, making this safe to use when the counter is referenced more than once
   * per element.
   *
   * @return the current counter value
   */
  public int get() {
    return counter.get()[0];
  }

  /**
   * Returns the current counter value and increments it. The first call after a {@link #reset()}
   * returns 0.
   *
   * @return the current counter value before incrementing
   */
  public int getAndIncrement() {
    return counter.get()[0]++;
  }

  /**
   * Increments the counter without returning a value. This is used to advance the counter after all
   * references to the current value have been evaluated.
   */
  public void increment() {
    counter.get()[0]++;
  }

  /**
   * Resets the counter to zero for the current thread. This should be called before evaluating each
   * top-level row to ensure the index sequence starts fresh.
   */
  public void reset() {
    counter.get()[0] = 0;
  }
}
