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

package au.csiro.pathling.utilities;

import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.Closeable;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

class ObjectHolderTest {

  /** A closeable value that records whether it was closed. */
  private static final class TrackingValue implements Closeable {

    private boolean closed;

    @Override
    public void close() {
      closed = true;
    }
  }

  @Test
  void singletonReusesInstanceForSameConfiguration() {
    final AtomicInteger constructions = new AtomicInteger();
    final ObjectHolder<String, TrackingValue> holder =
        ObjectHolder.singleton(
            config -> {
              constructions.incrementAndGet();
              return new TrackingValue();
            },
            false,
            0);

    final TrackingValue first = holder.getOrCreate("a");
    final TrackingValue second = holder.getOrCreate("a");

    // The same configuration must return the memoised instance built only once.
    assertSame(first, second);
    assertTrue(constructions.get() == 1);
  }

  @Test
  void singletonRejectsDifferentConfiguration() {
    final ObjectHolder<String, TrackingValue> holder =
        ObjectHolder.singleton(config -> new TrackingValue(), false, 0);

    holder.getOrCreate("a");

    // A single-instance holder may hold only one configuration for the life of the process.
    assertThrows(AssertionError.class, () -> holder.getOrCreate("b"));
  }

  @Test
  void perConfigurationReusesInstancePerConfiguration() {
    final AtomicInteger constructions = new AtomicInteger();
    final ObjectHolder<String, TrackingValue> holder =
        ObjectHolder.perConfiguration(
            config -> {
              constructions.incrementAndGet();
              return new TrackingValue();
            },
            false,
            0);

    final TrackingValue firstA = holder.getOrCreate("a");
    final TrackingValue secondA = holder.getOrCreate("a");

    // Repeated lookups of the same configuration return the memoised instance.
    assertSame(firstA, secondA);
    assertTrue(constructions.get() == 1);
  }

  @Test
  void perConfigurationHoldsDistinctInstancesForDistinctConfigurations() {
    final ObjectHolder<String, TrackingValue> holder =
        ObjectHolder.perConfiguration(config -> new TrackingValue(), false, 0);

    final TrackingValue valueA = holder.getOrCreate("a");
    final TrackingValue valueB = holder.getOrCreate("b");

    // Unlike a single-instance holder, distinct configurations coexist in the same process.
    assertNotSame(valueA, valueB);
    assertSame(valueA, holder.getOrCreate("a"));
    assertSame(valueB, holder.getOrCreate("b"));
  }

  @Test
  void perConfigurationResetClosesAndRebuildsInstances() {
    final ObjectHolder<String, TrackingValue> holder =
        ObjectHolder.perConfiguration(config -> new TrackingValue(), false, 0);

    final TrackingValue before = holder.getOrCreate("a");
    holder.reset();

    // Reset closes every held instance so resources are released.
    assertTrue(before.closed);

    // A subsequent lookup builds a fresh instance rather than returning the closed one.
    final TrackingValue after = holder.getOrCreate("a");
    assertNotSame(before, after);
  }
}
