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

package au.csiro.pathling.schema.annotation;

import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.Function;

/**
 * A computation memoised in a least-recently-used cache held per thread, so that several
 * annotations computed from one value share one computation of it (decision 145, design §4.6).
 *
 * <p>A Spark task evaluates its rows one at a time on one thread, so a cache per thread needs no
 * locking. The cache holds more than the last input, because a plan can read the annotations in
 * separate passes over an array, reading one annotation of every element before the next: such a
 * pass shares the computation only where the cache is at least as long as the array.
 *
 * <p>The results do not depend on the cache. A cached value must be immutable, and is keyed by the
 * input it was computed from: a date's text, or a list of a quantity's value, system and code. The
 * input must not change once it is a key. A computation that fails is not cached, so every call on
 * the same input fails as the uncached computation would. A miss costs one more computation, never
 * a different result.
 *
 * @param <K> the type of the input
 * @param <V> the type of the computed value
 */
public final class PerThreadLruCache<K, V> {

  @Nonnull private final Function<K, V> computation;

  @Nonnull private final ThreadLocal<LruMap<K, V>> entries;

  @Nonnull private final LongAdder computations = new LongAdder();

  /**
   * Creates a cache.
   *
   * @param capacity the number of entries each thread's cache holds
   * @param computation the computation, which returns an immutable value and may throw
   */
  public PerThreadLruCache(final int capacity, @Nonnull final Function<K, V> computation) {
    this.computation = computation;
    this.entries = ThreadLocal.withInitial(() -> new LruMap<>(capacity));
  }

  /**
   * Returns the computation of an input, from this thread's cache where it holds it, and computed
   * and cached otherwise.
   *
   * @param input the input
   * @return the computed value, or null where the computation returns null, which is not cached
   */
  @Nullable
  public V get(@Nonnull final K input) {
    final LruMap<K, V> cache = entries.get();
    // The annotations of one value are computed one after the other, so the input is most often the
    // last one read, which is compared before the input is hashed for the map.
    if (input.equals(cache.lastInput)) {
      return cache.lastValue;
    }
    V value = cache.get(input);
    if (value == null) {
      computations.increment();
      value = computation.apply(input);
      if (value == null) {
        return null;
      }
      cache.put(input, value);
    }
    cache.lastInput = input;
    cache.lastValue = value;
    return value;
  }

  /**
   * Returns the number of times the computation has been run in this JVM, on any thread, which is
   * the number of misses. It shows how often the cache spares a computation.
   *
   * @return the number of computations
   */
  public long getComputationCount() {
    return computations.sum();
  }
}
