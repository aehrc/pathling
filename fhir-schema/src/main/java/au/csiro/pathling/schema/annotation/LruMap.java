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
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Objects;
import java.util.function.Function;

/**
 * A map that holds at most a fixed number of entries, evicting the one least recently read. It is
 * not safe for use by more than one thread, and {@link PerThreadLruCache} keeps one per thread.
 *
 * @param <K> the type of the keys
 * @param <V> the type of the values
 */
final class LruMap<K, V> {

  private final int capacity;

  /** The entries, ordered by access, so that the eldest entry is the one least recently read. */
  @Nonnull private final LinkedHashMap<K, V> entries;

  /** The input last read from the map, which is also its most recently read entry. */
  @Nullable private K lastInput;

  /** The value of the input last read. */
  @Nullable private V lastValue;

  /**
   * Creates an empty map.
   *
   * @param capacity the number of entries it holds
   */
  LruMap(final int capacity) {
    this.capacity = capacity;
    this.entries = new LinkedHashMap<>(capacity * 2, 0.75f, true);
  }

  /**
   * Returns the value of an input, from the map where it holds it, and computed and held otherwise,
   * evicting the entry least recently read where the map then holds too many.
   *
   * @param input the input
   * @param computation computes the value of an input the map does not hold, never null
   * @return the value
   */
  @Nonnull
  V get(@Nonnull final K input, @Nonnull final Function<K, V> computation) {
    // The annotations of one value are computed one after the other, so the input is most often the
    // last one read, which is compared before the input is hashed for the map.
    if (input.equals(lastInput)) {
      // The last value is set with the last input, so it is not null once the input matches.
      return Objects.requireNonNull(lastValue);
    }
    final V value = entries.computeIfAbsent(input, computation);
    if (entries.size() > capacity) {
      final Iterator<K> eldest = entries.keySet().iterator();
      eldest.next();
      eldest.remove();
    }
    lastInput = input;
    lastValue = value;
    return value;
  }
}
