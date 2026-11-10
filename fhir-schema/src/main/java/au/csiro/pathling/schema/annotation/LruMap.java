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

import jakarta.annotation.Nullable;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * A map that holds at most a fixed number of entries, evicting the one least recently read. It is
 * not safe for use by more than one thread, and {@link PerThreadLruCache} keeps one per thread.
 *
 * @param <K> the type of the keys
 * @param <V> the type of the values
 */
final class LruMap<K, V> extends LinkedHashMap<K, V> {

  private static final long serialVersionUID = 1L;

  private final int capacity;

  /** The input last read from the map, which is also its most recently read entry. */
  @Nullable transient K lastInput;

  /** The value of the input last read. */
  @Nullable transient V lastValue;

  /**
   * Creates an empty map.
   *
   * @param capacity the number of entries it holds
   */
  LruMap(final int capacity) {
    // Ordered by access, so that the eldest entry is the one least recently read.
    super(capacity * 2, 0.75f, true);
    this.capacity = capacity;
  }

  @Override
  protected boolean removeEldestEntry(final Map.Entry<K, V> eldest) {
    return size() > capacity;
  }
}
