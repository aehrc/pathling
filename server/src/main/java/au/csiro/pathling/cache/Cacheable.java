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

package au.csiro.pathling.cache;

import jakarta.annotation.Nonnull;
import java.util.Optional;

public interface Cacheable {

  /**
   * @return the cache key for the object
   */
  Optional<String> getCacheKey();

  /**
   * Tests whether the current cache key matches another string.
   *
   * @param otherKey the string to be tested
   * @return true if the cache key matches the other string
   */
  boolean cacheKeyMatches(@Nonnull String otherKey);
}
