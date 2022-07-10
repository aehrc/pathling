/*
 * Copyright © 2018-2022, Commonwealth Scientific and Industrial Research
 * Organisation (CSIRO) ABN 41 687 119 230. Licensed under the CSIRO Open Source
 * Software Licence Agreement.
 */

package au.csiro.pathling.caching;

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
  boolean cacheKeyMatches(String otherKey);

}
