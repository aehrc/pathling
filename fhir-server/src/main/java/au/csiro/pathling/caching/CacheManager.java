/*
 * Copyright © 2018-2021, Commonwealth Scientific and Industrial Research
 * Organisation (CSIRO) ABN 41 687 119 230. Licensed under the CSIRO Open Source
 * Software Licence Agreement.
 */

package au.csiro.pathling.caching;

import java.util.Set;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Component;

/**
 * Used for the centralised invalidation of cached content.
 *
 * @author John Grimes
 */
@Component
@Profile("core")
@Slf4j
public class CacheManager {

  private final Set<? extends Cacheable> underManagement;

  /**
   * @param cacheables A set of {@link Cacheable} implementations
   */
  public CacheManager(final Set<? extends Cacheable> cacheables) {
    underManagement = cacheables;
  }

  /**
   * Invalidates the cache of all cachables under management.
   */
  public void invalidateAll() {
    underManagement.forEach(Cacheable::invalidateCache);
  }

}
