/*
 * Copyright © 2018-2022, Commonwealth Scientific and Industrial Research
 * Organisation (CSIRO) ABN 41 687 119 230. Licensed under the CSIRO Open Source
 * Software Licence Agreement.
 */

package au.csiro.pathling.caching;

import java.util.Date;
import javax.annotation.Nonnull;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.SparkSession;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Component;

/**
 * Used for the centralized invalidation of cached content.
 *
 * @author John Grimes
 */
@Component
@Profile("server")
@Slf4j
public class CacheInvalidator {

  @Nonnull
  private final EntityTagValidator entityTagValidator;

  @Nonnull
  private final SparkSession spark;

  /**
   * @param entityTagValidator an {@link EntityTagValidator} that can be reset
   * @param spark a {@link SparkSession} that can have its cache cleared
   */
  public CacheInvalidator(@Nonnull final EntityTagValidator entityTagValidator,
      @Nonnull final SparkSession spark) {
    this.entityTagValidator = entityTagValidator;
    this.spark = spark;
  }

  /**
   * Invalidates all caches.
   */
  public void invalidateAll() {
    log.info("Invalidating caches");
    entityTagValidator.expire(new Date().getTime());
    spark.sqlContext().clearCache();
  }

}
