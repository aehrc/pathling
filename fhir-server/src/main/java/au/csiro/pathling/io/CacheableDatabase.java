/*
 * Copyright 2023 Commonwealth Scientific and Industrial Research
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

package au.csiro.pathling.io;

import static au.csiro.pathling.io.FileSystemPersistence.safelyJoinPaths;

import au.csiro.pathling.caching.Cacheable;
import au.csiro.pathling.config.StorageConfiguration;
import au.csiro.pathling.encoders.FhirEncoders;
import jakarta.annotation.Nonnull;
import java.util.Optional;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.SparkSession;
import org.springframework.context.annotation.Profile;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.springframework.stereotype.Component;

/**
 * A database implementation that facilitates caching of results.
 *
 * @author John Grimes
 * @author Piotr Szul
 */
@Component
@Profile("(core | import) & !ga4gh")
@Slf4j
public class CacheableDatabase extends Database implements Cacheable {

  @Nonnull
  protected final ThreadPoolTaskExecutor executor;

  /**
   * @param configuration a {@link StorageConfiguration} object which controls the behaviour of the
   * database
   * @param spark a {@link SparkSession} for interacting with Spark
   * @param fhirEncoders {@link FhirEncoders} object for creating empty datasets
   * @param executor a {@link ThreadPoolTaskExecutor} for executing asynchronous tasks
   */
  public CacheableDatabase(@Nonnull final StorageConfiguration configuration,
      @Nonnull final SparkSession spark, @Nonnull final FhirEncoders fhirEncoders,
      @Nonnull final ThreadPoolTaskExecutor executor) {
    super(spark, fhirEncoders, new CacheableFileSystemPersistence(spark,
        safelyJoinPaths(configuration.getWarehouseUrl(), configuration.getDatabaseName()), executor,
        configuration.getCompactionThreshold()), configuration.getCacheDatasets());
    this.executor = executor;
  }

  @Override
  public Optional<String> getCacheKey() {
    return ((CacheableFileSystemPersistence) persistence).getCacheKey();
  }

  @Override
  public boolean cacheKeyMatches(@Nonnull final String otherKey) {
    return ((CacheableFileSystemPersistence) persistence).cacheKeyMatches(otherKey);
  }

}
