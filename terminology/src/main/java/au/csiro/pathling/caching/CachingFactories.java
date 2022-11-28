/*
 * Copyright 2022 Commonwealth Scientific and Industrial Research
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

package au.csiro.pathling.caching;

import java.io.File;
import javax.annotation.Nonnull;
import au.csiro.pathling.errors.InvalidConfigError;
import org.apache.http.impl.client.cache.CacheConfig;
import org.apache.http.impl.client.cache.CachingHttpClientBuilder;

/**
 * The implementation of standarty types of caching mechanisms.
 */
public final class CachingFactories {

  public static final String MEMORY_STORAGE = "memory";
  public static final String DISK_STORAGE = "disk";

  public static final String DISK_CACHE_DIR = "cacheDir";

  private static class MemoryCachingFactory extends AbstractCachingFactoryBase {

    @Nonnull
    @Override
    protected CachingHttpClientBuilder configure(@Nonnull final CachingHttpClientBuilder builder,
        @Nonnull final Config config, @Nonnull final CacheConfig cacheConfig) {
      return builder;
    }
  }

  private static class DiskCachingFactory extends AbstractCachingFactoryBase {

    @Nonnull
    @Override
    protected CachingHttpClientBuilder configure(@Nonnull final CachingHttpClientBuilder builder,
        @Nonnull final Config config, @Nonnull final CacheConfig cacheConfig) {
      return builder.setCacheDir(new File(config.required(DISK_CACHE_DIR)));
    }
  }

  private CachingFactories() {
    // utility class
  }

  /**
   * Creates a {@link CachingFactory} for specified storage type.
   *
   * @param storageType the type of caching storage.
   * @return the {@link CachingFactory} for specified storage type.
   */
  @Nonnull
  public static CachingFactory of(@Nonnull final String storageType) {
    if (MEMORY_STORAGE.equals(storageType)) {
      return new MemoryCachingFactory();
    } else if (DISK_STORAGE.equals(storageType)) {
      return new DiskCachingFactory();
    } else {
      throw new InvalidConfigError("Cannot configure cache with storageType: " + storageType);
    }
  }

}
