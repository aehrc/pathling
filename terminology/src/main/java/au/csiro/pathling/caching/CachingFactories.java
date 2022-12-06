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

import static au.csiro.pathling.utilities.Preconditions.check;
import static au.csiro.pathling.utilities.Preconditions.checkNotNull;

import au.csiro.pathling.config.HttpCacheConfiguration;
import au.csiro.pathling.config.HttpCacheConfiguration.StorageType;
import javax.annotation.Nonnull;
import org.apache.http.impl.client.cache.CachingHttpClientBuilder;

/**
 * The implementation of standard types of caching mechanisms.
 */
public final class CachingFactories {

  /**
   * A factory that can build a {@link CachingHttpClientBuilder} that uses an in-memory cache.
   */
  private static class MemoryCachingFactory extends BaseCachingFactory {

    public MemoryCachingFactory(@Nonnull final HttpCacheConfiguration config) {
      super(config);
    }

    @Nonnull
    @Override
    protected CachingHttpClientBuilder configure(@Nonnull final CachingHttpClientBuilder builder) {
      return builder.setHttpCacheStorage(new InfinispanInMemoryStorage());
    }
  }

  /**
   * A factory that can build a {@link CachingHttpClientBuilder} that uses a disk cache.
   */
  private static class DiskCachingFactory extends BaseCachingFactory {

    public DiskCachingFactory(@Nonnull final HttpCacheConfiguration config) {
      super(config);
      check(config.getStorageType().equals(HttpCacheConfiguration.StorageType.DISK));
    }

    @Nonnull
    @Override
    protected CachingHttpClientBuilder configure(@Nonnull final CachingHttpClientBuilder builder) {
      checkNotNull(config.getStoragePath());
      return builder.setHttpCacheStorage(new InfinispanPersistentStorage(config.getStoragePath()));
    }

  }

  private CachingFactories() {
    // utility class
  }

  /**
   * Returns a {@link CachingFactory} based upon the {@link HttpCacheConfiguration}.
   *
   * @param config the configuration for the caching mechanism
   * @return a {@link CachingFactory} for the specified configuration
   */
  @Nonnull
  public static CachingFactory of(@Nonnull final HttpCacheConfiguration config) {
    if (config.getStorageType().equals(StorageType.MEMORY)) {
      return new MemoryCachingFactory(config);
    } else if (config.getStorageType().equals(StorageType.DISK)) {
      return new DiskCachingFactory(config);
    } else {
      throw new AssertionError(
          "HTTP cache configuration encountered with invalid storage type: " + config);
    }
  }

}
