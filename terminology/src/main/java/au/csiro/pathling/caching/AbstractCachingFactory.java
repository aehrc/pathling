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

import au.csiro.pathling.config.HttpCacheConfiguration;
import javax.annotation.Nonnull;
import org.apache.http.impl.client.cache.CacheConfig;
import org.apache.http.impl.client.cache.CachingHttpClientBuilder;
import org.apache.http.impl.client.cache.CachingHttpClients;


/**
 * Base class for implementing {@link CachingFactory}.
 */
public abstract class AbstractCachingFactory implements CachingFactory {

  @Nonnull
  protected final HttpCacheConfiguration config;

  public AbstractCachingFactory(@Nonnull final HttpCacheConfiguration config) {
    this.config = config;
  }

  @Nonnull
  @Override
  public CachingHttpClientBuilder create(@Nonnull final CacheConfig cacheConfig) {
    final CachingHttpClientBuilder builder = CachingHttpClients.custom()
        .setCacheConfig(cacheConfig);
    return configure(builder);
  }

  /**
   * Configures the specific caching mechanism in the {@link CachingHttpClientBuilder}.
   *
   * @param builder the builder to configure.
   * @return the {@link CachingHttpClientBuilder} with configured caching.
   */
  @Nonnull
  protected abstract CachingHttpClientBuilder configure(
      @Nonnull final CachingHttpClientBuilder builder);
}
