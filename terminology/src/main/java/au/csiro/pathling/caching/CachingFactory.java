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

import javax.annotation.Nonnull;
import org.apache.http.impl.client.cache.CacheConfig;
import org.apache.http.impl.client.cache.CachingHttpClientBuilder;

/**
 * A factory class for creating {@link CachingHttpClientBuilder} instance, pre-configured for with
 * some caching implementation.
 */
public interface CachingFactory {

  /**
   * Creates a {@link CachingHttpClientBuilder}, initialized with the provided {@link CacheConfig}.
   *
   * @param cacheConfig the cache config to use
   * @return the pre-configured {@link CachingHttpClientBuilder}
   */
  @Nonnull
  CachingHttpClientBuilder create(@Nonnull CacheConfig cacheConfig);
}
