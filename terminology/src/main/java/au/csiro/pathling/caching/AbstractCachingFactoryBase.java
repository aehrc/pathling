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

import static java.util.Objects.nonNull;
import static java.util.Objects.requireNonNull;
import static java.util.Objects.requireNonNullElseGet;

import java.util.Collections;
import java.util.Map;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import au.csiro.pathling.errors.InvalidConfigError;
import org.apache.http.impl.client.cache.CacheConfig;
import org.apache.http.impl.client.cache.CachingHttpClientBuilder;
import org.apache.http.impl.client.cache.CachingHttpClients;


/**
 * Base class for implementing {@link CachingFactory}.
 */
public abstract class AbstractCachingFactoryBase implements CachingFactory {

  protected static class Config {

    @Nonnull
    private final Map<String, String> properties;

    public Config(@Nullable final Map<String, String> properties) {
      this.properties = nonNull(properties)
                        ? requireNonNull(properties)
                        : Collections.emptyMap();
    }

    @Nonnull
    public String required(@Nonnull final String key) {
      return requireNonNullElseGet(properties.get(key), () -> {
        throw new InvalidConfigError("Required config property missing: " + key);
      });
    }

    @Nonnull
    public String optional(@Nonnull final String key, @Nonnull final String defValue) {
      return properties.getOrDefault(key, defValue);
    }
  }


  @Nonnull
  @Override
  public CachingHttpClientBuilder create(@Nonnull final CacheConfig cacheConfig,
      @Nullable final Map<String, String> storageProperties) {
    final CachingHttpClientBuilder builder = CachingHttpClients.custom()
        .setCacheConfig(cacheConfig);
    return configure(builder, new Config(storageProperties), cacheConfig);
  }


  /**
   * Configures the specific caching mechanism in the {@link CachingHttpClientBuilder}.
   *
   * @param builder the builder to configure.
   * @param config the configuration for the caching mechanism.
   * @return the {@link CachingHttpClientBuilder} with configured caching.
   */
  @Nonnull
  protected abstract CachingHttpClientBuilder configure(
      @Nonnull final CachingHttpClientBuilder builder,
      @Nonnull final Config config, @Nonnull final CacheConfig cacheConfig);
}
