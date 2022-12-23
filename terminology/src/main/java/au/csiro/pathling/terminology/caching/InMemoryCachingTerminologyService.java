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

package au.csiro.pathling.terminology.caching;

import au.csiro.pathling.config.HttpClientCachingConfiguration;
import au.csiro.pathling.fhir.TerminologyClient;
import java.io.Closeable;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import org.infinispan.Cache;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.manager.DefaultCacheManager;
import org.infinispan.manager.EmbeddedCacheManager;

/**
 * A terminology service that uses embedded Infinispan to cache results in-memory.
 *
 * @author John Grimes
 */
public class InMemoryCachingTerminologyService extends CachingTerminologyService {

  public InMemoryCachingTerminologyService(@Nonnull final TerminologyClient terminologyClient,
      @Nullable final Closeable toClose,
      @Nonnull final HttpClientCachingConfiguration configuration) {
    super(terminologyClient, toClose, configuration);
  }

  @Override
  protected EmbeddedCacheManager buildCacheManager() {
    return new DefaultCacheManager();
  }

  @Override
  protected Cache<Integer, ?> buildCache(@Nonnull final EmbeddedCacheManager cacheManager,
      @Nonnull final String cacheName) {
    final Configuration cacheConfig = new ConfigurationBuilder()
        .memory()
        .maxCount(configuration.getMaxEntries())
        .maxSize(configuration.getMaxObjectSize() + "B")
        .build();
   
    cacheManager.defineConfiguration(cacheName, cacheConfig);
    return cacheManager.getCache(cacheName);
  }

}
