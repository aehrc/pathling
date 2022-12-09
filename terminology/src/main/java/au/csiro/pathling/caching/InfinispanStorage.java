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

import java.io.Closeable;
import java.io.IOException;
import javax.annotation.Nonnull;
import org.apache.http.client.cache.HttpCacheEntry;
import org.apache.http.client.cache.HttpCacheStorage;
import org.apache.http.client.cache.HttpCacheUpdateCallback;
import org.infinispan.Cache;
import org.infinispan.manager.DefaultCacheManager;

/**
 * An abstract {@link HttpCacheStorage} implementation that uses embedded Infinispan as the
 * underlying storage. Subclasses need to initialize the cache and cache manager.
 *
 * @author John Grimes
 */
public abstract class InfinispanStorage implements HttpCacheStorage, Closeable {

  protected static final String CACHE_NAME = "terminology";

  @Nonnull
  protected DefaultCacheManager cacheManager;

  @Nonnull
  protected Cache<String, HttpCacheEntry> cache;

  @Override
  public void putEntry(final String key, final HttpCacheEntry entry) {
    cache.put(key, entry);
  }

  @Override
  public HttpCacheEntry getEntry(final String key) {
    return cache.get(key);
  }

  @Override
  public void removeEntry(final String key) {
    cache.remove(key);
  }

  @Override
  public void updateEntry(final String key, final HttpCacheUpdateCallback callback) {
    cache.compute(key, (k, v) -> {
      try {
        return callback.update(v);
      } catch (final IOException e) {
        throw new RuntimeException(e);
      }
    });
  }

  @Override
  public void close() throws IOException {
    cacheManager.close();
  }

}
