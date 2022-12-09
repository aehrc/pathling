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

import java.nio.file.Path;
import javax.annotation.Nonnull;
import org.apache.http.client.cache.HttpCacheStorage;
import org.infinispan.commons.marshall.JavaSerializationMarshaller;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.global.GlobalConfiguration;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.manager.DefaultCacheManager;

/**
 * An abstract {@link HttpCacheStorage} implementation that uses embedded Infinispan with persistent
 * storage. Requires a storage path, which it uses to store cache data and indexes.
 *
 * @author John Grimes
 */
public class InfinispanPersistentStorage extends InfinispanStorage {

  private static final String DATA_DIRECTORY = "data";
  private static final String INDEX_DIRECTORY = "index";

  public InfinispanPersistentStorage(@Nonnull final String storagePath) {
    final GlobalConfigurationBuilder globalConfigBuilder = new GlobalConfigurationBuilder();
    globalConfigBuilder.serialization()
        .marshaller(new JavaSerializationMarshaller())
        .allowList()
        .addRegexp(".*");
    final GlobalConfiguration globalConfig = globalConfigBuilder.build();
    final String dataLocation = Path.of(storagePath, DATA_DIRECTORY).toString();
    final String indexLocation = Path.of(storagePath, INDEX_DIRECTORY).toString();

    cacheManager = new DefaultCacheManager(globalConfig);
    cacheManager.defineConfiguration(CACHE_NAME,
        new ConfigurationBuilder()
            .persistence()
            .addSoftIndexFileStore()
            .dataLocation(dataLocation)
            .indexLocation(indexLocation)
            .build());
    cache = cacheManager.getCache(CACHE_NAME);
  }

}
