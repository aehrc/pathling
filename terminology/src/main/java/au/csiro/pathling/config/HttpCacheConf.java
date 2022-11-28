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

package au.csiro.pathling.config;

import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.http.impl.client.cache.CacheConfig;
import javax.annotation.Nullable;
import javax.validation.constraints.Min;
import javax.validation.constraints.NotNull;
import java.io.Serializable;
import java.util.Map;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor(access = AccessLevel.PRIVATE)
public class HttpCacheConf implements Serializable {

  private static final long serialVersionUID = -3030386957343963899L;

  public static final boolean DEF_ENABLED = true;
  public static final int DEF_MAX_CACHE_ENTRIES = 10_000;
  public static final long DEF_MAX_OBJECT_SIZE = 64_000L;
  public static final String DEF_STORAGE_TYPE = "memory";

  /**
   * Enables client side caching of REST requests.
   */
  @NotNull
  @Builder.Default
  private boolean enabled = DEF_ENABLED;

  /**
   * Sets the maximum number of cache entries the cache will retain. See also: {@link
   * CacheConfig.Builder#setMaxCacheEntries(int)} ()}
   */
  @NotNull
  @Min(0)
  @Builder.Default
  private int maxCacheEntries = DEF_MAX_CACHE_ENTRIES;

  /**
   * Sets the maximum number of cache entries the cache will retain. See also: {@link
   * CacheConfig.Builder#setMaxObjectSize(long)} )}
   */
  @Min(0)
  @NotNull
  @Builder.Default
  private long maxObjectSize = DEF_MAX_OBJECT_SIZE;

  @NotNull
  @Builder.Default
  private String storageType = DEF_STORAGE_TYPE;

  @Nullable
  private Map<String, String> storage;

  public static HttpCacheConf defaults() {
    return HttpCacheConf.builder().build();
  }

  public static HttpCacheConf disabled() {
    return HttpCacheConf.builder().enabled(false).build();
  }

}
