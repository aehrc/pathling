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
