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
public class HttpCacheConfiguration implements Serializable {

  private static final long serialVersionUID = -3030386957343963899L;
  /**
   * Enables client side caching of REST requests.
   */
  @NotNull
  private boolean enabled;

  /**
   * Sets the maximum number of cache entries the cache will retain. See also: {@link
   * CacheConfig.Builder#setMaxCacheEntries(int)} ()}
   */
  @NotNull
  @Min(0)
  private int maxCacheEntries;

  /**
   * Sets the maximum number of cache entries the cache will retain. See also: {@link
   * CacheConfig.Builder#setMaxObjectSize(long)} )}
   */
  @Min(0)
  @NotNull
  private long maxObjectSize;

  @NotNull
  private String storageType;

  @Nullable
  private Map<String, String> storage;

  public static HttpCacheConfiguration defaults() {
    return HttpCacheConfiguration.builder().enabled(true).storageType("memory").build();
  }
}
