package au.csiro.pathling.config;

import lombok.Data;
import org.apache.http.impl.client.cache.CacheConfig;
import javax.validation.constraints.Min;
import javax.validation.constraints.NotNull;
import java.io.Serializable;
import java.util.Map;

@Data
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

  private Map<String, String> storage;

}
