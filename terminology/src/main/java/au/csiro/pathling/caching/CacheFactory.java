package au.csiro.pathling.caching;

import org.apache.http.impl.client.cache.CacheConfig;
import org.apache.http.impl.client.cache.CachingHttpClientBuilder;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Map;

public interface CacheFactory {

  @Nonnull
  CachingHttpClientBuilder create(@Nonnull CacheConfig cacheConfig, @Nullable Map<String, String> storageProperties);
}
