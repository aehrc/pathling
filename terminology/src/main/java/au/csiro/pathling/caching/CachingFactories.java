package au.csiro.pathling.caching;

import java.io.File;
import java.util.Map;
import javax.annotation.Nonnull;
import org.apache.http.impl.client.cache.CacheConfig;
import org.apache.http.impl.client.cache.CachingHttpClientBuilder;
import org.apache.http.impl.client.cache.CachingHttpClients;
import org.jetbrains.annotations.Nullable;

public class CachingFactories {

  private static class MemoryCachingFactory implements CacheFactory {

    @Nonnull
    @Override
    public CachingHttpClientBuilder create(@Nonnull final CacheConfig cacheConfig,
        @Nullable final Map<String, String> storageProperties) {
      return CachingHttpClients.custom().setCacheConfig(cacheConfig);
    }
  }

  private static class FileCachingFactory implements CacheFactory {

    @Nonnull
    @Override
    public CachingHttpClientBuilder create(@Nonnull final CacheConfig cacheConfig,
        @Nullable final Map<String, String> storageProperties) {
      // TODO: fix @Nullable
      return CachingHttpClients.custom()
          .setCacheConfig(cacheConfig)
          .setCacheDir(new File(storageProperties.get("cacheDir")));
    }
  }

  @Nonnull
  public static CacheFactory of(@Nonnull final String storageType) {
    if ("memory".equals(storageType)) {
      return new MemoryCachingFactory();
    } else if ("file".equals(storageType)) {
      return new FileCachingFactory();
    } else {
      throw new IllegalArgumentException("Cannot configure cache with storageType: " + storageType);
    }
  }

}
