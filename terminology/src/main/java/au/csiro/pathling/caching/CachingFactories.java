package au.csiro.pathling.caching;

import java.io.File;
import javax.annotation.Nonnull;
import au.csiro.pathling.errors.InvalidConfigError;
import org.apache.http.impl.client.cache.CachingHttpClientBuilder;

public class CachingFactories {

  public static final String MEMORY_STORAGE = "memory";
  public static final String DISK_STORAGE = "disk";

  public static final String DISK_CACHE_DIR = "cacheDir";

  private static class MemoryCachingFactory extends AbstractCachingFactoryBase {

    @Nonnull
    @Override
    protected CachingHttpClientBuilder configure(@Nonnull final CachingHttpClientBuilder builder,
        @Nonnull final Config config) {
      return builder;
    }
  }

  private static class DiskCachingFactory extends AbstractCachingFactoryBase {

    @Nonnull
    @Override
    protected CachingHttpClientBuilder configure(@Nonnull final CachingHttpClientBuilder builder,
        @Nonnull final Config config) {
      return builder.setCacheDir(new File(config.required(DISK_CACHE_DIR)));
    }
  }

  @Nonnull
  public static CachingFactory of(@Nonnull final String storageType) {
    if (MEMORY_STORAGE.equals(storageType)) {
      return new MemoryCachingFactory();
    } else if (DISK_STORAGE.equals(storageType)) {
      return new DiskCachingFactory();
    } else {
      throw new InvalidConfigError("Cannot configure cache with storageType: " + storageType);
    }
  }

}
