package au.csiro.pathling.caching;

import static java.util.Objects.nonNull;
import static java.util.Objects.requireNonNull;
import static java.util.Objects.requireNonNullElseGet;

import java.util.Collections;
import java.util.Map;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import au.csiro.pathling.errors.InvalidConfigError;
import org.apache.http.impl.client.cache.CacheConfig;
import org.apache.http.impl.client.cache.CachingHttpClientBuilder;
import org.apache.http.impl.client.cache.CachingHttpClients;


public abstract class AbstractCachingFactoryBase implements CachingFactory {

  protected static class Config {

    @Nonnull
    private final Map<String, String> properties;

    public Config(@Nullable final Map<String, String> properties) {
      this.properties = nonNull(properties)
                        ? requireNonNull(properties)
                        : Collections.emptyMap();
    }

    @Nonnull
    public String required(@Nonnull final String key) {
      return requireNonNullElseGet(properties.get(key), () -> {
        throw new InvalidConfigError("Required config property missing: " + key);
      });
    }

    @Nonnull
    public String optional(@Nonnull final String key, @Nonnull final String defValue) {
      return properties.getOrDefault(key, defValue);
    }
  }


  @Nonnull
  @Override
  public CachingHttpClientBuilder create(@Nonnull final CacheConfig cacheConfig,
      @Nullable final Map<String, String> storageProperties) {
    final CachingHttpClientBuilder builder = CachingHttpClients.custom()
        .setCacheConfig(cacheConfig);
    return configure(builder, new Config(storageProperties));
  }


  @Nonnull
  protected abstract CachingHttpClientBuilder configure(
      @Nonnull final CachingHttpClientBuilder builder,
      @Nonnull final Config config);
}
