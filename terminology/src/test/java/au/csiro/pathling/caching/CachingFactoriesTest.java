package au.csiro.pathling.caching;

import static org.apache.commons.lang.reflect.FieldUtils.readField;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import au.csiro.pathling.config.HttpCacheConfiguration;
import au.csiro.pathling.config.HttpCacheConfiguration.StorageType;
import java.io.File;
import org.apache.http.impl.client.cache.CacheConfig;
import org.apache.http.impl.client.cache.CachingHttpClientBuilder;
import org.junit.jupiter.api.Test;

public class CachingFactoriesTest {

  private static final CacheConfig HTTP_CLIENT_CACHE_CONFIG = CacheConfig.custom()
      .setMaxCacheEntries(100000)
      .build();

  @Test
  public void testMemoryStorageFactory() throws Exception {
    final HttpCacheConfiguration cacheConfig = HttpCacheConfiguration.defaults();
    cacheConfig.setStorageType(StorageType.MEMORY);
    final CachingHttpClientBuilder clientBuilder = CachingFactories.of(cacheConfig)
        .create(HTTP_CLIENT_CACHE_CONFIG);
    assertEquals(HTTP_CLIENT_CACHE_CONFIG, readField(clientBuilder, "cacheConfig", true));
    assertNull(readField(clientBuilder, "cacheDir", true));
  }


  @Test
  public void testDiskStorageFactory() throws Exception {
    final HttpCacheConfiguration cacheConfig = HttpCacheConfiguration.defaults();
    cacheConfig.setStorageType(StorageType.DISK);
    cacheConfig.setStoragePath("myCacheDir");
    final CachingHttpClientBuilder clientBuilder = CachingFactories.of(cacheConfig)
        .create(HTTP_CLIENT_CACHE_CONFIG);
    assertEquals(HTTP_CLIENT_CACHE_CONFIG, readField(clientBuilder, "cacheConfig", true));
    assertEquals(new File("myCacheDir"), readField(clientBuilder, "cacheDir", true));
  }

  @Test
  public void testDiskStorageFactoryRequiresStoragePath() {
    final HttpCacheConfiguration cacheConfig = HttpCacheConfiguration.defaults();
    cacheConfig.setStorageType(StorageType.DISK);
    assertThrows(NullPointerException.class,
        () -> CachingFactories.of(cacheConfig).create(HTTP_CLIENT_CACHE_CONFIG));
  }

}
