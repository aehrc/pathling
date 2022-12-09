package au.csiro.pathling.caching;

import static org.apache.commons.lang.reflect.FieldUtils.readField;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;

import au.csiro.pathling.config.HttpClientCachingConfiguration;
import au.csiro.pathling.config.HttpClientCachingConfiguration.StorageType;
import java.io.File;
import java.nio.file.Files;
import org.apache.http.impl.client.cache.CacheConfig;
import org.apache.http.impl.client.cache.CachingHttpClientBuilder;
import org.junit.jupiter.api.Test;

public class CachingFactoriesTest {

  private static final CacheConfig HTTP_CLIENT_CACHE_CONFIG = CacheConfig.custom()
      .setMaxCacheEntries(100000)
      .build();

  @Test
  @SuppressWarnings("resource")
  public void testMemoryStorageFactory() throws Exception {
    final HttpClientCachingConfiguration cacheConfig = HttpClientCachingConfiguration.defaults();
    cacheConfig.setStorageType(StorageType.MEMORY);
    final CachingHttpClientBuilder clientBuilder = CachingFactories.of(cacheConfig)
        .create(HTTP_CLIENT_CACHE_CONFIG);
    assertEquals(HTTP_CLIENT_CACHE_CONFIG, readField(clientBuilder, "cacheConfig", true));
    assertInstanceOf(InfinispanInMemoryStorage.class, readField(clientBuilder, "storage", true));
  }


  @Test
  @SuppressWarnings("resource")
  public void testDiskStorageFactory() throws Exception {
    final HttpClientCachingConfiguration cacheConfig = HttpClientCachingConfiguration.defaults();
    cacheConfig.setStorageType(StorageType.DISK);
    final File tempDirectory = Files.createTempDirectory("pathling-cache").toFile();
    tempDirectory.deleteOnExit();
    cacheConfig.setStoragePath(tempDirectory.getAbsolutePath());
    final CachingHttpClientBuilder clientBuilder = CachingFactories.of(cacheConfig)
        .create(HTTP_CLIENT_CACHE_CONFIG);
    assertEquals(HTTP_CLIENT_CACHE_CONFIG, readField(clientBuilder, "cacheConfig", true));
    assertInstanceOf(InfinispanPersistentStorage.class, readField(clientBuilder, "storage", true));
  }

  @Test
  public void testDiskStorageFactoryRequiresStoragePath() {
    final HttpClientCachingConfiguration cacheConfig = HttpClientCachingConfiguration.defaults();
    cacheConfig.setStorageType(StorageType.DISK);
    assertThrows(NullPointerException.class,
        () -> CachingFactories.of(cacheConfig).create(HTTP_CLIENT_CACHE_CONFIG));
  }

}
