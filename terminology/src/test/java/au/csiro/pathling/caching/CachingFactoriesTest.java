package au.csiro.pathling.caching;

import static org.apache.commons.lang.reflect.FieldUtils.readField;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import au.csiro.pathling.errors.InvalidConfigError;
import com.google.common.collect.ImmutableMap;
import java.io.File;
import org.apache.http.impl.client.cache.CacheConfig;
import org.apache.http.impl.client.cache.CachingHttpClientBuilder;
import org.junit.jupiter.api.Test;

public class CachingFactoriesTest {

  private static final CacheConfig CACHE_CONFIG = CacheConfig.custom().setMaxCacheEntries(100000)
      .build();

  @Test
  public void testMemoryStorageFactory() throws Exception {
    final CachingHttpClientBuilder clientBuilder = CachingFactories.of(
        CachingFactories.MEMORY_STORAGE).create(CACHE_CONFIG, null);
    assertEquals(CACHE_CONFIG, readField(clientBuilder, "cacheConfig", true));
    assertNull(readField(clientBuilder, "cacheDir", true));
  }


  @Test
  public void testDistStorageFactory() throws Exception {
    final CachingHttpClientBuilder clientBuilder = CachingFactories.of(
            CachingFactories.DISK_STORAGE)
        .create(CACHE_CONFIG, ImmutableMap.of(CachingFactories.DISK_CACHE_DIR, "myCacheDir"));
    assertEquals(CACHE_CONFIG, readField(clientBuilder, "cacheConfig", true));
    assertEquals(new File("myCacheDir"), readField(clientBuilder, "cacheDir", true));
  }

  @Test
  public void testDistStoragFactoryRequiresCondig() {
    final InvalidConfigError ex = assertThrows(InvalidConfigError.class, () -> CachingFactories.of(
        CachingFactories.DISK_STORAGE).create(CACHE_CONFIG, null));
    assertEquals("Required config property missing: cacheDir", ex.getMessage());
  }

}
