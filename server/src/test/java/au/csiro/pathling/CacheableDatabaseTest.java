package au.csiro.pathling;

import static org.assertj.core.api.Assertions.assertThat;

import au.csiro.pathling.cache.CacheableDatabase;
import au.csiro.pathling.test.SpringBootUnitTest;
import au.csiro.pathling.util.FhirServerTestConfiguration;
import au.csiro.pathling.util.TestDataSetup;
import io.delta.tables.DeltaTable;
import java.nio.file.Path;
import java.util.concurrent.TimeUnit;
import org.apache.spark.sql.SparkSession;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Import;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;

/**
 * Tests for {@link CacheableDatabase}, which manages cache keys derived from Delta Lake table
 * timestamps. These cache keys are used for HTTP ETag generation to support client-side caching of
 * search results.
 *
 * @author Felix Naumann
 * @author John Grimes
 */
@Import(FhirServerTestConfiguration.class)
@SpringBootUnitTest
class CacheableDatabaseTest {

  private CacheableDatabase cacheableDatabase;

  @Autowired private SparkSession sparkSession;

  @TempDir private Path tempDir;

  @Test
  void cacheKeyIsEmptyIfNoFiles() {
    cacheableDatabase =
        new CacheableDatabase(sparkSession, "file://" + tempDir.resolve("delta"), null);
    assertThat(cacheableDatabase.getCacheKey()).isEmpty();
  }

  @Test
  void cacheKeyIsPresentWhenFilesExist() {
    TestDataSetup.copyTestDataToTempDir(tempDir);
    cacheableDatabase =
        new CacheableDatabase(sparkSession, "file://" + tempDir.resolve("delta"), null);
    assertThat(cacheableDatabase.getCacheKey()).isPresent();
  }

  @Test
  void cacheKeyIsSameForSameFiles() {
    TestDataSetup.copyTestDataToTempDir(tempDir);
    cacheableDatabase =
        new CacheableDatabase(sparkSession, "file://" + tempDir.resolve("delta"), null);
    CacheableDatabase other =
        new CacheableDatabase(sparkSession, "file://" + tempDir.resolve("delta"), null);
    assertThat(cacheableDatabase.getCacheKey()).isEqualTo(other.getCacheKey());
    assertThat(cacheableDatabase.cacheKeyMatches(other.getCacheKey().orElse(""))).isTrue();
  }

  @Disabled("Is this a bug? https://github.com/delta-io/delta/issues/2570")
  @Test
  void cacheKeyIsDifferentWhenDeltaTableIsDeleted() {
    TestDataSetup.copyTestDataToTempDir(tempDir);
    cacheableDatabase =
        new CacheableDatabase(sparkSession, "file://" + tempDir.resolve("delta"), null);

    Path patientParquetPath = tempDir.resolve("delta").resolve("Patient.parquet");
    // BUG? https://github.com/delta-io/delta/issues/2570
    DeltaTable.forPath(sparkSession, patientParquetPath.toString()).delete();

    CacheableDatabase other =
        new CacheableDatabase(sparkSession, "file://" + tempDir.resolve("delta"), null);
    assertThat(cacheableDatabase.getCacheKey()).isNotEqualTo(other.getCacheKey());
    assertThat(cacheableDatabase.cacheKeyMatches(other.getCacheKey().orElse(""))).isFalse();
  }

  @Test
  void invalidateRefreshesCacheKeyAsynchronously() {
    // This test verifies that invalidate() triggers an asynchronous cache key refresh.
    // The actual cache key change is tested via integration tests.
    TestDataSetup.copyTestDataToTempDir(tempDir);
    final ThreadPoolTaskExecutor executor = new ThreadPoolTaskExecutor();
    executor.initialize();

    cacheableDatabase =
        new CacheableDatabase(sparkSession, "file://" + tempDir.resolve("delta"), executor);
    final String originalCacheKey = cacheableDatabase.getCacheKey().orElse("");
    assertThat(originalCacheKey).isNotEmpty();

    // Call invalidate - this triggers an async refresh.
    cacheableDatabase.invalidate();

    // Wait for the executor to process the task.
    Awaitility.await()
        .atMost(5, TimeUnit.SECONDS)
        .pollInterval(100, TimeUnit.MILLISECONDS)
        .until(() -> executor.getThreadPoolExecutor().getCompletedTaskCount() >= 1);

    // The cache key should still be present (the refresh should have completed).
    assertThat(cacheableDatabase.getCacheKey()).isPresent();

    executor.shutdown();
  }
}
