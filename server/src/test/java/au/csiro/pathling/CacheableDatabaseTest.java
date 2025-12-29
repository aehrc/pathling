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

  /**
   * Creates a no-op executor for tests that don't need async execution. This avoids passing null to
   * the @Nonnull executor parameter.
   *
   * @return a ThreadPoolTaskExecutor that does nothing when execute() is called
   */
  @SuppressWarnings("NullableProblems")
  private static ThreadPoolTaskExecutor createNoOpExecutor() {
    return new ThreadPoolTaskExecutor() {
      @Override
      public void execute(final Runnable task) {
        // No-op: don't execute anything.
      }
    };
  }

  @Test
  void cacheKeyIsEmptyIfNoFiles() {
    cacheableDatabase =
        new CacheableDatabase(
            sparkSession, "file://" + tempDir.resolve("delta"), createNoOpExecutor());
    assertThat(cacheableDatabase.getCacheKey()).isEmpty();
  }

  @Test
  void cacheKeyIsPresentWhenFilesExist() {
    TestDataSetup.copyTestDataToTempDir(tempDir);
    cacheableDatabase =
        new CacheableDatabase(
            sparkSession, "file://" + tempDir.resolve("delta"), createNoOpExecutor());
    assertThat(cacheableDatabase.getCacheKey()).isPresent();
  }

  @Test
  void cacheKeyIsSameForSameFiles() {
    TestDataSetup.copyTestDataToTempDir(tempDir);
    cacheableDatabase =
        new CacheableDatabase(
            sparkSession, "file://" + tempDir.resolve("delta"), createNoOpExecutor());
    final CacheableDatabase other =
        new CacheableDatabase(
            sparkSession, "file://" + tempDir.resolve("delta"), createNoOpExecutor());
    assertThat(cacheableDatabase.getCacheKey()).isEqualTo(other.getCacheKey());
    assertThat(cacheableDatabase.cacheKeyMatches(other.getCacheKey().orElse(""))).isTrue();
  }

  @Disabled("Is this a bug? https://github.com/delta-io/delta/issues/2570")
  @Test
  void cacheKeyIsDifferentWhenDeltaTableIsDeleted() {
    TestDataSetup.copyTestDataToTempDir(tempDir);
    cacheableDatabase =
        new CacheableDatabase(
            sparkSession, "file://" + tempDir.resolve("delta"), createNoOpExecutor());

    final Path patientParquetPath = tempDir.resolve("delta").resolve("Patient.parquet");
    // BUG? https://github.com/delta-io/delta/issues/2570
    DeltaTable.forPath(sparkSession, patientParquetPath.toString()).delete();

    final CacheableDatabase other =
        new CacheableDatabase(
            sparkSession, "file://" + tempDir.resolve("delta"), createNoOpExecutor());
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

  @Test
  void invalidateWithTablePathUpdatesCacheKey() {
    // This test verifies that invalidate(tablePath) queries only the specified table and updates
    // the cache key based on its Delta timestamp.
    TestDataSetup.copyTestDataToTempDir(tempDir);
    final ThreadPoolTaskExecutor executor = new ThreadPoolTaskExecutor();
    executor.initialize();

    final String databasePath = "file://" + tempDir.resolve("delta");
    cacheableDatabase = new CacheableDatabase(sparkSession, databasePath, executor);
    final String originalCacheKey = cacheableDatabase.getCacheKey().orElse("");
    assertThat(originalCacheKey).isNotEmpty();

    // Modify a single table to create a new Delta version with a newer timestamp.
    final String patientTablePath = databasePath + "/Patient.parquet";
    final DeltaTable patientTable = DeltaTable.forPath(sparkSession, patientTablePath);
    patientTable.toDF().limit(1).write().format("delta").mode("append").save(patientTablePath);

    // Call invalidate with the specific table path.
    cacheableDatabase.invalidate(patientTablePath);

    // Wait for the executor to process the task.
    Awaitility.await()
        .atMost(5, TimeUnit.SECONDS)
        .pollInterval(100, TimeUnit.MILLISECONDS)
        .until(() -> executor.getThreadPoolExecutor().getCompletedTaskCount() >= 1);

    // The cache key should have changed to reflect the new timestamp.
    final String newCacheKey = cacheableDatabase.getCacheKey().orElse("");
    assertThat(newCacheKey).isNotEmpty().isNotEqualTo(originalCacheKey);

    executor.shutdown();
  }

  @Test
  void invalidateWithTablePathCompletesQuickly() {
    // This test verifies that invalidate(tablePath) completes in O(1) time by querying only the
    // specified table, rather than scanning all tables in the database.
    TestDataSetup.copyTestDataToTempDir(tempDir);
    final ThreadPoolTaskExecutor executor = new ThreadPoolTaskExecutor();
    executor.initialize();

    final String databasePath = "file://" + tempDir.resolve("delta");
    cacheableDatabase = new CacheableDatabase(sparkSession, databasePath, executor);

    final String patientTablePath = databasePath + "/Patient.parquet";

    // Call invalidate with the specific table path and measure the time.
    final long startTime = System.currentTimeMillis();
    cacheableDatabase.invalidate(patientTablePath);

    // Wait for the executor to process the task.
    Awaitility.await()
        .atMost(2, TimeUnit.SECONDS)
        .pollInterval(10, TimeUnit.MILLISECONDS)
        .until(() -> executor.getThreadPoolExecutor().getCompletedTaskCount() >= 1);

    final long duration = System.currentTimeMillis() - startTime;

    // The operation should complete quickly since it only queries one table.
    assertThat(duration).isLessThan(1000);
    assertThat(cacheableDatabase.getCacheKey()).isPresent();

    executor.shutdown();
  }
}
