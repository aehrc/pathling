/*
 * Copyright © 2018-2026 Commonwealth Scientific and Industrial Research
 * Organisation (CSIRO) ABN 41 687 119 230.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package au.csiro.pathling;

import static org.assertj.core.api.Assertions.assertThat;

import au.csiro.pathling.cache.CacheableDatabase;
import au.csiro.pathling.test.SpringBootUnitTest;
import au.csiro.pathling.util.FhirServerTestConfiguration;
import au.csiro.pathling.util.TestDataSetup;
import io.delta.tables.DeltaTable;
import java.nio.file.Path;
import java.util.Optional;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Import;

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
    cacheableDatabase = new CacheableDatabase(sparkSession, "file://" + tempDir.resolve("delta"));
    assertThat(cacheableDatabase.getCacheKey()).isEmpty();
  }

  @Test
  void cacheKeyIsPresentWhenFilesExist() {
    TestDataSetup.copyTestDataToTempDir(tempDir);
    cacheableDatabase = new CacheableDatabase(sparkSession, "file://" + tempDir.resolve("delta"));
    assertThat(cacheableDatabase.getCacheKey()).isPresent();
  }

  @Test
  void cacheKeyIsSameForSameFiles() {
    TestDataSetup.copyTestDataToTempDir(tempDir);
    cacheableDatabase = new CacheableDatabase(sparkSession, "file://" + tempDir.resolve("delta"));
    final CacheableDatabase other =
        new CacheableDatabase(sparkSession, "file://" + tempDir.resolve("delta"));
    assertThat(cacheableDatabase.getCacheKey()).isEqualTo(other.getCacheKey());
    assertThat(cacheableDatabase.cacheKeyMatches(other.getCacheKey().orElse(""))).isTrue();
  }

  @Test
  void invalidateUpdatesCacheKeyBeforeReturning() {
    // A full invalidation must refresh the cache key before it returns, so that a request made
    // after the write is not answered with 304 Not Modified for the data from before it.
    TestDataSetup.copyTestDataToTempDir(tempDir);
    final String databasePath = "file://" + tempDir.resolve("delta");
    cacheableDatabase = new CacheableDatabase(sparkSession, databasePath);
    final String originalCacheKey = cacheableDatabase.getCacheKey().orElse("");
    assertThat(originalCacheKey).isNotEmpty();

    appendToPatientTable(databasePath);
    cacheableDatabase.invalidate();

    assertThat(cacheableDatabase.getCacheKey()).isPresent().get().isNotEqualTo(originalCacheKey);
    assertThat(cacheableDatabase.cacheKeyMatches(originalCacheKey)).isFalse();
  }

  @Test
  void invalidateWithTablePathUpdatesCacheKeyBeforeReturning() {
    // Invalidating a single table must refresh the cache key from that table's latest Delta
    // timestamp before it returns, for the same reason as the full invalidation.
    TestDataSetup.copyTestDataToTempDir(tempDir);
    final String databasePath = "file://" + tempDir.resolve("delta");
    cacheableDatabase = new CacheableDatabase(sparkSession, databasePath);
    final String originalCacheKey = cacheableDatabase.getCacheKey().orElse("");
    assertThat(originalCacheKey).isNotEmpty();

    final String patientTablePath = appendToPatientTable(databasePath);
    cacheableDatabase.invalidate(patientTablePath);

    assertThat(cacheableDatabase.getCacheKey()).isPresent().get().isNotEqualTo(originalCacheKey);
    assertThat(cacheableDatabase.cacheKeyMatches(originalCacheKey)).isFalse();
  }

  @Test
  void invalidateWithTablePathCompletesQuickly() {
    // Invalidating a single table queries only that table, rather than scanning every table in the
    // database, so it adds little to the time taken by the write that precedes it.
    TestDataSetup.copyTestDataToTempDir(tempDir);
    final String databasePath = "file://" + tempDir.resolve("delta");
    cacheableDatabase = new CacheableDatabase(sparkSession, databasePath);

    final long startTime = System.currentTimeMillis();
    cacheableDatabase.invalidate(databasePath + "/Patient.parquet");
    final long duration = System.currentTimeMillis() - startTime;

    assertThat(duration).isLessThan(1000);
    assertThat(cacheableDatabase.getCacheKey()).isPresent();
  }

  @Test
  void invalidateWithInaccessibleTableKeepsCacheKey() {
    // The write has already succeeded by the time the cache is invalidated, so a table that cannot
    // be read must not cause the invalidation to throw. The previous cache key is kept.
    TestDataSetup.copyTestDataToTempDir(tempDir);
    final String databasePath = "file://" + tempDir.resolve("delta");
    cacheableDatabase = new CacheableDatabase(sparkSession, databasePath);
    final Optional<String> originalCacheKey = cacheableDatabase.getCacheKey();
    assertThat(originalCacheKey).isPresent();

    cacheableDatabase.invalidate(databasePath + "/Missing.parquet");

    assertThat(cacheableDatabase.getCacheKey()).isEqualTo(originalCacheKey);
  }

  /**
   * Appends a row to the Patient table, creating a new Delta version with a newer timestamp.
   *
   * @param databasePath the path to the database
   * @return the path to the Patient table
   */
  private String appendToPatientTable(final String databasePath) {
    final String patientTablePath = databasePath + "/Patient.parquet";
    final DeltaTable patientTable = DeltaTable.forPath(sparkSession, patientTablePath);
    patientTable.toDF().limit(1).write().format("delta").mode("append").save(patientTablePath);
    return patientTablePath;
  }
}
