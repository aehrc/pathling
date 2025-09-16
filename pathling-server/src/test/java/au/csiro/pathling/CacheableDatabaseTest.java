package au.csiro.pathling;

import au.csiro.pathling.cache.CacheableDatabase;
import au.csiro.pathling.test.SpringBootUnitTest;
import au.csiro.pathling.util.FhirServerTestConfiguration;
import au.csiro.pathling.util.TestDataSetup;
import io.delta.tables.DeltaTable;
import org.apache.commons.io.FileUtils;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Import;
import java.io.IOException;
import java.nio.file.Path;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * @author Felix Naumann
 */
//@Execution(ExecutionMode.CONCURRENT)
@Import(FhirServerTestConfiguration.class)
@SpringBootUnitTest
public class CacheableDatabaseTest {

  private CacheableDatabase cacheableDatabase;
  
  @Autowired
  private SparkSession sparkSession;

  @TempDir
  private Path tempDir;
  @Autowired
  private TestDataSetup testDataSetup;

  @BeforeEach
  void setUp() {
    
  }
  
  @Test
  void cache_key_is_empty_if_no_files() {
    cacheableDatabase = new CacheableDatabase(sparkSession, "file://" + tempDir.resolve("delta"));
    assertThat(cacheableDatabase.getCacheKey()).isEmpty();
  }
  
  @Test
  void cache_key_is_present_when_files_exist() {
    testDataSetup.copyTestDataToTempDir(tempDir);
    cacheableDatabase = new CacheableDatabase(sparkSession, "file://" + tempDir.resolve("delta"));
    assertThat(cacheableDatabase.getCacheKey()).isPresent();
  }
  
  @Test
  void cache_key_is_same_for_same_files() {
    testDataSetup.copyTestDataToTempDir(tempDir);
    cacheableDatabase = new CacheableDatabase(sparkSession, "file://" + tempDir.resolve("delta"));
    CacheableDatabase other = new CacheableDatabase(sparkSession, "file://" + tempDir.resolve("delta"));
    assertThat(cacheableDatabase.getCacheKey()).isEqualTo(other.getCacheKey());
    assertThat(cacheableDatabase.cacheKeyMatches(other.getCacheKey().orElse(""))).isTrue();
  }
  
  @Test
  void cache_key_is_different_when_delta_table_is_deleted() {
    testDataSetup.copyTestDataToTempDir(tempDir);
    cacheableDatabase = new CacheableDatabase(sparkSession, "file://" + tempDir.resolve("delta"));
    
    Path patientParquetPath = tempDir.resolve("delta").resolve("Patient.parquet");
    DeltaTable.forPath(sparkSession, patientParquetPath.toString()).delete();
    
    CacheableDatabase other = new CacheableDatabase(sparkSession, "file://" + tempDir.resolve("delta"));
    assertThat(cacheableDatabase.getCacheKey()).isNotEqualTo(other.getCacheKey());
    assertThat(cacheableDatabase.cacheKeyMatches(other.getCacheKey().orElse(""))).isFalse();
  }
  
}
