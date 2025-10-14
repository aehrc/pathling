/*
 * Copyright © 2018-2025 Commonwealth Scientific and Industrial Research
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

package au.csiro.pathling.library.io;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.withSettings;

import au.csiro.pathling.encoders.FhirEncoders;
import au.csiro.pathling.library.PathlingContext;
import au.csiro.pathling.library.TestHelpers;
import au.csiro.pathling.library.io.sink.DataSinkBuilder;
import au.csiro.pathling.library.io.source.DataSourceBuilder;
import au.csiro.pathling.library.io.source.QueryableDataSource;
import au.csiro.pathling.terminology.TerminologyServiceFactory;
import au.csiro.pathling.test.assertions.DatasetAssert;
import jakarta.annotation.Nonnull;
import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import java.util.Set;
import java.util.function.Function;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.io.FileUtils;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * Tests for reading from and writing to various data sources and sinks.
 * <p>
 * This test suite validates the functionality of the Pathling library's data I/O operations,
 * including reading from and writing to NDJSON, Parquet, Delta, bundles, datasets, and catalog
 * tables. Tests cover various save modes (overwrite, append, ignore, merge, error) and format
 * options to ensure data integrity and proper error handling across all supported data sources.
 *
 * @author John Grimes
 * @author Piotr Szul
 */
@Slf4j
class DataSourcesTest {

  static final Path TEST_DATA_PATH = Path.of(
      "src/test/resources/test-data").toAbsolutePath().normalize();

  static PathlingContext pathlingContext;
  static SparkSession spark;
  static Path temporaryDirectory;

  /**
   * Set up Spark.
   */
  @BeforeAll
  static void setupContext() throws IOException {
    // Create a temporary directory that we can use to write data to.
    temporaryDirectory = Files.createTempDirectory("pathling-datasources-test-");
    log.info("Created temporary directory: {}", temporaryDirectory);

    // Create a Spark session, with Hive support and a warehouse in the temp directory.
    final Path warehouseLocation = temporaryDirectory.resolve("spark-warehouse");
    final Path metastoreLocation = temporaryDirectory.resolve("metastore_db");
    spark = TestHelpers.sparkBuilder()
        .config("spark.sql.catalogImplementation", "hive")
        .config("spark.sql.warehouse.dir", warehouseLocation.toString())
        .config("javax.jdo.option.ConnectionURL",
            "jdbc:derby:" + metastoreLocation + ";create=true")
        .getOrCreate();

    // Create the test schema.
    spark.sql("CREATE DATABASE IF NOT EXISTS test");

    // Create a mock terminology service factory.
    final TerminologyServiceFactory terminologyServiceFactory = mock(
        TerminologyServiceFactory.class, withSettings().serializable());

    // Create the Pathling context.
    pathlingContext = PathlingContext.create(spark, FhirEncoders.forR4().getOrCreate(),
        terminologyServiceFactory);
  }

  /**
   * Clean up catalog after each test.
   */
  @AfterEach
  void cleanupCatalog() {
    // Drop all tables in the default schema.
    spark.sql("SHOW TABLES").collectAsList().forEach(row -> {
      final String tableName = row.getAs("tableName");
      spark.sql("DROP TABLE IF EXISTS " + tableName);
    });

    // Drop all tables in the test schema.
    spark.sql("SHOW TABLES IN test").collectAsList().forEach(row -> {
      final String tableName = row.getAs("tableName");
      spark.sql("DROP TABLE IF EXISTS test." + tableName);
    });
  }

  /**
   * Tear down Spark.
   */
  @AfterAll
  static void tearDownAll() throws IOException {
    spark.stop();
    FileUtils.deleteDirectory(temporaryDirectory.toFile());
  }

  // NDJSON Tests
  @Test
  void ndjsonReadWrite() {
    // Read the test NDJSON data.
    final QueryableDataSource data = pathlingContext.read()
        .ndjson(TEST_DATA_PATH.resolve("ndjson").toString());

    // Query the data.
    queryNdjsonData(data);

    // Write the data back out to a temporary location.
    data.write().saveMode("error").ndjson(temporaryDirectory.resolve("ndjson").toString());

    // Read the data back in.
    final QueryableDataSource newData = pathlingContext.read()
        .ndjson(temporaryDirectory.resolve("ndjson").toString());

    // Query the data.
    queryNdjsonData(newData);
  }

  @ParameterizedTest
  @ValueSource(strings = {"overwrite", "append", "ignore"})
  void ndjsonWriteWithSaveModes(final String saveMode) {
    // Read the test NDJSON data.
    final QueryableDataSource data = pathlingContext.read()
        .ndjson(TEST_DATA_PATH.resolve("ndjson").toString());

    // Write the data back out to a temporary location with the specified save mode.
    data.write().saveMode(saveMode)
        .ndjson(temporaryDirectory.resolve("ndjson-" + saveMode).toString());

    // Read the data back in.
    final QueryableDataSource newData = pathlingContext.read()
        .ndjson(temporaryDirectory.resolve("ndjson-" + saveMode).toString());

    // Query the data.
    queryNdjsonData(newData);
  }

  @Test
  void ndjsonWithExtension() {
    // Read the test NDJSON data.
    final QueryableDataSource data = pathlingContext.read()
        .ndjson(TEST_DATA_PATH.resolve("jsonl").toString(), "jsonl");

    // Query the data.
    queryNdjsonData(data);
  }

  @Test
  void ndjsonReadQualified() {
    // Read the test NDJSON data.
    final QueryableDataSource data = pathlingContext.read()
        .ndjson(TEST_DATA_PATH.resolve("ndjson-qualified").toString());

    // Query the data.
    queryNdjsonData(data);
  }

  @Test
  void ndjsonReadWriteCustom() {
    final Function<String, Set<String>> readMapper = baseName -> Collections.singleton(
        baseName.replaceFirst("Custom", ""));

    // Read the test NDJSON data.
    final QueryableDataSource data = pathlingContext.read()
        .ndjson(TEST_DATA_PATH.resolve("ndjson-custom").toString(), "ndjson",
            readMapper);

    // Query the data.
    queryNdjsonData(data);

    // Write the data back out to a temporary location.
    data.write().saveMode("error").ndjson(temporaryDirectory.resolve("ndjson-custom").toString(),
        baseName -> baseName.replaceFirst("Custom", ""));

    // Read the data back in.
    final QueryableDataSource newData = pathlingContext.read()
        .ndjson(temporaryDirectory.resolve("ndjson-custom").toString(), "ndjson",
            readMapper);

    // Query the data.
    queryNdjsonData(newData);
  }

  @Test
  void ndjsonWithExtract() {
    // Read the test NDJSON data.
    final QueryableDataSource data = pathlingContext.read()
        .ndjson(TEST_DATA_PATH.resolve("ndjson").toString());

    // Query the data.
    extractNdjsonData(data);
  }

  // Bundles Tests
  @Test
  void bundlesRead() {
    // Read the test bundles.
    final QueryableDataSource data = pathlingContext.read()
        .bundles(TEST_DATA_PATH.resolve("bundles").toString(),
            Set.of("Patient", "Condition"), PathlingContext.FHIR_JSON);

    // Query the data.
    queryBundlesData(data);
  }

  // Datasets Tests
  @Test
  void datasetsRead() {
    // Create the test datasets from Delta source data, using the Spark API.
    final Dataset<Row> condition = spark.read().format("delta")
        .load(TEST_DATA_PATH.resolve("delta").resolve("Condition.parquet").toString());
    final Dataset<Row> patient = spark.read().format("delta")
        .load(TEST_DATA_PATH.resolve("delta").resolve("Patient.parquet").toString());

    // Create a dataset source from the datasets.
    final QueryableDataSource data = pathlingContext.read().datasets()
        .dataset("Condition", condition)
        .dataset("Patient", patient);

    // Query the data.
    queryDeltaData(data);
  }

  // Parquet Tests
  @Test
  void parquetReadWrite() {
    // Read the test Parquet data.
    final QueryableDataSource data = pathlingContext.read()
        .parquet(TEST_DATA_PATH.resolve("parquet").toString());

    // Query the data.
    queryParquetData(data);

    // Write the data back out to a temporary location.
    data.write().saveMode("error").parquet(temporaryDirectory.resolve("parquet").toString());

    // Read the data back in.
    final QueryableDataSource newData = pathlingContext.read()
        .parquet(temporaryDirectory.resolve("parquet").toString());

    // Query the data.
    queryParquetData(newData);
  }

  @ParameterizedTest
  @ValueSource(strings = {"overwrite", "append", "ignore"})
  void parquetWriteWithSaveModes(final String saveMode) {
    // Read the test NDJSON data.
    final QueryableDataSource data = pathlingContext.read()
        .ndjson(TEST_DATA_PATH.resolve("ndjson").toString());

    // Write the data to Parquet with the specified save mode.
    data.write().saveMode(saveMode)
        .parquet(temporaryDirectory.resolve("parquet-" + saveMode).toString());

    // Read the Parquet data back in.
    final QueryableDataSource newData = pathlingContext.read()
        .parquet(temporaryDirectory.resolve("parquet-" + saveMode).toString());

    // Query the data.
    queryNdjsonData(newData);
  }

  @Test
  void parquetReadWriteCustom() {
    final Function<String, Set<String>> readMapper = baseName -> Collections.singleton(
        baseName.replaceFirst("Custom", ""));

    // Read the test Parquet data with custom filename mapping.
    final QueryableDataSource data = pathlingContext.read()
        .parquet(TEST_DATA_PATH.resolve("parquet-custom").toString(), readMapper);

    // Query the data.
    queryParquetData(data);

    // Write the data back out to a temporary location.
    data.write().saveMode("error").parquet(temporaryDirectory.resolve("parquet-custom").toString(),
        baseName -> baseName.replaceFirst("Custom", ""));

    // Read the data back in.
    final QueryableDataSource newData = pathlingContext.read()
        .parquet(temporaryDirectory.resolve("parquet-custom").toString(), readMapper);

    // Query the data.
    queryParquetData(newData);
  }

  // Delta Tests
  @Test
  void deltaReadWrite() {
    // Read the test Delta data.
    final QueryableDataSource data = pathlingContext.read()
        .delta(TEST_DATA_PATH.resolve("delta").toString());

    // Query the data.
    queryDeltaData(data);

    // Write the data back out to a temporary location.
    data.write().delta(temporaryDirectory.resolve("delta").toString(), false);

    // Read the data back in.
    final QueryableDataSource newData = pathlingContext.read()
        .delta(temporaryDirectory.resolve("delta").toString());

    // Query the data.
    queryDeltaData(newData);
  }

  @ParameterizedTest
  @ValueSource(strings = {"overwrite", "append", "ignore"})
  void deltaWriteWithSaveModes(final String saveMode) {
    // Read the test NDJSON data.
    final QueryableDataSource data = pathlingContext.read()
        .ndjson(TEST_DATA_PATH.resolve("ndjson").toString());

    // Write the data to Delta with the specified save mode.
    data.write().saveMode(saveMode)
        .delta(temporaryDirectory.resolve("delta-" + saveMode).toString(), false);

    // Read the Delta data back in.
    final QueryableDataSource newData = pathlingContext.read()
        .delta(temporaryDirectory.resolve("delta-" + saveMode).toString());

    // Query the data.
    queryNdjsonData(newData);
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void deltaReadWriteWithMerge(boolean deleteOnMerge) {
    final String sourcePath = TEST_DATA_PATH.resolve("delta").toString();
    final String destinationPath = temporaryDirectory.resolve("delta-rw-merge").toString();

    // Read the test Delta data.
    final QueryableDataSource data = pathlingContext.read().delta(sourcePath);

    // Query the data.
    queryDeltaData(data);

    // Write the data back out to a temporary location.
    data.write().saveMode("merge").delta(destinationPath, deleteOnMerge);

    // Read the data back in.
    final QueryableDataSource newData = pathlingContext.read().delta(destinationPath);

    // Query the data.
    queryDeltaData(newData);

    // Merge the data back into the Delta table.
    data.write().saveMode("merge").delta(destinationPath, deleteOnMerge);

    // Query the data.
    queryDeltaData(newData);
  }

  @Test
  void deltaWriteOverwriteExisting() {
    // Read the test NDJSON data.
    final QueryableDataSource data = pathlingContext.read()
        .ndjson(TEST_DATA_PATH.resolve("ndjson").toString());

    // Write the data to Delta initially.
    final String deltaPath = temporaryDirectory.resolve("delta-overwrite-test").toString();
    data.write().saveMode("overwrite").delta(deltaPath, false);

    // Read the data back to verify it was written.
    final QueryableDataSource initialData = pathlingContext.read().delta(deltaPath);
    queryNdjsonData(initialData);

    // Write the same data again using overwrite mode - this should succeed.
    // This tests the deltaTableExists method and the delete() workaround in DeltaSink.
    data.write().saveMode("overwrite").delta(deltaPath, false);

    // Read the overwritten data back.
    final QueryableDataSource overwrittenData = pathlingContext.read().delta(deltaPath);

    // Query the data to ensure it's still correct after overwrite.
    queryNdjsonData(overwrittenData);
  }

  // Tables (CatalogSink) Tests
  @Test
  void tablesReadWrite() {
    // Read the test NDJSON data.
    final QueryableDataSource data = pathlingContext.read()
        .ndjson(TEST_DATA_PATH.resolve("ndjson").toString());

    // Query the data.
    queryNdjsonData(data);

    // Write the data back out to tables.
    data.write().tables();

    // Read the data back in.
    final QueryableDataSource newData = pathlingContext.read().tables();

    // Query the data.
    queryNdjsonData(newData);
  }

  @Test
  void tablesWriteWithOverwrite() {
    // Read the test NDJSON data.
    final QueryableDataSource data = pathlingContext.read()
        .ndjson(TEST_DATA_PATH.resolve("ndjson").toString());

    // Write the data back out to tables.
    data.write().saveMode("overwrite").tables("test");

    // Read the data back in.
    final QueryableDataSource newData = pathlingContext.read().tables("test");

    // Query the data.
    queryNdjsonData(newData);
  }

  @ParameterizedTest
  @ValueSource(strings = {"append", "ignore"})
  void tablesWriteWithSaveModes(final String saveMode) {
    // Read the test NDJSON data.
    final QueryableDataSource data = pathlingContext.read()
        .ndjson(TEST_DATA_PATH.resolve("ndjson").toString());

    // Write the data to catalog tables with the specified save mode.
    data.write().saveMode(saveMode).tables();

    // Read the data back from catalog tables.
    final QueryableDataSource newData = pathlingContext.read().tables();

    // Query the data.
    queryNdjsonData(newData);
  }

  @Test
  void tablesWriteWithMerge() {
    // Read the test NDJSON data.
    final QueryableDataSource data = pathlingContext.read()
        .ndjson(TEST_DATA_PATH.resolve("ndjson").toString());

    // Write the data back out to tables using merge (creates new tables since none exist).
    data.write().saveMode("merge").tables("test", "delta");

    // Read the data back in.
    final QueryableDataSource newData = pathlingContext.read().tables("test");

    // Query the data.
    queryNdjsonData(newData);

    // Now test merging again into the existing Delta table.
    data.write().saveMode("merge").tables("test", "delta");

    // Read the merged data back in.
    final QueryableDataSource mergedData = pathlingContext.read().tables("test");

    // Query the merged data.
    queryNdjsonData(mergedData);
  }

  @Test
  void tablesWriteWithParquetFormat() {
    // Read the test NDJSON data.
    final QueryableDataSource data = pathlingContext.read()
        .ndjson(TEST_DATA_PATH.resolve("ndjson").toString());

    // Write the data to catalog tables using Parquet format.
    data.write().saveMode("overwrite").tables("test", "parquet");

    // Read the data back from the test schema.
    final QueryableDataSource newData = pathlingContext.read().tables("test");

    // Query the data.
    queryNdjsonData(newData);
  }

  @Test
  void tablesWriteWithDeltaFormat() {
    // Read the test NDJSON data.
    final QueryableDataSource data = pathlingContext.read()
        .ndjson(TEST_DATA_PATH.resolve("ndjson").toString());

    // Write the data back out to tables using delta format and overwrite mode.
    data.write().saveMode("overwrite").tables("test", "delta");

    // Read the data back in.
    final QueryableDataSource newData = pathlingContext.read().tables("test");

    // Query the data.
    queryNdjsonData(newData);
  }

  // Error Condition Tests
  @Test
  void readNonExistentResource() {
    // Create a dataset source with no datasets.
    final QueryableDataSource data = pathlingContext.read().datasets();

    // Attempting to read a non-existent resource should throw an exception.
    assertThrows(IllegalArgumentException.class, () -> data.read("Patient"));
  }

  @Test
  void readInvalidUri() {
    // Create a data source builder.
    final DataSourceBuilder builder = pathlingContext.read();

    // Attempting to read from an invalid URI should throw a RuntimeException.
    final RuntimeException exception = assertThrows(RuntimeException.class,
        () -> builder.ndjson("file:\\\\non-existent"));

    // The cause should be a URISyntaxException.
    assertInstanceOf(URISyntaxException.class, exception.getCause());
  }

  @Test
  void ndjsonWriteWithMergeShouldFail() {
    // Read the test NDJSON data.
    final QueryableDataSource data = pathlingContext.read()
        .ndjson(TEST_DATA_PATH.resolve("ndjson").toString());
    final DataSinkBuilder dataSinkBuilder = data.write();
    final String destinationPath = temporaryDirectory.resolve("ndjson-merge-fail").toString();
    final DataSinkBuilder builder = dataSinkBuilder.saveMode("merge");

    // Attempting to write NDJSON data using merge mode should throw UnsupportedOperationException.
    final UnsupportedOperationException exception = assertThrows(
        UnsupportedOperationException.class, () -> builder.ndjson(destinationPath));

    // Verify the error message.
    assertEquals("Merge operation is not supported for NDJSON", exception.getMessage());
  }

  @Test
  void parquetWriteWithMergeShouldFail() {
    // Read the test NDJSON data.
    final QueryableDataSource data = pathlingContext.read()
        .ndjson(TEST_DATA_PATH.resolve("ndjson").toString());
    final DataSinkBuilder dataSinkBuilder = data.write();
    final String destinationPath = temporaryDirectory.resolve("parquet-merge-fail").toString();
    final DataSinkBuilder builder = dataSinkBuilder.saveMode("merge");

    // Attempting to write Parquet data using merge mode should throw UnsupportedOperationException.
    final UnsupportedOperationException exception = assertThrows(
        UnsupportedOperationException.class, () -> builder.parquet(destinationPath));

    // Verify the error message.
    assertEquals("Merge operation is not supported for Parquet - use Delta if merging is required",
        exception.getMessage());
  }

  // Data Source Caching and Transformation Tests
  @Test
  void ndjsonSourceCache() {
    // Create an NDJSON source directly to test the cache method.
    final QueryableDataSource data = pathlingContext.read()
        .ndjson(TEST_DATA_PATH.resolve("ndjson").toString());

    // Apply cache to the source - this tests that cache() works without errors.
    data.cache();

    // Verify the original source still works correctly after caching operation.
    queryNdjsonData(data);
  }

  @Test
  void ndjsonSourceMap() {
    // Create an NDJSON source to test the map method.
    final QueryableDataSource data = pathlingContext.read()
        .ndjson(TEST_DATA_PATH.resolve("ndjson").toString());

    // Apply a transformation that adds a column to all datasets.
    data.map(dataset -> dataset.withColumn("test_column", functions.lit("test_value")));

    // Verify the transformation functionality works (testing that map() accepts the operator).
    // The actual testing of transformed data would require more complex setup.
    assertEquals(2, data.getResourceTypes().size());
    assertTrue(data.getResourceTypes().contains("Patient"));
    assertTrue(data.getResourceTypes().contains("Condition"));
  }

  @Test
  void parquetSourceCache() {
    // Create a Parquet source to test the cache method.
    final QueryableDataSource data = pathlingContext.read()
        .parquet(TEST_DATA_PATH.resolve("parquet").toString());

    // Apply cache to the source - this tests that cache() works without errors.
    data.cache();

    // Verify the original source still works correctly after caching operation.
    queryParquetData(data);
  }

  @Test
  void parquetSourceMap() {
    // Create a Parquet source to test the map method.
    final QueryableDataSource data = pathlingContext.read()
        .parquet(TEST_DATA_PATH.resolve("parquet").toString());

    // Apply a transformation that adds a column to all datasets.
    data.map(dataset -> dataset.withColumn("test_column", functions.lit("test_value")));

    // Verify the transformation functionality works (testing that map() accepts the operator).
    // The actual testing of transformed data would require more complex setup.
    assertEquals(2, data.getResourceTypes().size());
    assertTrue(data.getResourceTypes().contains("Patient"));
    assertTrue(data.getResourceTypes().contains("Condition"));
  }


  private static final String PATIENT_VIEW_JSON = """
        {
        "resource": "Patient",
        "select": [
          {
            "column": [
              {
                "path": "id",
                "name": "id"
              }
            ]
          }
        ],
        "where": [
          {
            "path": "gender = 'male'"
          }
        ]
      }
      """;


  private static final String CONDITION_VIEW_JSON = """
        {
        "resource": "Condition",
        "select": [
          {
            "column": [
              {
                "path": "id",
                "name": "id"
              },
              {
                "path": "onset.ofType(Period).start",
                "name": "onset_start"
              }
            ]
          }
        ]
      }
      """;

  private static void queryNdjsonData(@Nonnull final QueryableDataSource data) {
    assertEquals(2, data.getResourceTypes().size());
    assertTrue(data.getResourceTypes().contains("Patient"));
    assertTrue(data.getResourceTypes().contains("Condition"));

    final Dataset<Row> patientCount = data.view("Patient")
        .json(PATIENT_VIEW_JSON)
        .execute()
        .agg(functions.count("id"));

    DatasetAssert.of(patientCount).hasRows(RowFactory.create(5));

    final Dataset<Row> conditionCount = data.view("Condition")
        .json(CONDITION_VIEW_JSON)
        .execute()
        .agg(functions.count("id"));
    DatasetAssert.of(conditionCount).hasRows(RowFactory.create(71));
  }

  private static void queryBundlesData(@Nonnull final QueryableDataSource data) {
    assertEquals(2, data.getResourceTypes().size());
    assertTrue(data.getResourceTypes().contains("Patient"));
    assertTrue(data.getResourceTypes().contains("Condition"));

    final Dataset<Row> patientCount = data.view("Patient")
        .json(PATIENT_VIEW_JSON)
        .execute()
        .agg(functions.count("id"));

    DatasetAssert.of(patientCount).hasRows(RowFactory.create(4));

    final Dataset<Row> conditionCount = data.view("Condition")
        .json(CONDITION_VIEW_JSON)
        .execute()
        .agg(functions.count("id"));

    DatasetAssert.of(conditionCount).hasRows(RowFactory.create(246));
  }

  private static void queryDeltaData(@Nonnull final QueryableDataSource dataSource) {
    queryNdjsonData(dataSource);
  }

  private static void queryParquetData(@Nonnull final QueryableDataSource dataSource) {
    queryNdjsonData(dataSource);
  }

  private static final String EXTRACT_VIEW_JSON = """
      {
        "resource": "Patient",
        "select": [
          {
            "column": [
              {
                "path": "id",
                "name": "Patient_id"
              },
              {
                "path": "gender",
                "name": "gender"
              },
              {
                "path": "address.postalCode.first()",
                "name": "address_postalCode"
              }
            ]
          }
        ],
        "where": [
          {
            "path": "id = 'beff242e-580b-47c0-9844-c1a68c36c5bf'"
          }
        ]
      }
      """;

  private static void extractNdjsonData(@Nonnull final QueryableDataSource dataSource) {
    final Dataset<Row> patient = dataSource.view("Patient")
        .json(EXTRACT_VIEW_JSON)
        .execute()
        .limit(1);
    DatasetAssert.of(patient)
        .hasRows(RowFactory.create("beff242e-580b-47c0-9844-c1a68c36c5bf", "male", "02138"));
  }

  // Null Parameter Validation Tests for DataSourceBuilder
  @Test
  void ndjsonWithNullPathShouldThrowIllegalArgumentException() {
    final DataSourceBuilder builder = pathlingContext.read();
    final IllegalArgumentException exception = assertThrows(IllegalArgumentException.class,
        () -> builder.ndjson(null));
    assertEquals("Argument must not be null", exception.getMessage());
  }

  @Test
  void ndjsonWithNullExtensionShouldThrowIllegalArgumentException() {
    final DataSourceBuilder builder = pathlingContext.read();
    final IllegalArgumentException exception = assertThrows(IllegalArgumentException.class,
        () -> builder.ndjson("path", null));
    assertEquals("Argument must not be null", exception.getMessage());
  }

  @Test
  void ndjsonWithNullPathAndExtensionShouldThrowIllegalArgumentException() {
    final DataSourceBuilder builder = pathlingContext.read();
    final IllegalArgumentException exception = assertThrows(IllegalArgumentException.class,
        () -> builder.ndjson(null, "extension"));
    assertEquals("Argument must not be null", exception.getMessage());
  }

  @Test
  void ndjsonWithNullFileNameMapperShouldThrowIllegalArgumentException() {
    final DataSourceBuilder builder = pathlingContext.read();
    final IllegalArgumentException exception = assertThrows(IllegalArgumentException.class,
        () -> builder.ndjson("path", "extension", null));
    assertEquals("Argument must not be null", exception.getMessage());
  }

  @Test
  void bundlesWithNullPathShouldThrowIllegalArgumentException() {
    final DataSourceBuilder builder = pathlingContext.read();
    final Set<String> resources = Set.of("Patient");
    final IllegalArgumentException exception = assertThrows(IllegalArgumentException.class,
        () -> builder.bundles(null, resources, "application/fhir+json"));
    assertEquals("Argument must not be null", exception.getMessage());
  }

  @Test
  void bundlesWithNullResourceTypesShouldThrowIllegalArgumentException() {
    final DataSourceBuilder builder = pathlingContext.read();
    final IllegalArgumentException exception = assertThrows(IllegalArgumentException.class,
        () -> builder.bundles("path", null, "application/fhir+json"));
    assertEquals("Argument must not be null", exception.getMessage());
  }

  @Test
  void bundlesWithNullMimeTypeShouldThrowIllegalArgumentException() {
    final DataSourceBuilder builder = pathlingContext.read();
    final Set<String> resources = Set.of("Patient");
    final IllegalArgumentException exception = assertThrows(IllegalArgumentException.class,
        () -> builder.bundles("path", resources, null));
    assertEquals("Argument must not be null", exception.getMessage());
  }

  @Test
  void parquetWithNullPathShouldThrowIllegalArgumentException() {
    final DataSourceBuilder builder = pathlingContext.read();
    final IllegalArgumentException exception = assertThrows(IllegalArgumentException.class,
        () -> builder.parquet(null));
    assertEquals("Argument must not be null", exception.getMessage());
  }

  @Test
  void parquetWithNullFileNameMapperShouldThrowIllegalArgumentException() {
    final DataSourceBuilder builder = pathlingContext.read();
    final IllegalArgumentException exception = assertThrows(IllegalArgumentException.class,
        () -> builder.parquet("path", null));
    assertEquals("Argument must not be null", exception.getMessage());
  }

  @Test
  void deltaWithNullPathShouldThrowIllegalArgumentException() {
    final DataSourceBuilder builder = pathlingContext.read();
    final IllegalArgumentException exception = assertThrows(IllegalArgumentException.class,
        () -> builder.delta(null));
    assertEquals("Argument must not be null", exception.getMessage());
  }

  @Test
  void tablesWithNullSchemaShouldThrowIllegalArgumentException() {
    final DataSourceBuilder builder = pathlingContext.read();
    final IllegalArgumentException exception = assertThrows(IllegalArgumentException.class,
        () -> builder.tables(null));
    assertEquals("Argument must not be null", exception.getMessage());
  }

  @Test
  void bulkWithNullClientShouldThrowIllegalArgumentException() {
    final DataSourceBuilder builder = pathlingContext.read();
    final IllegalArgumentException exception = assertThrows(IllegalArgumentException.class,
        () -> builder.bulk(null));
    assertEquals("Argument must not be null", exception.getMessage());
  }

  // Null Parameter Validation Tests for DataSinkBuilder
  @Test
  void sinkNdjsonWithNullPathShouldThrowIllegalArgumentException() {
    final QueryableDataSource data = pathlingContext.read()
        .ndjson(TEST_DATA_PATH.resolve("ndjson").toString());
    final DataSinkBuilder builder = data.write();
    final IllegalArgumentException exception = assertThrows(IllegalArgumentException.class,
        () -> builder.ndjson(null));
    assertEquals("Argument must not be null", exception.getMessage());
  }

  @Test
  void sinkNdjsonWithNullFileNameMapperShouldThrowIllegalArgumentException() {
    final QueryableDataSource data = pathlingContext.read()
        .ndjson(TEST_DATA_PATH.resolve("ndjson").toString());
    final DataSinkBuilder builder = data.write();
    final IllegalArgumentException exception = assertThrows(IllegalArgumentException.class,
        () -> builder.ndjson("path", null));
    assertEquals("Argument must not be null", exception.getMessage());
  }

  @Test
  void sinkParquetWithNullPathShouldThrowIllegalArgumentException() {
    final QueryableDataSource data = pathlingContext.read()
        .ndjson(TEST_DATA_PATH.resolve("ndjson").toString());
    final DataSinkBuilder builder = data.write();
    final IllegalArgumentException exception = assertThrows(IllegalArgumentException.class,
        () -> builder.parquet(null));
    assertEquals("Argument must not be null", exception.getMessage());
  }

  @Test
  void sinkParquetWithNullFileNameMapperShouldThrowIllegalArgumentException() {
    final QueryableDataSource data = pathlingContext.read()
        .ndjson(TEST_DATA_PATH.resolve("ndjson").toString());
    final DataSinkBuilder builder = data.write();
    final IllegalArgumentException exception = assertThrows(IllegalArgumentException.class,
        () -> builder.parquet("path", null));
    assertEquals("Argument must not be null", exception.getMessage());
  }

  @Test
  void sinkDeltaWithNullPathShouldThrowIllegalArgumentException() {
    final QueryableDataSource data = pathlingContext.read()
        .ndjson(TEST_DATA_PATH.resolve("ndjson").toString());
    final DataSinkBuilder builder = data.write();
    final IllegalArgumentException exception = assertThrows(IllegalArgumentException.class,
        () -> builder.delta(null, false));
    assertEquals("Argument must not be null", exception.getMessage());
  }

  @Test
  void sinkDeltaWithNullFileNameMapperShouldThrowIllegalArgumentException() {
    final QueryableDataSource data = pathlingContext.read()
        .ndjson(TEST_DATA_PATH.resolve("ndjson").toString());
    final DataSinkBuilder builder = data.write();
    final IllegalArgumentException exception = assertThrows(IllegalArgumentException.class,
        () -> builder.delta("path", null, false));
    assertEquals("Argument must not be null", exception.getMessage());
  }

  @Test
  void sinkTablesWithNullSchemaShouldThrowIllegalArgumentException() {
    final QueryableDataSource data = pathlingContext.read()
        .ndjson(TEST_DATA_PATH.resolve("ndjson").toString());
    final DataSinkBuilder builder = data.write();
    final IllegalArgumentException exception = assertThrows(IllegalArgumentException.class,
        () -> builder.tables(null));
    assertEquals("Argument must not be null", exception.getMessage());
  }

  @Test
  void sinkTablesWithNullFormatShouldThrowIllegalArgumentException() {
    final QueryableDataSource data = pathlingContext.read()
        .ndjson(TEST_DATA_PATH.resolve("ndjson").toString());
    final DataSinkBuilder builder = data.write();
    final IllegalArgumentException exception = assertThrows(IllegalArgumentException.class,
        () -> builder.tables("schema", null));
    assertEquals("Argument must not be null", exception.getMessage());
  }

}
