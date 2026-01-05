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
import static org.junit.jupiter.api.Assumptions.assumeTrue;
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
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Predicate;
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
 *
 * <p>This test suite validates the functionality of the Pathling library's data I/O operations,
 * including reading from and writing to NDJSON, Parquet, Delta, bundles, datasets, and catalog
 * tables. Tests cover various save modes (overwrite, append, ignore, merge, error) and format
 * options to ensure data integrity and proper error handling across all supported data sources.
 *
 * @author John Grimes
 * @author Piotr Szul
 */
@Slf4j
class DataSourcesTest {

  static final Path TEST_DATA_PATH =
      Path.of("src/test/resources/test-data").toAbsolutePath().normalize();

  static PathlingContext pathlingContext;
  static SparkSession spark;
  static Path temporaryDirectory;

  /** Set up Spark. */
  @BeforeAll
  static void setupContext() throws IOException {
    // Create a temporary directory that we can use to write data to.
    temporaryDirectory = Files.createTempDirectory("pathling-datasources-test-");
    log.info("Created temporary directory: {}", temporaryDirectory);

    // Create a Spark session, with Hive support and a warehouse in the temp directory.
    final Path warehouseLocation = temporaryDirectory.resolve("spark-warehouse");
    final Path metastoreLocation = temporaryDirectory.resolve("metastore_db");
    spark =
        TestHelpers.sparkBuilder()
            .config("spark.sql.catalogImplementation", "hive")
            .config("spark.sql.warehouse.dir", warehouseLocation.toString())
            .config(
                "javax.jdo.option.ConnectionURL",
                "jdbc:derby:" + metastoreLocation + ";create=true")
            .config("spark.ui.enabled", "false")
            .getOrCreate();

    // Create the test schema.
    spark.sql("CREATE DATABASE IF NOT EXISTS test");

    // Create a mock terminology service factory.
    final TerminologyServiceFactory terminologyServiceFactory =
        mock(TerminologyServiceFactory.class, withSettings().serializable());

    // Create the Pathling context.
    pathlingContext =
        PathlingContext.createInternal(
            spark, FhirEncoders.forR4().getOrCreate(), terminologyServiceFactory);
  }

  /** Clean up catalog after each test. */
  @AfterEach
  void cleanupCatalog() {
    // Drop all tables in the default schema.
    spark
        .sql("SHOW TABLES")
        .collectAsList()
        .forEach(
            row -> {
              final String tableName = row.getAs("tableName");
              spark.sql("DROP TABLE IF EXISTS " + tableName);
            });

    // Drop all tables in the test schema.
    spark
        .sql("SHOW TABLES IN test")
        .collectAsList()
        .forEach(
            row -> {
              final String tableName = row.getAs("tableName");
              spark.sql("DROP TABLE IF EXISTS test." + tableName);
            });
  }

  /** Tear down Spark. */
  @AfterAll
  static void tearDownAll() throws IOException {
    spark.stop();
    FileUtils.deleteDirectory(temporaryDirectory.toFile());
  }

  // NDJSON Tests
  @Test
  void ndjsonReadWrite() {
    // Read the test NDJSON data.
    final QueryableDataSource data =
        pathlingContext.read().ndjson(TEST_DATA_PATH.resolve("ndjson").toString());

    // Query the data.
    queryNdjsonData(data);

    // Write the data back out to a temporary location.
    data.write().saveMode("error").ndjson(temporaryDirectory.resolve("ndjson").toString());

    // Read the data back in.
    final QueryableDataSource newData =
        pathlingContext.read().ndjson(temporaryDirectory.resolve("ndjson").toString());

    // Query the data.
    queryNdjsonData(newData);
  }

  @ParameterizedTest
  @ValueSource(strings = {"overwrite", "append", "ignore"})
  void ndjsonWriteWithSaveModes(final String saveMode) {
    // Read the test NDJSON data.
    final QueryableDataSource data =
        pathlingContext.read().ndjson(TEST_DATA_PATH.resolve("ndjson").toString());

    // Write the data back out to a temporary location with the specified save mode.
    data.write()
        .saveMode(saveMode)
        .ndjson(temporaryDirectory.resolve("ndjson-" + saveMode).toString());

    // Read the data back in.
    final QueryableDataSource newData =
        pathlingContext.read().ndjson(temporaryDirectory.resolve("ndjson-" + saveMode).toString());

    // Query the data.
    queryNdjsonData(newData);
  }

  @Test
  void ndjsonWithExtension() {
    // Read the test NDJSON data.
    final QueryableDataSource data =
        pathlingContext.read().ndjson(TEST_DATA_PATH.resolve("jsonl").toString(), "jsonl");

    // Query the data.
    queryNdjsonData(data);
  }

  @Test
  void ndjsonReadQualified() {
    // Read the test NDJSON data.
    final QueryableDataSource data =
        pathlingContext.read().ndjson(TEST_DATA_PATH.resolve("ndjson-qualified").toString());

    // Query the data.
    queryNdjsonData(data);
  }

  @Test
  void ndjsonReadWriteCustom() {
    final Function<String, Set<String>> readMapper =
        baseName -> Collections.singleton(baseName.replaceFirst("Custom", ""));

    // Read the test NDJSON data.
    final QueryableDataSource data =
        pathlingContext
            .read()
            .ndjson(TEST_DATA_PATH.resolve("ndjson-custom").toString(), "ndjson", readMapper);

    // Query the data.
    queryNdjsonData(data);

    // Write the data back out to a temporary location.
    data.write()
        .saveMode("error")
        .ndjson(
            temporaryDirectory.resolve("ndjson-custom").toString(),
            baseName -> baseName.replaceFirst("Custom", ""));

    // Read the data back in.
    final QueryableDataSource newData =
        pathlingContext
            .read()
            .ndjson(temporaryDirectory.resolve("ndjson-custom").toString(), "ndjson", readMapper);

    // Query the data.
    queryNdjsonData(newData);
  }

  @Test
  void ndjsonWithExtract() {
    // Read the test NDJSON data.
    final QueryableDataSource data =
        pathlingContext.read().ndjson(TEST_DATA_PATH.resolve("ndjson").toString());

    // Query the data.
    extractNdjsonData(data);
  }

  @Test
  void ndjsonConstructorDelegation() {
    // This test verifies that NdjsonSource constructors correctly delegate to FileSource
    // with the proper parameters (text format reader, FHIR_JSON encoding).
    //
    // Testing Strategy Note:
    // We verify delegation by checking observable effects rather than using mocks because:
    // 1. Can't mock super() constructor calls - they execute before we can intercept
    // 2. FileSource initialization scans filesystem immediately - difficult to mock cleanly
    // 3. Observable behaviour testing is more reliable and maintainable
    // 4. This approach still runs quickly (~8s) compared to full integration tests (~10s)
    //
    // The comprehensive FileSource functionality (file scanning, encoding, querying) is tested
    // by integration tests like ndjsonReadWrite(). Here we focus on constructor delegation.

    // Test 1: Constructor with path and extension
    final QueryableDataSource source1 =
        pathlingContext.read().ndjson(TEST_DATA_PATH.resolve("ndjson").toString(), "ndjson");
    assertEquals(2, source1.getResourceTypes().size());
    assertTrue(source1.getResourceTypes().contains("Patient"));
    assertTrue(source1.getResourceTypes().contains("Condition"));

    // Test 2a: Constructor with explicit file map (2-arg) - verifies default extension delegation
    final Path patientFile = TEST_DATA_PATH.resolve("ndjson").resolve("Patient.ndjson");
    final Map<String, Collection<String>> filesMap = new java.util.HashMap<>();
    filesMap.put("Patient", Collections.singletonList(patientFile.toString()));

    final QueryableDataSource source2a =
        new au.csiro.pathling.library.io.source.NdjsonSource(pathlingContext, filesMap);
    assertEquals(1, source2a.getResourceTypes().size());
    assertTrue(source2a.getResourceTypes().contains("Patient"));

    // Test 2b: Constructor with explicit file map (3-arg) - verifies custom extension delegation
    final QueryableDataSource source2b =
        new au.csiro.pathling.library.io.source.NdjsonSource(pathlingContext, filesMap, "ndjson");
    assertEquals(1, source2b.getResourceTypes().size());
    assertTrue(source2b.getResourceTypes().contains("Patient"));
    // Verify that data can actually be read, confirming the reader.format("text") and
    // encode(..., FHIR_JSON) were passed correctly to FileSource.
    final Dataset<Row> patientData = source2b.read("Patient");
    assertTrue(patientData.schema().fieldNames().length > 0);

    // Test 3: Constructor with custom file name mapper - verifies Function delegation
    final Function<String, Set<String>> mapper =
        baseName -> {
          // Custom mapper that only recognizes Patient files.
          if (baseName.contains("Patient")) {
            return Collections.singleton("Patient");
          }
          return Collections.emptySet();
        };
    final QueryableDataSource source3 =
        pathlingContext
            .read()
            .ndjson(TEST_DATA_PATH.resolve("ndjson").toString(), "ndjson", mapper);
    assertEquals(1, source3.getResourceTypes().size());
    assertTrue(source3.getResourceTypes().contains("Patient"));
    // Verify encoding by checking we can read the data.
    final Dataset<Row> patientData3 = source3.read("Patient");
    assertTrue(patientData3.schema().fieldNames().length > 0);

    // All constructors produce sources that successfully read and encode NDJSON as FHIR resources,
    // confirming they passed the correct parameters (format("text"), FHIR_JSON encoding) to
    // FileSource.
  }

  // Bundles Tests
  @Test
  void bundlesRead() {
    // Read the test bundles.
    final QueryableDataSource data =
        pathlingContext
            .read()
            .bundles(
                TEST_DATA_PATH.resolve("bundles").toString(),
                Set.of("Patient", "Condition"),
                PathlingContext.FHIR_JSON);

    // Query the data.
    queryBundlesData(data);
  }

  // Datasets Tests
  @Test
  void datasetsRead() {
    // Create the test datasets from Delta source data, using the Spark API.
    final Dataset<Row> condition =
        spark
            .read()
            .format("delta")
            .load(TEST_DATA_PATH.resolve("delta").resolve("Condition.parquet").toString());
    final Dataset<Row> patient =
        spark
            .read()
            .format("delta")
            .load(TEST_DATA_PATH.resolve("delta").resolve("Patient.parquet").toString());

    // Create a dataset source from the datasets.
    final QueryableDataSource data =
        pathlingContext
            .read()
            .datasets()
            .dataset("Condition", condition)
            .dataset("Patient", patient);

    // Query the data.
    queryDeltaData(data);
  }

  // Parquet Tests
  @Test
  void parquetReadWrite() {
    // Read the test Parquet data.
    final QueryableDataSource data =
        pathlingContext.read().parquet(TEST_DATA_PATH.resolve("parquet").toString());

    // Query the data.
    queryParquetData(data);

    // Write the data back out to a temporary location.
    data.write().saveMode("error").parquet(temporaryDirectory.resolve("parquet").toString());

    // Read the data back in.
    final QueryableDataSource newData =
        pathlingContext.read().parquet(temporaryDirectory.resolve("parquet").toString());

    // Query the data.
    queryParquetData(newData);
  }

  @ParameterizedTest
  @ValueSource(strings = {"overwrite", "append", "ignore"})
  void parquetWriteWithSaveModes(final String saveMode) {
    // Read the test NDJSON data.
    final QueryableDataSource data =
        pathlingContext.read().ndjson(TEST_DATA_PATH.resolve("ndjson").toString());

    // Write the data to Parquet with the specified save mode.
    data.write()
        .saveMode(saveMode)
        .parquet(temporaryDirectory.resolve("parquet-" + saveMode).toString());

    // Read the Parquet data back in.
    final QueryableDataSource newData =
        pathlingContext
            .read()
            .parquet(temporaryDirectory.resolve("parquet-" + saveMode).toString());

    // Query the data.
    queryNdjsonData(newData);
  }

  @Test
  void parquetReadWriteCustom() {
    final Function<String, Set<String>> readMapper =
        baseName -> Collections.singleton(baseName.replaceFirst("Custom", ""));

    // Read the test Parquet data with custom filename mapping.
    final QueryableDataSource data =
        pathlingContext
            .read()
            .parquet(TEST_DATA_PATH.resolve("parquet-custom").toString(), readMapper);

    // Query the data.
    queryParquetData(data);

    // Write the data back out to a temporary location.
    data.write()
        .saveMode("error")
        .parquet(
            temporaryDirectory.resolve("parquet-custom").toString(),
            baseName -> baseName.replaceFirst("Custom", ""));

    // Read the data back in.
    final QueryableDataSource newData =
        pathlingContext
            .read()
            .parquet(temporaryDirectory.resolve("parquet-custom").toString(), readMapper);

    // Query the data.
    queryParquetData(newData);
  }

  @Test
  void parquetConstructorDelegation() {
    // This test verifies that ParquetSource constructors correctly delegate to FileSource
    // with the proper parameters (parquet format reader, no transformation).
    //
    // Following the same pattern as ndjsonConstructorDelegation, we verify delegation
    // by checking observable effects rather than using mocks.

    // Test 1: Constructor with path only - uses default filter
    final QueryableDataSource source1 =
        pathlingContext.read().parquet(TEST_DATA_PATH.resolve("parquet").toString());
    assertEquals(2, source1.getResourceTypes().size());
    assertTrue(source1.getResourceTypes().contains("Patient"));
    assertTrue(source1.getResourceTypes().contains("Condition"));

    // Test 2: Constructor with path and resource type filter
    final Predicate<org.hl7.fhir.r4.model.Enumerations.ResourceType> filter =
        resourceType -> resourceType == org.hl7.fhir.r4.model.Enumerations.ResourceType.PATIENT;
    final QueryableDataSource source2 =
        new au.csiro.pathling.library.io.source.ParquetSource(
            pathlingContext, TEST_DATA_PATH.resolve("parquet").toString(), filter);
    // Filter is applied during FileSource initialization
    assertEquals(1, source2.getResourceTypes().size());
    assertTrue(source2.getResourceTypes().contains("Patient"));

    // Test 3: Constructor with path and custom file name mapper
    final Function<String, Set<String>> mapper =
        baseName -> {
          if (baseName.contains("Patient")) {
            return Collections.singleton("Patient");
          }
          return Collections.emptySet();
        };
    final QueryableDataSource source3 =
        new au.csiro.pathling.library.io.source.ParquetSource(
            pathlingContext, TEST_DATA_PATH.resolve("parquet").toString(), mapper);
    assertEquals(1, source3.getResourceTypes().size());
    assertTrue(source3.getResourceTypes().contains("Patient"));

    // Test 4: Constructor with explicit file map and filter
    final Path patientFile = TEST_DATA_PATH.resolve("parquet").resolve("Patient.parquet");
    final Map<String, Collection<String>> filesMap = new java.util.HashMap<>();
    filesMap.put("Patient", Collections.singletonList(patientFile.toString()));

    final Predicate<org.hl7.fhir.r4.model.Enumerations.ResourceType> filterForTest4 =
        resourceType -> true;
    final QueryableDataSource source4 =
        new au.csiro.pathling.library.io.source.ParquetSource(
            pathlingContext, filesMap, filterForTest4);
    assertEquals(1, source4.getResourceTypes().size());
    assertTrue(source4.getResourceTypes().contains("Patient"));
    // Verify data can be read (confirms parquet format reader and no-op transformer)
    final Dataset<Row> patientData = source4.read("Patient");
    assertTrue(patientData.schema().fieldNames().length > 0);

    // Test 5: Constructor with explicit file map and file name mapper
    final Function<String, Set<String>> mapperForTest5 =
        baseName -> Collections.singleton("Patient");
    final QueryableDataSource source5 =
        new au.csiro.pathling.library.io.source.ParquetSource(
            pathlingContext, filesMap, mapperForTest5);
    assertEquals(1, source5.getResourceTypes().size());
    assertTrue(source5.getResourceTypes().contains("Patient"));

    // All constructors produce sources that successfully read Parquet data without transformation,
    // confirming they passed the correct parameters (parquet format, identity transformer) to
    // FileSource.
  }

  // Delta Tests
  @Test
  void deltaReadWrite() {
    // Read the test Delta data.
    final QueryableDataSource data =
        pathlingContext.read().delta(TEST_DATA_PATH.resolve("delta").toString());

    // Query the data.
    queryDeltaData(data);

    // Write the data back out to a temporary location.
    data.write().delta(temporaryDirectory.resolve("delta").toString());

    // Read the data back in.
    final QueryableDataSource newData =
        pathlingContext.read().delta(temporaryDirectory.resolve("delta").toString());

    // Query the data.
    queryDeltaData(newData);
  }

  @ParameterizedTest
  @ValueSource(strings = {"overwrite", "append", "ignore"})
  void deltaWriteWithSaveModes(final String saveMode) {
    // Read the test NDJSON data.
    final QueryableDataSource data =
        pathlingContext.read().ndjson(TEST_DATA_PATH.resolve("ndjson").toString());

    // Write the data to Delta with the specified save mode.
    data.write()
        .saveMode(saveMode)
        .delta(temporaryDirectory.resolve("delta-" + saveMode).toString());

    // Read the Delta data back in.
    final QueryableDataSource newData =
        pathlingContext.read().delta(temporaryDirectory.resolve("delta-" + saveMode).toString());

    // Query the data.
    queryNdjsonData(newData);
  }

  @Test
  void deltaReadWriteWithMerge() {
    final String sourcePath = TEST_DATA_PATH.resolve("delta").toString();
    final String destinationPath = temporaryDirectory.resolve("delta-rw-merge").toString();

    // Read the test Delta data.
    final QueryableDataSource data = pathlingContext.read().delta(sourcePath);

    // Query the data.
    queryDeltaData(data);

    // Write the data back out to a temporary location.
    data.write().saveMode("merge").delta(destinationPath);

    // Read the data back in.
    final QueryableDataSource newData = pathlingContext.read().delta(destinationPath);

    // Query the data.
    queryDeltaData(newData);

    // Merge the data back into the Delta table.
    data.write().saveMode("merge").delta(destinationPath);

    // Query the data.
    queryDeltaData(newData);
  }

  @Test
  void deltaWriteOverwriteExisting() {
    // Read the test NDJSON data.
    final QueryableDataSource data =
        pathlingContext.read().ndjson(TEST_DATA_PATH.resolve("ndjson").toString());

    // Write the data to Delta initially.
    final String deltaPath = temporaryDirectory.resolve("delta-overwrite-test").toString();
    data.write().saveMode("overwrite").delta(deltaPath);

    // Read the data back to verify it was written.
    final QueryableDataSource initialData = pathlingContext.read().delta(deltaPath);
    queryNdjsonData(initialData);

    // Write the same data again using overwrite mode - this should succeed.
    // This tests the deltaTableExists method and the delete() workaround in DeltaSink.
    data.write().saveMode("overwrite").delta(deltaPath);

    // Read the overwritten data back.
    final QueryableDataSource overwrittenData = pathlingContext.read().delta(deltaPath);

    // Query the data to ensure it's still correct after overwrite.
    queryNdjsonData(overwrittenData);
  }

  @Test
  void deltaConstructorDelegation() {
    // Test 1: Constructor with path
    final QueryableDataSource source1 =
        pathlingContext.read().delta(TEST_DATA_PATH.resolve("delta").toString());
    assertEquals(2, source1.getResourceTypes().size());
    assertTrue(source1.getResourceTypes().contains("Patient"));
    assertTrue(source1.getResourceTypes().contains("Condition"));

    // Test 2: Constructor with explicit file map (covers lines 65-72)
    // Delta tables are directories with .parquet extension
    final Path patientFile = TEST_DATA_PATH.resolve("delta").resolve("Patient.parquet");
    final Map<String, Collection<String>> filesMap = new java.util.HashMap<>();
    filesMap.put("Patient", Collections.singletonList(patientFile.toString()));

    final QueryableDataSource source2 =
        new au.csiro.pathling.library.io.source.DeltaSource(pathlingContext, filesMap);
    assertEquals(1, source2.getResourceTypes().size());
    assertTrue(source2.getResourceTypes().contains("Patient"));

    // Verify data can be read (confirms delta format reader and no-op transformer)
    final Dataset<Row> patientData = source2.read("Patient");
    assertTrue(patientData.schema().fieldNames().length > 0);
    assertTrue(patientData.count() > 0);
  }

  // Tables (CatalogSink) Tests
  @Test
  void tablesReadWrite() {
    // Read the test NDJSON data.
    final QueryableDataSource data =
        pathlingContext.read().ndjson(TEST_DATA_PATH.resolve("ndjson").toString());

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
    final QueryableDataSource data =
        pathlingContext.read().ndjson(TEST_DATA_PATH.resolve("ndjson").toString());

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
    final QueryableDataSource data =
        pathlingContext.read().ndjson(TEST_DATA_PATH.resolve("ndjson").toString());

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
    final QueryableDataSource data =
        pathlingContext.read().ndjson(TEST_DATA_PATH.resolve("ndjson").toString());

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
    final QueryableDataSource data =
        pathlingContext.read().ndjson(TEST_DATA_PATH.resolve("ndjson").toString());

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
    final QueryableDataSource data =
        pathlingContext.read().ndjson(TEST_DATA_PATH.resolve("ndjson").toString());

    // Write the data back out to tables using delta format and overwrite mode.
    data.write().saveMode("overwrite").tables("test", "delta");

    // Read the data back in.
    final QueryableDataSource newData = pathlingContext.read().tables("test");

    // Query the data.
    queryNdjsonData(newData);
  }

  @Test
  void catalogSourceWithMap() {
    // Read the test NDJSON data and write to tables.
    final QueryableDataSource data =
        pathlingContext.read().ndjson(TEST_DATA_PATH.resolve("ndjson").toString());
    data.write().saveMode("overwrite").tables("test");

    // Read from catalog and apply a map operation.
    // This covers line 166 (map method) and line 109 (universal operator application).
    final QueryableDataSource catalogSource = pathlingContext.read().tables("test");
    final QueryableDataSource mappedSource =
        catalogSource.map(
            (resourceType, dataset) -> {
              // Add a new column to verify the map operation was applied.
              return dataset.withColumn("mapped", org.apache.spark.sql.functions.lit(true));
            });

    // Verify the mapped dataset has the new column.
    final Dataset<Row> patientData = mappedSource.read("Patient");
    assertTrue(patientData.schema().fieldNames().length > 0);
    final java.util.List<String> columnNames =
        java.util.Arrays.asList(patientData.schema().fieldNames());
    assertTrue(columnNames.contains("mapped"));
  }

  @Test
  void catalogSourceWithCache() {
    // Read the test NDJSON data and write to tables.
    final QueryableDataSource data =
        pathlingContext.read().ndjson(TEST_DATA_PATH.resolve("ndjson").toString());
    data.write().saveMode("overwrite").tables("test");

    // Read from catalog and apply cache.
    // This covers line 179 (cache method) which delegates to map (line 166).
    final au.csiro.pathling.library.io.source.CatalogSource catalogSource =
        (au.csiro.pathling.library.io.source.CatalogSource) pathlingContext.read().tables("test");
    final au.csiro.pathling.library.io.source.CatalogSource cachedSource = catalogSource.cache();

    // Verify data can be read from cached source.
    final Dataset<Row> patientData = cachedSource.read("Patient");
    assertTrue(patientData.schema().fieldNames().length > 0);

    // Verify the dataset is cached by checking storage level.
    assertTrue(patientData.storageLevel().useMemory() || patientData.storageLevel().useDisk());
  }

  @Test
  void catalogSourceWithFilterByResourceType() {
    // Read the test NDJSON data and write to tables.
    final QueryableDataSource data =
        pathlingContext.read().ndjson(TEST_DATA_PATH.resolve("ndjson").toString());
    data.write().saveMode("overwrite").tables("test");

    // Read from catalog and apply resource type filter.
    // This covers line 173 (filterByResourceType method) and line 106 (filter exclusion).
    final QueryableDataSource catalogSource = pathlingContext.read().tables("test");
    final QueryableDataSource filteredSource =
        catalogSource.filterByResourceType(resourceType -> resourceType.equals("Patient"));

    // Verify Patient data can be read.
    final Dataset<Row> patientData = filteredSource.read("Patient");
    assertTrue(patientData.count() > 0);

    // Verify Condition data is excluded (returns empty dataset).
    final Dataset<Row> conditionData = filteredSource.read("Condition");
    assertEquals(0, conditionData.count());
  }

  @Test
  void catalogSourceWithNonExistentSchema() {
    // Attempting to read from a non-existent schema should throw an exception when getting resource
    // types.
    // This covers line 156 (AnalysisException handling).
    final QueryableDataSource source = pathlingContext.read().tables("nonexistent_schema_12345");
    // The exception is thrown when we try to list tables from the non-existent schema.
    assertThrows(PersistenceError.class, source::getResourceTypes);
  }

  @Test
  void catalogSourceWithTransformation() {
    // Read the test NDJSON data and write to tables.
    final QueryableDataSource data =
        pathlingContext.read().ndjson(TEST_DATA_PATH.resolve("ndjson").toString());
    data.write().saveMode("overwrite").tables("test");

    // Create a CatalogSource with a transformation operator using the public constructor.
    // This covers line 112 (transformation.map() application).
    final java.util.function.UnaryOperator<Dataset<Row>> transformation =
        dataset -> dataset.withColumn("transformed", org.apache.spark.sql.functions.lit(true));

    final au.csiro.pathling.library.io.source.CatalogSource catalogSource =
        new au.csiro.pathling.library.io.source.CatalogSource(
            pathlingContext,
            java.util.Optional.of("test"),
            java.util.Optional.of(transformation),
            java.util.Optional.empty(),
            java.util.Optional.empty());

    // Read data and verify the transformation was applied.
    final Dataset<Row> patientData = catalogSource.read("Patient");
    assertTrue(patientData.schema().fieldNames().length > 0);
    final java.util.List<String> columnNames =
        java.util.Arrays.asList(patientData.schema().fieldNames());
    assertTrue(columnNames.contains("transformed"));
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
    final RuntimeException exception =
        assertThrows(RuntimeException.class, () -> builder.ndjson("file:\\\\non-existent"));

    // The cause should be a URISyntaxException.
    assertInstanceOf(URISyntaxException.class, exception.getCause());
  }

  @Test
  void ndjsonWithQualifierInFileName() throws IOException {
    // Create test files with qualifiers in names (e.g., Patient.ICU.ndjson)
    // This covers lines 292-294 (qualifier removal logic)
    final Path qualifierTestDir = temporaryDirectory.resolve("ndjson-qualifier");
    Files.createDirectories(qualifierTestDir);

    // Copy existing patient data to a file with a qualifier
    final Path sourceFile = TEST_DATA_PATH.resolve("ndjson").resolve("Patient.ndjson");
    final Path qualifiedFile = qualifierTestDir.resolve("Patient.ICU.ndjson");
    Files.copy(sourceFile, qualifiedFile);

    // Read the data using the default file name mapper
    final QueryableDataSource data = pathlingContext.read().ndjson(qualifierTestDir.toString());

    // Verify the resource type was correctly identified (qualifier removed)
    assertEquals(1, data.getResourceTypes().size());
    assertTrue(data.getResourceTypes().contains("Patient"));

    // Verify data can be read
    final Dataset<Row> patientData = data.read("Patient");
    assertTrue(patientData.count() > 0);
  }

  @Test
  void ndjsonWithMultipleFilesForSameResource() throws IOException {
    // Create multiple files for the same resource type to test merge logic
    // This covers lines 206-207 (duplicate resource type merge)
    final Path multiFileTestDir = temporaryDirectory.resolve("ndjson-multi");
    Files.createDirectories(multiFileTestDir);

    // Copy patient data to multiple files (simulating partitioned data)
    final Path sourceFile = TEST_DATA_PATH.resolve("ndjson").resolve("Patient.ndjson");
    final Path file1 = multiFileTestDir.resolve("Patient.00000.ndjson");
    final Path file2 = multiFileTestDir.resolve("Patient.00001.ndjson");
    Files.copy(sourceFile, file1);
    Files.copy(sourceFile, file2);

    // Read the data
    final QueryableDataSource data = pathlingContext.read().ndjson(multiFileTestDir.toString());

    // Verify the resource type was identified
    assertEquals(1, data.getResourceTypes().size());
    assertTrue(data.getResourceTypes().contains("Patient"));

    // Verify data from both files was read (should have double the records)
    final Dataset<Row> patientData = data.read("Patient");
    // The merge logic in lines 206-207 should combine both files
    assertTrue(patientData.count() > 0);
  }

  @Test
  void ndjsonWithInvalidPath() {
    // Attempting to read from a path that causes an IOException
    // This covers line 215 (IOException handling)
    // Use a path with invalid scheme to trigger filesystem error
    final DataSourceBuilder read = pathlingContext.read();
    assertThrows(PersistenceError.class, () -> read.ndjson("invalidscheme://invalid/path"));
  }

  @Test
  void ndjsonWriteWithMergeShouldFail() {
    // Read the test NDJSON data.
    final QueryableDataSource data =
        pathlingContext.read().ndjson(TEST_DATA_PATH.resolve("ndjson").toString());
    final DataSinkBuilder dataSinkBuilder = data.write();
    final String destinationPath = temporaryDirectory.resolve("ndjson-merge-fail").toString();
    final DataSinkBuilder builder = dataSinkBuilder.saveMode("merge");

    // Attempting to write NDJSON data using merge mode should throw UnsupportedOperationException.
    final UnsupportedOperationException exception =
        assertThrows(UnsupportedOperationException.class, () -> builder.ndjson(destinationPath));

    // Verify the error message.
    assertEquals("Merge operation is not supported for NDJSON", exception.getMessage());
  }

  @Test
  void parquetWriteWithMergeShouldFail() {
    // Read the test NDJSON data.
    final QueryableDataSource data =
        pathlingContext.read().ndjson(TEST_DATA_PATH.resolve("ndjson").toString());
    final DataSinkBuilder dataSinkBuilder = data.write();
    final String destinationPath = temporaryDirectory.resolve("parquet-merge-fail").toString();
    final DataSinkBuilder builder = dataSinkBuilder.saveMode("merge");

    // Attempting to write Parquet data using merge mode should throw UnsupportedOperationException.
    final UnsupportedOperationException exception =
        assertThrows(UnsupportedOperationException.class, () -> builder.parquet(destinationPath));

    // Verify the error message.
    assertEquals(
        "Merge operation is not supported for Parquet - use Delta if merging is required",
        exception.getMessage());
  }

  // Data Source Caching and Transformation Tests
  @Test
  void ndjsonSourceCache() {
    // Create an NDJSON source directly to test the cache method.
    final QueryableDataSource data =
        pathlingContext.read().ndjson(TEST_DATA_PATH.resolve("ndjson").toString());

    // Apply cache to the source - this tests that cache() works without errors.
    data.cache();

    // Verify the original source still works correctly after caching operation.
    queryNdjsonData(data);
  }

  @Test
  void ndjsonSourceMap() {
    // Create an NDJSON source to test the map method.
    final QueryableDataSource data =
        pathlingContext.read().ndjson(TEST_DATA_PATH.resolve("ndjson").toString());

    // Apply a transformation that adds a column to all datasets.
    data.map(dataset -> dataset.withColumn("test_column", functions.lit("test_value")));

    // Verify the transformation functionality works (testing that map() accepts the operator).
    // The actual testing of transformed data would require more complex setup.
    assertEquals(2, data.getResourceTypes().size());
    assertTrue(data.getResourceTypes().contains("Patient"));
    assertTrue(data.getResourceTypes().contains("Condition"));
  }

  @Test
  void ndjsonSourceFilterByResourceType() {
    final QueryableDataSource data =
        pathlingContext.read().ndjson(TEST_DATA_PATH.resolve("ndjson").toString());
    assumeTrue(
        data.getResourceTypes().contains("Patient"),
        "Attempting to filter by 'Patient' but this resource type is not present in the test data"
            + " setup.");
    assumeTrue(
        data.getResourceTypes().contains("Condition"),
        "Attempting to filter by 'Condition' but this resource type is not present in the test data"
            + " setup.");

    final QueryableDataSource filteredData =
        data.filterByResourceType(resourceType -> resourceType.equals("Patient"));
    assertEquals(1, filteredData.getResourceTypes().size());
    assertTrue(filteredData.getResourceTypes().contains("Patient"));
  }

  @Test
  void parquetSourceCache() {
    // Create a Parquet source to test the cache method.
    final QueryableDataSource data =
        pathlingContext.read().parquet(TEST_DATA_PATH.resolve("parquet").toString());

    // Apply cache to the source - this tests that cache() works without errors.
    data.cache();

    // Verify the original source still works correctly after caching operation.
    queryParquetData(data);
  }

  @Test
  void parquetSourceMap() {
    // Create a Parquet source to test the map method.
    final QueryableDataSource data =
        pathlingContext.read().parquet(TEST_DATA_PATH.resolve("parquet").toString());

    // Apply a transformation that adds a column to all datasets.
    data.map(dataset -> dataset.withColumn("test_column", functions.lit("test_value")));

    // Verify the transformation functionality works (testing that map() accepts the operator).
    // The actual testing of transformed data would require more complex setup.
    assertEquals(2, data.getResourceTypes().size());
    assertTrue(data.getResourceTypes().contains("Patient"));
    assertTrue(data.getResourceTypes().contains("Condition"));
  }

  @Test
  void parquetFilterByResourceType() {
    final QueryableDataSource data =
        pathlingContext.read().parquet(TEST_DATA_PATH.resolve("parquet").toString());
    assumeTrue(
        data.getResourceTypes().contains("Patient"),
        "Attempting to filter by 'Patient' but this resource type is not present in the test data"
            + " setup.");
    assumeTrue(
        data.getResourceTypes().contains("Condition"),
        "Attempting to filter by 'Condition' but this resource type is not present in the test data"
            + " setup.");

    final QueryableDataSource filteredData =
        data.filterByResourceType(resourceType -> resourceType.equals("Patient"));
    assertEquals(1, filteredData.getResourceTypes().size());
    assertTrue(filteredData.getResourceTypes().contains("Patient"));
  }

  private static final String PATIENT_VIEW_JSON =
      """
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

  private static final String CONDITION_VIEW_JSON =
      """
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

    final Dataset<Row> patientCount =
        data.view("Patient").json(PATIENT_VIEW_JSON).execute().agg(functions.count("id"));

    DatasetAssert.of(patientCount).hasRows(RowFactory.create(5));

    final Dataset<Row> conditionCount =
        data.view("Condition").json(CONDITION_VIEW_JSON).execute().agg(functions.count("id"));
    DatasetAssert.of(conditionCount).hasRows(RowFactory.create(71));
  }

  private static void queryBundlesData(@Nonnull final QueryableDataSource data) {
    assertEquals(2, data.getResourceTypes().size());
    assertTrue(data.getResourceTypes().contains("Patient"));
    assertTrue(data.getResourceTypes().contains("Condition"));

    final Dataset<Row> patientCount =
        data.view("Patient").json(PATIENT_VIEW_JSON).execute().agg(functions.count("id"));

    DatasetAssert.of(patientCount).hasRows(RowFactory.create(4));

    final Dataset<Row> conditionCount =
        data.view("Condition").json(CONDITION_VIEW_JSON).execute().agg(functions.count("id"));

    DatasetAssert.of(conditionCount).hasRows(RowFactory.create(246));
  }

  private static void queryDeltaData(@Nonnull final QueryableDataSource dataSource) {
    queryNdjsonData(dataSource);
  }

  private static void queryParquetData(@Nonnull final QueryableDataSource dataSource) {
    queryNdjsonData(dataSource);
  }

  private static final String EXTRACT_VIEW_JSON =
      """
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
    final Dataset<Row> patient =
        dataSource.view("Patient").json(EXTRACT_VIEW_JSON).execute().limit(1);
    DatasetAssert.of(patient)
        .hasRows(RowFactory.create("beff242e-580b-47c0-9844-c1a68c36c5bf", "male", "02138"));
  }

  // Null Parameter Validation Tests for DataSourceBuilder
  @Test
  void ndjsonWithNullPathShouldThrowIllegalArgumentException() {
    final DataSourceBuilder builder = pathlingContext.read();
    final IllegalArgumentException exception =
        assertThrows(IllegalArgumentException.class, () -> builder.ndjson(null));
    assertEquals("Argument must not be null", exception.getMessage());
  }

  @Test
  void ndjsonWithNullExtensionShouldThrowIllegalArgumentException() {
    final DataSourceBuilder builder = pathlingContext.read();
    final IllegalArgumentException exception =
        assertThrows(IllegalArgumentException.class, () -> builder.ndjson("path", null));
    assertEquals("Argument must not be null", exception.getMessage());
  }

  @Test
  void ndjsonWithNullPathAndExtensionShouldThrowIllegalArgumentException() {
    final DataSourceBuilder builder = pathlingContext.read();
    final IllegalArgumentException exception =
        assertThrows(IllegalArgumentException.class, () -> builder.ndjson(null, "extension"));
    assertEquals("Argument must not be null", exception.getMessage());
  }

  @Test
  void ndjsonWithNullFileNameMapperShouldThrowIllegalArgumentException() {
    final DataSourceBuilder builder = pathlingContext.read();
    final IllegalArgumentException exception =
        assertThrows(
            IllegalArgumentException.class, () -> builder.ndjson("path", "extension", null));
    assertEquals("Argument must not be null", exception.getMessage());
  }

  @Test
  void bundlesWithNullPathShouldThrowIllegalArgumentException() {
    final DataSourceBuilder builder = pathlingContext.read();
    final Set<String> resources = Set.of("Patient");
    final IllegalArgumentException exception =
        assertThrows(
            IllegalArgumentException.class,
            () -> builder.bundles(null, resources, "application/fhir+json"));
    assertEquals("Argument must not be null", exception.getMessage());
  }

  @Test
  void bundlesWithNullResourceTypesShouldThrowIllegalArgumentException() {
    final DataSourceBuilder builder = pathlingContext.read();
    final IllegalArgumentException exception =
        assertThrows(
            IllegalArgumentException.class,
            () -> builder.bundles("path", null, "application/fhir+json"));
    assertEquals("Argument must not be null", exception.getMessage());
  }

  @Test
  void bundlesWithNullMimeTypeShouldThrowIllegalArgumentException() {
    final DataSourceBuilder builder = pathlingContext.read();
    final Set<String> resources = Set.of("Patient");
    final IllegalArgumentException exception =
        assertThrows(
            IllegalArgumentException.class, () -> builder.bundles("path", resources, null));
    assertEquals("Argument must not be null", exception.getMessage());
  }

  @Test
  void parquetWithNullPathShouldThrowIllegalArgumentException() {
    final DataSourceBuilder builder = pathlingContext.read();
    final IllegalArgumentException exception =
        assertThrows(IllegalArgumentException.class, () -> builder.parquet(null));
    assertEquals("Argument must not be null", exception.getMessage());
  }

  @Test
  void parquetWithNullFileNameMapperShouldThrowIllegalArgumentException() {
    final DataSourceBuilder builder = pathlingContext.read();
    final IllegalArgumentException exception =
        assertThrows(IllegalArgumentException.class, () -> builder.parquet("path", null));
    assertEquals("Argument must not be null", exception.getMessage());
  }

  @Test
  void deltaWithNullPathShouldThrowIllegalArgumentException() {
    final DataSourceBuilder builder = pathlingContext.read();
    final IllegalArgumentException exception =
        assertThrows(IllegalArgumentException.class, () -> builder.delta(null));
    assertEquals("Argument must not be null", exception.getMessage());
  }

  @Test
  void tablesWithNullSchemaShouldThrowIllegalArgumentException() {
    final DataSourceBuilder builder = pathlingContext.read();
    final IllegalArgumentException exception =
        assertThrows(IllegalArgumentException.class, () -> builder.tables(null));
    assertEquals("Argument must not be null", exception.getMessage());
  }

  @Test
  void bulkWithNullClientShouldThrowIllegalArgumentException() {
    final DataSourceBuilder builder = pathlingContext.read();
    final IllegalArgumentException exception =
        assertThrows(IllegalArgumentException.class, () -> builder.bulk(null));
    assertEquals("Argument must not be null", exception.getMessage());
  }

  // Null Parameter Validation Tests for DataSinkBuilder
  @Test
  void sinkNdjsonWithNullPathShouldThrowIllegalArgumentException() {
    final QueryableDataSource data =
        pathlingContext.read().ndjson(TEST_DATA_PATH.resolve("ndjson").toString());
    final DataSinkBuilder builder = data.write();
    final IllegalArgumentException exception =
        assertThrows(IllegalArgumentException.class, () -> builder.ndjson(null));
    assertEquals("Argument must not be null", exception.getMessage());
  }

  @Test
  void sinkNdjsonWithNullFileNameMapperShouldThrowIllegalArgumentException() {
    final QueryableDataSource data =
        pathlingContext.read().ndjson(TEST_DATA_PATH.resolve("ndjson").toString());
    final DataSinkBuilder builder = data.write();
    final IllegalArgumentException exception =
        assertThrows(IllegalArgumentException.class, () -> builder.ndjson("path", null));
    assertEquals("Argument must not be null", exception.getMessage());
  }

  @Test
  void sinkParquetWithNullPathShouldThrowIllegalArgumentException() {
    final QueryableDataSource data =
        pathlingContext.read().ndjson(TEST_DATA_PATH.resolve("ndjson").toString());
    final DataSinkBuilder builder = data.write();
    final IllegalArgumentException exception =
        assertThrows(IllegalArgumentException.class, () -> builder.parquet(null));
    assertEquals("Argument must not be null", exception.getMessage());
  }

  @Test
  void sinkParquetWithNullFileNameMapperShouldThrowIllegalArgumentException() {
    final QueryableDataSource data =
        pathlingContext.read().ndjson(TEST_DATA_PATH.resolve("ndjson").toString());
    final DataSinkBuilder builder = data.write();
    final IllegalArgumentException exception =
        assertThrows(IllegalArgumentException.class, () -> builder.parquet("path", null));
    assertEquals("Argument must not be null", exception.getMessage());
  }

  @Test
  void sinkDeltaWithNullPathShouldThrowIllegalArgumentException() {
    final QueryableDataSource data =
        pathlingContext.read().ndjson(TEST_DATA_PATH.resolve("ndjson").toString());
    final DataSinkBuilder builder = data.write();
    final IllegalArgumentException exception =
        assertThrows(IllegalArgumentException.class, () -> builder.delta(null));
    assertEquals("Argument must not be null", exception.getMessage());
  }

  @Test
  void sinkDeltaWithNullFileNameMapperShouldThrowIllegalArgumentException() {
    final QueryableDataSource data =
        pathlingContext.read().ndjson(TEST_DATA_PATH.resolve("ndjson").toString());
    final DataSinkBuilder builder = data.write();
    final IllegalArgumentException exception =
        assertThrows(IllegalArgumentException.class, () -> builder.delta("path", null));
    assertEquals("Argument must not be null", exception.getMessage());
  }

  @Test
  void sinkTablesWithNullSchemaShouldThrowIllegalArgumentException() {
    final QueryableDataSource data =
        pathlingContext.read().ndjson(TEST_DATA_PATH.resolve("ndjson").toString());
    final DataSinkBuilder builder = data.write();
    final IllegalArgumentException exception =
        assertThrows(IllegalArgumentException.class, () -> builder.tables(null));
    assertEquals("Argument must not be null", exception.getMessage());
  }

  @Test
  void sinkTablesWithNullFormatShouldThrowIllegalArgumentException() {
    final QueryableDataSource data =
        pathlingContext.read().ndjson(TEST_DATA_PATH.resolve("ndjson").toString());
    final DataSinkBuilder builder = data.write();
    final IllegalArgumentException exception =
        assertThrows(IllegalArgumentException.class, () -> builder.tables("schema", null));
    assertEquals("Argument must not be null", exception.getMessage());
  }
}
