/*
 * Copyright 2023 Commonwealth Scientific and Industrial Research
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

  @Test
  void ndjsonReadWrite() {
    // Read the test NDJSON data.
    final QueryableDataSource data = pathlingContext.read()
        .ndjson(TEST_DATA_PATH.resolve("ndjson").toString());

    // Query the data.
    queryNdjsonData(data);

    // Write the data back out to a temporary location.
    data.write().ndjson(temporaryDirectory.resolve("ndjson").toString(), "error");

    // Read the data back in.
    final QueryableDataSource newData = pathlingContext.read()
        .ndjson(temporaryDirectory.resolve("ndjson").toString());

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
    data.write().ndjson(temporaryDirectory.resolve("ndjson-custom").toString(), "error",
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

  @Test
  void bundlesRead() {
    // Read the test bundles.
    final QueryableDataSource data = pathlingContext.read()
        .bundles(TEST_DATA_PATH.resolve("bundles").toString(),
            Set.of("Patient", "Condition"), PathlingContext.FHIR_JSON);

    // Query the data.
    queryBundlesData(data);
  }

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

  @Test
  void parquetReadWrite() {
    // Read the test Parquet data.
    final QueryableDataSource data = pathlingContext.read()
        .parquet(TEST_DATA_PATH.resolve("parquet").toString());

    // Query the data.
    queryParquetData(data);

    // Write the data back out to a temporary location.
    data.write().parquet(temporaryDirectory.resolve("parquet").toString(), "error");

    // Read the data back in.
    final QueryableDataSource newData = pathlingContext.read()
        .parquet(temporaryDirectory.resolve("parquet").toString());

    // Query the data.
    queryParquetData(newData);
  }

  @Test
  void deltaReadWrite() {
    // Read the test Delta data.
    final QueryableDataSource data = pathlingContext.read()
        .delta(TEST_DATA_PATH.resolve("delta").toString());

    // Query the data.
    queryDeltaData(data);

    // Write the data back out to a temporary location.
    data.write().delta(temporaryDirectory.resolve("delta").toString());

    // Read the data back in.
    final QueryableDataSource newData = pathlingContext.read()
        .delta(temporaryDirectory.resolve("delta").toString());

    // Query the data.
    queryDeltaData(newData);
  }

  @Test
  void deltaReadWriteWithMerge() {
    // Read the test Delta data.
    final QueryableDataSource data = pathlingContext.read()
        .delta(TEST_DATA_PATH.resolve("delta").toString());

    // Query the data.
    queryDeltaData(data);

    // Write the data back out to a temporary location.
    data.write().delta(temporaryDirectory.resolve("delta").toString(), "merge");

    // Read the data back in.
    final QueryableDataSource newData = pathlingContext.read()
        .delta(temporaryDirectory.resolve("delta").toString());

    // Query the data.
    queryDeltaData(newData);
  }

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
  void tablesReadWriteWithImportMode() {
    // Read the test NDJSON data.
    final QueryableDataSource data = pathlingContext.read()
        .ndjson(TEST_DATA_PATH.resolve("ndjson").toString());

    // Write the data back out to tables.
    data.write().tables("merge");

    // Read the data back in.
    final QueryableDataSource newData = pathlingContext.read().tables();

    // Query the data.
    queryNdjsonData(newData);
  }

  @Test
  void tablesReadWriteWithImportModeAndSchema() {
    // Read the test NDJSON data.
    final QueryableDataSource data = pathlingContext.read()
        .ndjson(TEST_DATA_PATH.resolve("ndjson").toString());

    // Write the data back out to tables.
    data.write().tables("overwrite", "test");

    // Read the data back in.
    final QueryableDataSource newData = pathlingContext.read().tables("test");

    // Query the data.
    queryNdjsonData(newData);
  }

  @Test
  void readNonExistentResource() {
    final QueryableDataSource data = pathlingContext.read().datasets();
    assertThrows(IllegalArgumentException.class, () -> data.read("Patient"));
  }

  @Test
  void readInvalidUri() {
    final DataSourceBuilder builder = pathlingContext.read();
    final RuntimeException exception = assertThrows(RuntimeException.class,
        () -> builder.ndjson("file:\\\\non-existent"));
    assertInstanceOf(URISyntaxException.class, exception.getCause());
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

}
