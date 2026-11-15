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

package au.csiro.pathling.io;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import au.csiro.pathling.library.io.source.LayoutDetector;
import au.csiro.pathling.library.io.source.StorageLayout;
import io.delta.tables.DeltaTable;
import jakarta.annotation.Nonnull;
import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.apache.commons.io.FileUtils;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.MethodOrderer.OrderAnnotation;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;
import org.junit.jupiter.api.io.TempDir;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.http.HttpHeaders;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.test.web.reactive.server.WebTestClient;

/**
 * End-to-end test of a server upgraded in place over a warehouse written by Pathling server 3.1.0,
 * which stored resources in the layout that preceded Parquet on FHIR.
 *
 * <p>The fixture under {@code legacy-layout/warehouse} was written by the published 3.1.0 Docker
 * image ({@code ghcr.io/aehrc/pathling:3.1.0}) through ordinary {@code PUT} requests, and the
 * Parquet files under {@code legacy-layout/parquet} are that server's Parquet {@code $export} of
 * the same Patient resources. The server under test is started over a copy of the warehouse, so
 * every test sees the warehouse after the startup migration.
 *
 * @author John Grimes
 */
@Tag("IntegrationTest")
@SpringBootTest(
    webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT,
    properties = {
      "pathling.storage.databaseName=default",
      // An old configuration that still sets the retired drift setting must keep working.
      "pathling.storage.schemaAutoMerge=true",
      "spark.driver.bindAddress=localhost",
      "spark.driver.host=localhost"
    })
@ActiveProfiles({"integration-test"})
@TestMethodOrder(OrderAnnotation.class)
class LegacyWarehouseMigrationIT {

  private static final Path FIXTURE = Path.of("src/test/resources/legacy-layout");

  private static final List<String> TABLES =
      List.of("Patient", "Observation", "Condition", "ViewDefinition");

  @TempDir private static Path warehouseDir;

  @LocalServerPort int port;

  @Autowired WebTestClient webTestClient;

  @Autowired SparkSession spark;

  @DynamicPropertySource
  static void configureProperties(final DynamicPropertyRegistry registry) throws IOException {
    FileUtils.copyDirectory(FIXTURE.resolve("warehouse").toFile(), warehouseDir.toFile());
    registry.add("pathling.storage.warehouseUrl", () -> "file://" + warehouseDir.toAbsolutePath());
    registry.add(
        "pathling.import.allowableSources",
        () -> "file://" + FIXTURE.resolve("parquet").toAbsolutePath() + "/");
  }

  @BeforeEach
  void setUp() {
    webTestClient =
        webTestClient
            .mutate()
            .codecs(configurer -> configurer.defaultCodecs().maxInMemorySize(100 * 1024 * 1024))
            .responseTimeout(Duration.ofSeconds(120))
            .build();
  }

  // Every table of the earlier layout has been converted at startup, and the version of each
  // table that holds the data as 3.1.0 wrote it is still in the table's history.
  @Test
  @Order(1)
  void everyTableIsConvertedAndItsEarlierVersionKept() {
    for (final String type : TABLES) {
      final String path = tablePath(type);
      assertThat(LayoutDetector.detect(spark.read().format("delta").load(path).schema()).getLayout())
          .as(type)
          .isEqualTo(StorageLayout.PARQUET_ON_FHIR);
      final long latest =
          DeltaTable.forPath(spark, path).history(1).select("version").first().getLong(0);
      assertThat(
              LegacyLayoutConversion.isLegacy(
                  spark
                      .read()
                      .format("delta")
                      .option("versionAsOf", latest - 1)
                      .load(path)
                      .schema()))
          .as(type)
          .isTrue();
    }
  }

  // The resources written by 3.1.0 are read back with their content.
  @Test
  @Order(2)
  void resourcesWrittenByTheEarlierVersionAreReadable() {
    get("/fhir/Patient/p1")
        .jsonPath("$.gender")
        .isEqualTo("female")
        .jsonPath("$.birthDate")
        .isEqualTo("1970-01-02")
        .jsonPath("$.name[0].family")
        .isEqualTo("Smith")
        .jsonPath("$.maritalStatus.coding[0].code")
        .isEqualTo("M");
    get("/fhir/Observation/o1")
        .jsonPath("$.valueQuantity.value")
        .isEqualTo(5.25)
        .jsonPath("$.subject.reference")
        .isEqualTo("Patient/p1");
    get("/fhir/ViewDefinition/v1")
        .jsonPath("$.name")
        .isEqualTo("patient_demographics")
        .jsonPath("$.select[0].column[1].path")
        .isEqualTo("gender");
  }

  // Search over the converted tables works, for standard and FHIRPath search.
  @Test
  @Order(3)
  void convertedTablesCanBeSearched() {
    get("/fhir/Patient?gender=female").jsonPath("$.total").isEqualTo(1);
    get("/fhir/Patient?_query=fhirPath&filter=name.family = 'Jones'")
        .jsonPath("$.entry[0].resource.id")
        .isEqualTo("p2");
    get("/fhir/Observation?subject=Patient/p2").jsonPath("$.total").isEqualTo(1);
  }

  // An update that introduces an element the converted table has not stored widens the table.
  @Test
  @Order(4)
  void convertedTablesAcceptUpdates() {
    webTestClient
        .put()
        .uri(url("/fhir/Patient/p2"))
        .header("Content-Type", "application/fhir+json")
        .bodyValue(
            """
            {"resourceType": "Patient", "id": "p2", "gender": "male",
             "deceasedBoolean": true, "name": [{"family": "Jones", "given": ["Bob"]}]}
            """)
        .exchange()
        .expectStatus()
        .isOk();

    get("/fhir/Patient/p2").jsonPath("$.deceasedBoolean").isEqualTo(true);
  }

  // Parquet exported by 3.1.0 is converted as it is imported.
  @Test
  @Order(5)
  void parquetExportedByTheEarlierVersionCanBeImported() {
    final String inputs =
        String.join(
            ",",
            Arrays.stream(requireFiles(FIXTURE.resolve("parquet").toFile()))
                .map(
                    file ->
                        """
                        {"type": "Patient", "url": "file://%s"}
                        """
                            .formatted(file.getAbsolutePath()))
                .toList());
    final String contentLocation =
        webTestClient
            .post()
            .uri(url("/fhir/$import"))
            .header("Content-Type", "application/json")
            .header("Accept", "application/fhir+json")
            .header("Prefer", "respond-async")
            .bodyValue(
                """
                {"inputFormat": "application/vnd.apache.parquet",
                 "inputSource": "https://example.org/source",
                 "input": [%s], "saveMode": "overwrite"}
                """
                    .formatted(inputs))
            .exchange()
            .expectStatus()
            .isAccepted()
            .returnResult(String.class)
            .getResponseHeaders()
            .getFirst(HttpHeaders.CONTENT_LOCATION);
    assertThat(contentLocation).isNotNull();

    await()
        .atMost(120, TimeUnit.SECONDS)
        .pollInterval(2, TimeUnit.SECONDS)
        .untilAsserted(
            () ->
                webTestClient
                    .get()
                    .uri(contentLocation)
                    .header("Accept", "application/fhir+json")
                    .exchange()
                    .expectStatus()
                    .isOk());

    assertThat(
            LayoutDetector.detect(spark.read().format("delta").load(tablePath("Patient")).schema())
                .getLayout())
        .isEqualTo(StorageLayout.PARQUET_ON_FHIR);
    // The overwrite replaced the updated p2 with the exported one, which is not deceased.
    get("/fhir/Patient?_count=10").jsonPath("$.total").isEqualTo(2);
    get("/fhir/Patient/p1").jsonPath("$.name[0].given[0]").isEqualTo("Jane");
  }

  @Nonnull
  private WebTestClient.BodyContentSpec get(@Nonnull final String path) {
    return webTestClient
        .get()
        .uri(url(path))
        .header("Accept", "application/fhir+json")
        .exchange()
        .expectStatus()
        .isOk()
        .expectBody();
  }

  @Nonnull
  private String url(@Nonnull final String path) {
    return "http://localhost:" + port + path;
  }

  @Nonnull
  private static String tablePath(@Nonnull final String type) {
    return warehouseDir.resolve("default").resolve(type + ".parquet").toString();
  }

  @Nonnull
  private static File[] requireFiles(@Nonnull final File directory) {
    final File[] files = directory.listFiles(file -> file.getName().endsWith(".parquet"));
    assertThat(files).isNotEmpty();
    return files;
  }
}
