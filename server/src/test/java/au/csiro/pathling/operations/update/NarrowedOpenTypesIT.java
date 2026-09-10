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

package au.csiro.pathling.operations.update;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import au.csiro.pathling.encoders.FhirEncoders;
import au.csiro.pathling.io.SchemaDrift;
import au.csiro.pathling.util.TestDataSetup;
import jakarta.annotation.Nonnull;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.concurrent.TimeUnit;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructType;
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
 * End-to-end test for user story 3 and quickstart scenario 3: a server whose {@code
 * pathling.encoding.openTypes} has been narrowed against a populated warehouse can still write to
 * and read from the affected resource types, with no configuration change.
 *
 * <p>The prebuilt Delta test data is written with the standard open types. This test starts the
 * server with {@code Address} and {@code Identifier} removed from that set, so the Patient table
 * carries the {@code valueAddress} and {@code valueIdentifier} fields inside its extension element
 * while the running encoder does not emit them. The difference is purely narrowing, which is the
 * state reported in issue #2697. {@code pathling.storage.schemaAutoMerge} is left disabled, so
 * nothing here depends on the migratable direction being permitted.
 *
 * <p>Reading such a table back is only sound because of the core-side decode fix in {@code
 * 048-name-based-nested-decode}, which resolves nested fields by name rather than by position. A
 * server resolving {@code pathling.version} to a core build without it would decode these rows at
 * the wrong offsets.
 *
 * @author John Grimes
 */
@Slf4j
@Tag("IntegrationTest")
@SpringBootTest(
    webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT,
    properties = {
      // The standard open types, less Address and Identifier. The warehouse on disk carries the
      // full
      // standard set, so the stored extension element is wider than this encoder's.
      "pathling.encoding.openTypes=boolean,code,date,dateTime,decimal,integer,string,Coding,"
          + "CodeableConcept,Reference",
      "pathling.encoding.maxNestingLevel=3",
      "pathling.encoding.enableExtensions=true",
      // Left at the shipped default, so the tolerance cannot be mistaken for the widening policy.
      "pathling.storage.schemaAutoMerge=false",
      // Bind the Spark driver to localhost so that executor class fetches do not depend on the
      // machine's LAN address being reachable (e.g. under a firewall or VPN).
      "spark.driver.bindAddress=localhost",
      "spark.driver.host=localhost"
    })
@ActiveProfiles({"integration-test"})
@TestMethodOrder(OrderAnnotation.class)
class NarrowedOpenTypesIT {

  /** The extension value fields the stored table carries and the running encoder does not emit. */
  private static final String[] STORED_ONLY_FIELDS = {
    "_extension.valueAddress", "_extension.valueIdentifier"
  };

  /** A Patient id that exists in the prebuilt test data. */
  private static final String EXISTING_PATIENT_ID = "72df0f76-2758-fac4-67cd-de33c4a2c95e";

  private static final String NEW_PATIENT_ID = "narrowed-open-types-write";

  private static final String IMPORTED_PATIENT_ID = "narrowed-open-types-import";

  @LocalServerPort int port;

  @Autowired WebTestClient webTestClient;

  @Autowired SparkSession spark;

  @Autowired FhirEncoders fhirEncoders;

  @TempDir private static Path warehouseDir;

  @TempDir private static Path ndjsonDir;

  @DynamicPropertySource
  static void configureProperties(final DynamicPropertyRegistry registry) throws IOException {
    TestDataSetup.copyTestDataToTempDir(warehouseDir);
    Files.writeString(
        ndjsonDir.resolve("Patient.ndjson"),
        """
        {"resourceType":"Patient","id":"%s","active":true,\
        "extension":[{"url":"http://example.org/string","valueString":"imported"}]}
        """
            .formatted(IMPORTED_PATIENT_ID),
        StandardCharsets.UTF_8);
    registry.add("pathling.storage.warehouseUrl", () -> "file://" + warehouseDir.toAbsolutePath());
    registry.add(
        "pathling.import.allowableSources", () -> "file://" + ndjsonDir.toAbsolutePath() + "/");
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

  /**
   * Confirms the premise of every test below: the stored table really is wider than the running
   * encoder, and only wider. If this fails the configuration above no longer describes the #2697
   * state, and the tests that follow would pass without exercising the tolerance at all.
   */
  @Test
  @Order(1)
  void theStoredTableIsWiderThanTheEncoderAndOnlyWider() {
    final StructType tableSchema = patientTableSchema();
    final StructType encoderSchema = encoderPatientSchema();

    assertThat(SchemaDrift.excessFieldPaths(encoderSchema, tableSchema))
        .contains(STORED_ONLY_FIELDS);
    assertThat(SchemaDrift.missingFieldPaths(encoderSchema, tableSchema)).isEmpty();
  }

  /**
   * US3 scenarios 1 and 3: a PUT under a new id succeeds, and a read returns the resource as sent.
   * This is the reproduction that fails today with {@code 500 Unexpected error occurred} (SC-005).
   */
  @Test
  @Order(2)
  void putUnderNewIdSucceedsAndReadsBackAsSent() {
    webTestClient
        .put()
        .uri("http://localhost:" + port + "/fhir/Patient/" + NEW_PATIENT_ID)
        .header("Content-Type", "application/fhir+json")
        .header("Accept", "application/fhir+json")
        .bodyValue(patientJson(NEW_PATIENT_ID, "kept"))
        .exchange()
        .expectStatus()
        .isOk();

    webTestClient
        .get()
        .uri("http://localhost:" + port + "/fhir/Patient/" + NEW_PATIENT_ID)
        .header("Accept", "application/fhir+json")
        .exchange()
        .expectStatus()
        .isOk()
        .expectBody()
        .jsonPath("$.id")
        .isEqualTo(NEW_PATIENT_ID)
        .jsonPath("$.active")
        .isEqualTo(true)
        .jsonPath("$.extension[0].valueString")
        .isEqualTo("kept");
  }

  /**
   * US3 scenario 2: a PUT over an existing id replaces the row rather than duplicating it, and the
   * replacement reads back.
   */
  @Test
  @Order(3)
  void putOverExistingIdReplacesTheRow() {
    webTestClient
        .put()
        .uri("http://localhost:" + port + "/fhir/Patient/" + EXISTING_PATIENT_ID)
        .header("Content-Type", "application/fhir+json")
        .header("Accept", "application/fhir+json")
        .bodyValue(patientJson(EXISTING_PATIENT_ID, "replaced"))
        .exchange()
        .expectStatus()
        .isOk();

    webTestClient
        .get()
        .uri("http://localhost:" + port + "/fhir/Patient/" + EXISTING_PATIENT_ID)
        .header("Accept", "application/fhir+json")
        .exchange()
        .expectStatus()
        .isOk()
        .expectBody()
        .jsonPath("$.extension[0].valueString")
        .isEqualTo("replaced");
  }

  /**
   * US3 scenario 4: the write leaves the table's wider schema in place, so a server later restarted
   * with the original open types still reads the columns it wrote before. Narrowing the table here
   * would have discarded data irrecoverably.
   */
  @Test
  @Order(4)
  void theTableKeepsItsWiderSchemaAfterTheWrite() {
    assertThat(SchemaDrift.excessFieldPaths(encoderPatientSchema(), patientTableSchema()))
        .contains(STORED_ONLY_FIELDS);
  }

  /**
   * US3 scenario 7 and FR-010: {@code $import} in merge mode writes through the core Delta sink
   * rather than through {@link UpdateExecutor}, so this verifies the core-side fix from {@code
   * 048-name-based-nested-decode} rather than any server change.
   */
  @Test
  @Order(5)
  void importInMergeModeSucceeds() {
    runImport("merge");

    webTestClient
        .get()
        .uri("http://localhost:" + port + "/fhir/Patient/" + IMPORTED_PATIENT_ID)
        .header("Accept", "application/fhir+json")
        .exchange()
        .expectStatus()
        .isOk()
        .expectBody()
        .jsonPath("$.extension[0].valueString")
        .isEqualTo("imported");
    // Merge mode has not narrowed the table either.
    assertThat(SchemaDrift.excessFieldPaths(encoderPatientSchema(), patientTableSchema()))
        .contains(STORED_ONLY_FIELDS);
  }

  /**
   * US3 scenario 8: {@code $import} in overwrite mode still rewrites the table at the encoder's
   * schema, as it does today, so the stored-only columns are gone afterwards. This runs last
   * because it destroys the wider schema the tests above depend on.
   */
  @Test
  @Order(6)
  void importInOverwriteModeRewritesAtTheEncoderSchema() {
    runImport("overwrite");

    assertThat(SchemaDrift.excessFieldPaths(encoderPatientSchema(), patientTableSchema()))
        .isEmpty();
  }

  // ---- helpers ----

  /** Kicks off an asynchronous $import of the Patient NDJSON and waits for the job to complete. */
  private void runImport(final String saveMode) {
    final String body =
        """
        {
          "inputFormat": "application/fhir+ndjson",
          "inputSource": "https://example.org/source",
          "input": [{"type": "Patient", "url": "%s"}],
          "saveMode": "%s"
        }
        """
            .formatted(ndjsonDir.resolve("Patient.ndjson").toUri(), saveMode);

    final var result =
        webTestClient
            .post()
            .uri("http://localhost:" + port + "/fhir/$import")
            .header("Content-Type", "application/json")
            .header("Accept", "application/fhir+json")
            .header("Prefer", "respond-async")
            .bodyValue(body)
            .exchange()
            .expectStatus()
            .isAccepted()
            .expectHeader()
            .exists(HttpHeaders.CONTENT_LOCATION)
            .returnResult(String.class);

    final String contentLocation =
        result.getResponseHeaders().getFirst(HttpHeaders.CONTENT_LOCATION);
    assertThat(contentLocation).isNotNull();

    // A job that failed on a schema mismatch never reaches 200 here.
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
  }

  @Nonnull
  private StructType patientTableSchema() {
    final String tablePath =
        warehouseDir.resolve("delta").resolve("Patient.parquet").toAbsolutePath().toString();
    spark.catalog().refreshByPath(tablePath);
    return spark.read().format("delta").load(tablePath).schema();
  }

  /** The schema the running server's encoder produces for Patient. */
  @Nonnull
  private StructType encoderPatientSchema() {
    return fhirEncoders.of("Patient").schema();
  }

  @Nonnull
  private static String patientJson(final String id, final String extensionValue) {
    return """
    {"resourceType":"Patient","id":"%s","active":true,
     "extension":[{"url":"http://example.org/string","valueString":"%s"}]}
    """
        .formatted(id, extensionValue);
  }
}
