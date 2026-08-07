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

import au.csiro.pathling.util.DeltaSchemaFixtures;
import au.csiro.pathling.util.TestDataSetup;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Set;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.test.web.reactive.server.WebTestClient;

/**
 * End-to-end test for the diagnostics served when a write cannot be reconciled with the stored
 * table, covering quickstart scenario 1 and the acceptance scenarios of user story 1.
 *
 * <p>The warehouse is prepared before the application context starts: the Patient table's schema is
 * downgraded on disk by removing the {@code prefix} and {@code suffix} fields from every nested
 * struct, which is where the {@code name} element's fields live. That is the difference Delta's
 * MERGE refuses to reconcile - a target struct with fewer fields than the corresponding source
 * struct - and with {@code pathling.storage.schemaAutoMerge} left at its shipped default of
 * disabled it is not migrated either, which is exactly the state that produces {@code 500
 * Unexpected error occurred} today.
 *
 * <p>Removing a top-level column instead would not do: Delta 4 resolves top-level columns by name
 * and silently tolerates extra ones in the source, which is why {@link SchemaEvolutionReadIT} can
 * drift {@code gender} without provoking a failure at all.
 *
 * @author John Grimes
 */
@Slf4j
@Tag("IntegrationTest")
@SpringBootTest(
    webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT,
    properties = {
      "pathling.storage.schemaAutoMerge=false",
      // Bind the Spark driver to localhost so that executor class fetches do not depend on the
      // machine's LAN address being reachable (e.g. under a firewall or VPN).
      "spark.driver.bindAddress=localhost",
      "spark.driver.host=localhost"
    })
@ActiveProfiles({"integration-test"})
class SchemaMismatchDiagnosticsIT {

  /** The nested fields removed from the Patient table, which live inside the name element. */
  private static final Set<String> REMOVED_FIELDS = Set.of("prefix", "suffix");

  private static final String PATIENT_ID = "schema-mismatch-diagnostics-patient";

  private static final String CONDITION_ID = "schema-mismatch-diagnostics-condition";

  @LocalServerPort int port;

  @Autowired WebTestClient webTestClient;

  @TempDir private static java.nio.file.Path warehouseDir;

  @BeforeEach
  void setUp() {
    webTestClient =
        webTestClient
            .mutate()
            .codecs(configurer -> configurer.defaultCodecs().maxInMemorySize(100 * 1024 * 1024))
            .responseTimeout(Duration.ofSeconds(120))
            .build();
  }

  @DynamicPropertySource
  static void configureProperties(final DynamicPropertyRegistry registry) throws Exception {
    TestDataSetup.copyTestDataToTempDir(warehouseDir);
    DeltaSchemaFixtures.removeFieldsFromTableSchema(
        warehouseDir.resolve("delta").resolve("Patient.parquet"), REMOVED_FIELDS);
    registry.add("pathling.storage.warehouseUrl", () -> "file://" + warehouseDir.toAbsolutePath());
  }

  /**
   * User story 1, acceptance scenario 1: a PUT against the affected type returns a 500 whose
   * OperationOutcome diagnostics name the resource type, the missing field paths and the remedy,
   * and does not read {@code Unexpected error occurred} (FR-001, SC-001).
   */
  @Test
  void putOfAffectedTypeReturnsActionableDiagnostics() {
    final String body = responseBody(putPatient(PATIENT_ID).expectStatus().isEqualTo(500));

    assertThat(body).contains("OperationOutcome");
    assertThat(diagnostics(body))
        .contains("Patient")
        .contains("prefix")
        .contains("suffix")
        .contains("schemaAutoMerge")
        .doesNotContain("Unexpected error occurred");
  }

  /**
   * FR-002 and SC-003: the diagnostics must not expose the raw Delta message, which embeds both
   * struct definitions in full, nor any warehouse path.
   */
  @Test
  void diagnosticsExposeNoStructDefinitionOrWarehousePath() {
    final String body = responseBody(putPatient(PATIENT_ID).expectStatus().isEqualTo(500));

    assertThat(diagnostics(body))
        .doesNotContain("struct<")
        .doesNotContain("Cannot cast")
        .doesNotContain("DELTA_UPDATE_SCHEMA_MISMATCH_EXPRESSION")
        .doesNotContain(warehouseDir.toAbsolutePath().toString())
        .doesNotContain(".parquet");
  }

  /**
   * User story 1, acceptance scenario 2: within a batch, the entry of the affected type carries the
   * same diagnostics while the entry of an unaffected type succeeds. A batch's entries are
   * independent, so one unwritable type must not fail the whole request.
   */
  @Test
  void batchReportsTheAffectedEntryAndSucceedsForOthers() {
    final String batch =
        """
        {
          "resourceType": "Bundle",
          "type": "batch",
          "entry": [
            {
              "request": {"method": "PUT", "url": "Patient/%s"},
              "resource": {
                "resourceType": "Patient", "id": "%s", "active": true,
                "name": [{"family": "Mismatch", "given": ["Schema"]}]
              }
            },
            {
              "request": {"method": "PUT", "url": "Condition/%s"},
              "resource": {
                "resourceType": "Condition", "id": "%s",
                "subject": {"reference": "Patient/unaffected"}
              }
            }
          ]
        }
        """
            .formatted(PATIENT_ID, PATIENT_ID, CONDITION_ID, CONDITION_ID);

    final String body =
        responseBody(
            webTestClient
                .post()
                .uri("http://localhost:" + port + "/fhir")
                .header("Content-Type", "application/fhir+json")
                .header("Accept", "application/fhir+json")
                .bodyValue(batch)
                .exchange()
                .expectStatus()
                .isOk());

    // The Patient entry carries the failure, naming the type, the paths and the remedy.
    assertThat(body).contains("\"status\":\"500\"");
    assertThat(diagnostics(body))
        .contains("Patient")
        .contains("prefix")
        .contains("schemaAutoMerge")
        .doesNotContain("Unexpected error occurred");
    // The Condition entry, whose table reconciles, still succeeds.
    assertThat(body).contains("\"status\":\"200\"");
  }

  /**
   * FR-006: a type whose table reconciles with the encoders is written exactly as it is today, so
   * the translation has not changed write semantics for anything else.
   */
  @Test
  void putOfUnaffectedTypeStillSucceeds() {
    webTestClient
        .put()
        .uri("http://localhost:" + port + "/fhir/Condition/" + CONDITION_ID)
        .header("Content-Type", "application/fhir+json")
        .header("Accept", "application/fhir+json")
        .bodyValue(
            """
            {"resourceType": "Condition", "id": "%s",
             "subject": {"reference": "Patient/unaffected"}}
            """
                .formatted(CONDITION_ID))
        .exchange()
        .expectStatus()
        .isOk();
  }

  // ---- helpers ----

  private WebTestClient.ResponseSpec putPatient(final String id) {
    return webTestClient
        .put()
        .uri("http://localhost:" + port + "/fhir/Patient/" + id)
        .header("Content-Type", "application/fhir+json")
        .header("Accept", "application/fhir+json")
        .bodyValue(
            """
            {"resourceType": "Patient", "id": "%s", "active": true,
             "name": [{"family": "Mismatch", "given": ["Schema"]}]}
            """
                .formatted(id))
        .exchange();
  }

  /**
   * Returns the response body as a string, so that assertions can be made across the whole body.
   */
  private static String responseBody(final WebTestClient.ResponseSpec response) {
    final byte[] bytes = response.expectBody().returnResult().getResponseBody();
    assertThat(bytes).isNotNull();
    return new String(bytes, StandardCharsets.UTF_8);
  }

  /**
   * Extracts the diagnostics text from a body carrying an OperationOutcome, whether at the top
   * level or nested inside a batch response entry's outcome.
   */
  private static String diagnostics(final String body) {
    final int start = body.indexOf("\"diagnostics\":\"");
    assertThat(start).as("body must carry a diagnostics field: %s", body).isNotNegative();
    final int from = start + "\"diagnostics\":\"".length();
    final int end = body.indexOf('"', from);
    return body.substring(from, end);
  }
}
