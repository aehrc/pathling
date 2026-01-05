/*
 * Copyright 2025 Commonwealth Scientific and Industrial Research
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

package au.csiro.pathling.operations.delete;

import au.csiro.pathling.util.TestDataSetup;
import java.io.IOException;
import java.nio.file.Path;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.io.FileUtils;
import org.junit.jupiter.api.AfterEach;
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
 * Integration tests for the FHIR delete operation.
 *
 * @author John Grimes
 */
@Slf4j
@Tag("IntegrationTest")
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@ActiveProfiles({"integration-test"})
class DeleteOperationIT {

  @LocalServerPort int port;

  @Autowired WebTestClient webTestClient;

  @TempDir private static Path warehouseDir;

  @DynamicPropertySource
  static void configureProperties(final DynamicPropertyRegistry registry) {
    TestDataSetup.copyTestDataToTempDir(warehouseDir);
    registry.add("pathling.storage.warehouseUrl", () -> "file://" + warehouseDir.toAbsolutePath());
  }

  @BeforeEach
  void setup() {
    webTestClient =
        webTestClient
            .mutate()
            .codecs(configurer -> configurer.defaultCodecs().maxInMemorySize(100 * 1024 * 1024))
            .responseTimeout(java.time.Duration.ofSeconds(60))
            .build();
  }

  @AfterEach
  void cleanup() throws IOException {
    FileUtils.cleanDirectory(warehouseDir.toFile());
  }

  // -------------------------------------------------------------------------
  // Test successful delete operations
  // -------------------------------------------------------------------------

  @Test
  void deleteReturns204NoContent() {
    TestDataSetup.copyTestDataToTempDir(warehouseDir);

    // First, create a patient to delete.
    final String createUri = "http://localhost:" + port + "/fhir/Patient";
    final String createBody =
        """
        {
          "resourceType": "Patient",
          "name": [{"family": "ToDelete"}]
        }
        """;

    // Create the patient and get the ID.
    final String location =
        webTestClient
            .post()
            .uri(createUri)
            .header("Content-Type", "application/fhir+json")
            .header("Accept", "application/fhir+json")
            .bodyValue(createBody)
            .exchange()
            .expectStatus()
            .isCreated()
            .returnResult(String.class)
            .getResponseHeaders()
            .getLocation()
            .toString();

    // Extract the patient ID from the location header.
    final String patientId = location.substring(location.lastIndexOf("/") + 1).split("\\?")[0];
    final String deleteUri = "http://localhost:" + port + "/fhir/Patient/" + patientId;

    // When: HTTP DELETE [base]/Patient/[id].
    // Then: Response is 204 No Content.
    webTestClient
        .delete()
        .uri(deleteUri)
        .header("Accept", "application/fhir+json")
        .exchange()
        .expectStatus()
        .isNoContent()
        .expectBody()
        .isEmpty();

    log.info("Delete returned 204 No Content successfully");
  }

  @Test
  void deleteNonExistentReturns404() {
    TestDataSetup.copyTestDataToTempDir(warehouseDir);

    final String uri = "http://localhost:" + port + "/fhir/Patient/non-existent-id";

    // When: HTTP DELETE [base]/Patient/nonexistent-id.
    // Then: Response is 404 Not Found.
    webTestClient
        .delete()
        .uri(uri)
        .header("Accept", "application/fhir+json")
        .exchange()
        .expectStatus()
        .isNotFound();

    log.info("Delete non-existent resource returned 404 correctly");
  }

  @Test
  void readAfterDeleteReturns404() {
    TestDataSetup.copyTestDataToTempDir(warehouseDir);

    // First, create a patient to delete.
    final String createUri = "http://localhost:" + port + "/fhir/Patient";
    final String createBody =
        """
        {
          "resourceType": "Patient",
          "name": [{"family": "ToDelete"}]
        }
        """;

    // Create the patient and get the ID.
    final String location =
        webTestClient
            .post()
            .uri(createUri)
            .header("Content-Type", "application/fhir+json")
            .header("Accept", "application/fhir+json")
            .bodyValue(createBody)
            .exchange()
            .expectStatus()
            .isCreated()
            .returnResult(String.class)
            .getResponseHeaders()
            .getLocation()
            .toString();

    // Extract the patient ID from the location header.
    final String patientId = location.substring(location.lastIndexOf("/") + 1).split("\\?")[0];
    final String patientUri = "http://localhost:" + port + "/fhir/Patient/" + patientId;

    // Delete the patient.
    webTestClient
        .delete()
        .uri(patientUri)
        .header("Accept", "application/fhir+json")
        .exchange()
        .expectStatus()
        .isNoContent();

    // When: HTTP GET [base]/Patient/[id] after deletion.
    // Then: Response is 404 Not Found.
    webTestClient
        .get()
        .uri(patientUri)
        .header("Accept", "application/fhir+json")
        .exchange()
        .expectStatus()
        .isNotFound();

    log.info("Read after delete returned 404 correctly");
  }

  // -------------------------------------------------------------------------
  // Test batch operations with DELETE
  // -------------------------------------------------------------------------

  @Test
  void batchWithDeleteEntry() {
    TestDataSetup.copyTestDataToTempDir(warehouseDir);

    // First, create a patient to delete via batch.
    final String createUri = "http://localhost:" + port + "/fhir/Patient";
    final String createBody =
        """
        {
          "resourceType": "Patient",
          "id": "batch-delete-patient",
          "name": [{"family": "ToDelete"}]
        }
        """;

    webTestClient
        .put()
        .uri(createUri + "/batch-delete-patient")
        .header("Content-Type", "application/fhir+json")
        .header("Accept", "application/fhir+json")
        .bodyValue(createBody)
        .exchange()
        .expectStatus()
        .isOk();

    // Now delete via batch.
    final String batchUri = "http://localhost:" + port + "/fhir";
    final String batchBody =
        """
        {
          "resourceType": "Bundle",
          "type": "batch",
          "entry": [
            {
              "request": {
                "method": "DELETE",
                "url": "Patient/batch-delete-patient"
              }
            }
          ]
        }
        """;

    // When: POST bundle with DELETE entry.
    // Then: Bundle response contains 204 for that entry.
    webTestClient
        .post()
        .uri(batchUri)
        .header("Content-Type", "application/fhir+json")
        .header("Accept", "application/fhir+json")
        .bodyValue(batchBody)
        .exchange()
        .expectStatus()
        .isOk()
        .expectBody()
        .jsonPath("$.resourceType")
        .isEqualTo("Bundle")
        .jsonPath("$.type")
        .isEqualTo("batch-response")
        .jsonPath("$.entry[0].response.status")
        .isEqualTo("204");

    log.info("Batch with DELETE entry completed successfully");
  }

  @Test
  void batchWithMultipleDeleteEntries() {
    TestDataSetup.copyTestDataToTempDir(warehouseDir);

    // First, create multiple patients to delete.
    final String baseUri = "http://localhost:" + port + "/fhir";

    for (int i = 1; i <= 3; i++) {
      final String createBody =
          String.format(
              """
              {
                "resourceType": "Patient",
                "id": "batch-delete-%d",
                "name": [{"family": "ToDelete%d"}]
              }
              """,
              i, i);

      webTestClient
          .put()
          .uri(baseUri + "/Patient/batch-delete-" + i)
          .header("Content-Type", "application/fhir+json")
          .header("Accept", "application/fhir+json")
          .bodyValue(createBody)
          .exchange()
          .expectStatus()
          .isOk();
    }

    // Now delete all via batch.
    final String batchBody =
        """
        {
          "resourceType": "Bundle",
          "type": "batch",
          "entry": [
            {
              "request": {
                "method": "DELETE",
                "url": "Patient/batch-delete-1"
              }
            },
            {
              "request": {
                "method": "DELETE",
                "url": "Patient/batch-delete-2"
              }
            },
            {
              "request": {
                "method": "DELETE",
                "url": "Patient/batch-delete-3"
              }
            }
          ]
        }
        """;

    // When: POST bundle with multiple DELETE entries.
    // Then: All are deleted successfully.
    webTestClient
        .post()
        .uri(baseUri)
        .header("Content-Type", "application/fhir+json")
        .header("Accept", "application/fhir+json")
        .bodyValue(batchBody)
        .exchange()
        .expectStatus()
        .isOk()
        .expectBody()
        .jsonPath("$.resourceType")
        .isEqualTo("Bundle")
        .jsonPath("$.type")
        .isEqualTo("batch-response")
        .jsonPath("$.entry.length()")
        .isEqualTo(3)
        .jsonPath("$.entry[0].response.status")
        .isEqualTo("204")
        .jsonPath("$.entry[1].response.status")
        .isEqualTo("204")
        .jsonPath("$.entry[2].response.status")
        .isEqualTo("204");

    log.info("Batch with multiple DELETE entries completed successfully");
  }

  @Test
  void batchWithMixedOperationsIncludingDelete() {
    TestDataSetup.copyTestDataToTempDir(warehouseDir);

    // First, create a patient to delete.
    final String baseUri = "http://localhost:" + port + "/fhir";
    final String createBody =
        """
        {
          "resourceType": "Patient",
          "id": "mixed-delete-patient",
          "name": [{"family": "ToDelete"}]
        }
        """;

    webTestClient
        .put()
        .uri(baseUri + "/Patient/mixed-delete-patient")
        .header("Content-Type", "application/fhir+json")
        .header("Accept", "application/fhir+json")
        .bodyValue(createBody)
        .exchange()
        .expectStatus()
        .isOk();

    // Now perform batch with mixed CREATE, UPDATE, and DELETE.
    final String batchBody =
        """
        {
          "resourceType": "Bundle",
          "type": "batch",
          "entry": [
            {
              "resource": {
                "resourceType": "Patient",
                "name": [{"family": "NewPatient"}]
              },
              "request": {
                "method": "POST",
                "url": "Patient"
              }
            },
            {
              "request": {
                "method": "DELETE",
                "url": "Patient/mixed-delete-patient"
              }
            }
          ]
        }
        """;

    webTestClient
        .post()
        .uri(baseUri)
        .header("Content-Type", "application/fhir+json")
        .header("Accept", "application/fhir+json")
        .bodyValue(batchBody)
        .exchange()
        .expectStatus()
        .isOk()
        .expectBody()
        .jsonPath("$.resourceType")
        .isEqualTo("Bundle")
        .jsonPath("$.type")
        .isEqualTo("batch-response")
        .jsonPath("$.entry.length()")
        .isEqualTo(2)
        .jsonPath("$.entry[0].response.status")
        .isEqualTo("201")
        .jsonPath("$.entry[1].response.status")
        .isEqualTo("204");

    log.info("Batch with mixed operations including DELETE completed successfully");
  }
}
