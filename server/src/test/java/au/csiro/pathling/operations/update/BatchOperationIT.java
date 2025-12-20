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

package au.csiro.pathling.operations.update;

import static org.assertj.core.api.Assertions.assertThat;

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
 * Integration test for the FHIR batch operation.
 *
 * @author John Grimes
 */
@Slf4j
@Tag("IntegrationTest")
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@ActiveProfiles({"integration-test"})
class BatchOperationIT {

  @LocalServerPort
  int port;

  @Autowired
  WebTestClient webTestClient;

  @TempDir
  private static Path warehouseDir;

  @DynamicPropertySource
  static void configureProperties(final DynamicPropertyRegistry registry) {
    TestDataSetup.copyTestDataToTempDir(warehouseDir);
    registry.add("pathling.storage.warehouseUrl", () -> "file://" + warehouseDir.toAbsolutePath());
  }

  @BeforeEach
  void setup() {
    webTestClient = webTestClient.mutate()
        .codecs(configurer -> configurer.defaultCodecs().maxInMemorySize(100 * 1024 * 1024))
        .responseTimeout(java.time.Duration.ofSeconds(60))
        .build();
  }

  @AfterEach
  void cleanup() throws IOException {
    FileUtils.cleanDirectory(warehouseDir.toFile());
  }

  @Test
  void testBatchUpdateSinglePatient() {
    TestDataSetup.copyTestDataToTempDir(warehouseDir);

    final String uri = "http://localhost:" + port + "/fhir";
    final String requestBody = """
        {
          "resourceType": "Bundle",
          "type": "batch",
          "entry": [
            {
              "fullUrl": "urn:uuid:test-patient-1",
              "resource": {
                "resourceType": "Patient",
                "id": "batch-patient-1",
                "name": [
                  {
                    "family": "BatchFamily",
                    "given": ["BatchGiven"]
                  }
                ]
              },
              "request": {
                "method": "PUT",
                "url": "Patient/batch-patient-1"
              }
            }
          ]
        }
        """;

    webTestClient.post()
        .uri(uri)
        .header("Content-Type", "application/fhir+json")
        .header("Accept", "application/fhir+json")
        .bodyValue(requestBody)
        .exchange()
        .expectStatus().isOk()
        .expectBody()
        .jsonPath("$.resourceType").isEqualTo("Bundle")
        .jsonPath("$.type").isEqualTo("batch-response")
        .jsonPath("$.entry[0].response.status").isEqualTo("200")
        .jsonPath("$.entry[0].resource.resourceType").isEqualTo("Patient")
        .jsonPath("$.entry[0].resource.id").isEqualTo("batch-patient-1");

    log.info("Batch update single patient completed successfully");
  }

  @Test
  void testBatchUpdateMultiplePatients() {
    TestDataSetup.copyTestDataToTempDir(warehouseDir);

    final String uri = "http://localhost:" + port + "/fhir";
    final String requestBody = """
        {
          "resourceType": "Bundle",
          "type": "batch",
          "entry": [
            {
              "resource": {
                "resourceType": "Patient",
                "id": "batch-patient-1",
                "name": [{"family": "Family1"}]
              },
              "request": {
                "method": "PUT",
                "url": "Patient/batch-patient-1"
              }
            },
            {
              "resource": {
                "resourceType": "Patient",
                "id": "batch-patient-2",
                "name": [{"family": "Family2"}]
              },
              "request": {
                "method": "PUT",
                "url": "Patient/batch-patient-2"
              }
            }
          ]
        }
        """;

    webTestClient.post()
        .uri(uri)
        .header("Content-Type", "application/fhir+json")
        .header("Accept", "application/fhir+json")
        .bodyValue(requestBody)
        .exchange()
        .expectStatus().isOk()
        .expectBody()
        .jsonPath("$.resourceType").isEqualTo("Bundle")
        .jsonPath("$.type").isEqualTo("batch-response")
        .jsonPath("$.entry.length()").isEqualTo(2)
        .jsonPath("$.entry[0].response.status").isEqualTo("200")
        .jsonPath("$.entry[1].response.status").isEqualTo("200");

    log.info("Batch update multiple patients completed successfully");
  }

  @Test
  void testBatchUpdateMixedResourceTypes() {
    TestDataSetup.copyTestDataToTempDir(warehouseDir);

    final String uri = "http://localhost:" + port + "/fhir";
    final String requestBody = """
        {
          "resourceType": "Bundle",
          "type": "batch",
          "entry": [
            {
              "resource": {
                "resourceType": "Patient",
                "id": "mixed-patient-1",
                "name": [{"family": "PatientFamily"}]
              },
              "request": {
                "method": "PUT",
                "url": "Patient/mixed-patient-1"
              }
            },
            {
              "resource": {
                "resourceType": "Observation",
                "id": "mixed-obs-1",
                "status": "final",
                "code": {
                  "coding": [{"system": "http://loinc.org", "code": "8867-4"}]
                }
              },
              "request": {
                "method": "PUT",
                "url": "Observation/mixed-obs-1"
              }
            }
          ]
        }
        """;

    webTestClient.post()
        .uri(uri)
        .header("Content-Type", "application/fhir+json")
        .header("Accept", "application/fhir+json")
        .bodyValue(requestBody)
        .exchange()
        .expectStatus().isOk()
        .expectBody()
        .jsonPath("$.resourceType").isEqualTo("Bundle")
        .jsonPath("$.type").isEqualTo("batch-response")
        .jsonPath("$.entry.length()").isEqualTo(2)
        .jsonPath("$.entry[0].response.status").isEqualTo("200")
        .jsonPath("$.entry[1].response.status").isEqualTo("200");

    log.info("Batch update mixed resource types completed successfully");
  }

  @Test
  void testBatchCreateGeneratesUuid() {
    TestDataSetup.copyTestDataToTempDir(warehouseDir);

    final String uri = "http://localhost:" + port + "/fhir";
    final String requestBody = """
        {
          "resourceType": "Bundle",
          "type": "batch",
          "entry": [
            {
              "resource": {
                "resourceType": "Patient",
                "name": [{"family": "Test"}]
              },
              "request": {
                "method": "POST",
                "url": "Patient"
              }
            }
          ]
        }
        """;

    webTestClient.post()
        .uri(uri)
        .header("Content-Type", "application/fhir+json")
        .header("Accept", "application/fhir+json")
        .bodyValue(requestBody)
        .exchange()
        .expectStatus().isOk()
        .expectBody()
        .jsonPath("$.resourceType").isEqualTo("Bundle")
        .jsonPath("$.type").isEqualTo("batch-response")
        .jsonPath("$.entry[0].response.status").isEqualTo("201")
        .jsonPath("$.entry[0].resource.resourceType").isEqualTo("Patient")
        .jsonPath("$.entry[0].resource.id").isNotEmpty();

    log.info("Batch create with POST correctly generated UUID");
  }

  @Test
  void testBatchWithUnsupportedMethodReturnsError() {
    TestDataSetup.copyTestDataToTempDir(warehouseDir);

    final String uri = "http://localhost:" + port + "/fhir";
    final String requestBody = """
        {
          "resourceType": "Bundle",
          "type": "batch",
          "entry": [
            {
              "resource": {
                "resourceType": "Patient",
                "id": "test-patient",
                "name": [{"family": "Test"}]
              },
              "request": {
                "method": "DELETE",
                "url": "Patient/test-patient"
              }
            }
          ]
        }
        """;

    webTestClient.post()
        .uri(uri)
        .header("Content-Type", "application/fhir+json")
        .header("Accept", "application/fhir+json")
        .bodyValue(requestBody)
        .exchange()
        .expectStatus().is4xxClientError()
        .expectBody()
        .jsonPath("$.issue[0].diagnostics").value(diagnostics ->
            assertThat(diagnostics.toString()).containsIgnoringCase("create"));

    log.info("Batch with unsupported method correctly returned error");
  }

  @Test
  void testBatchWithInvalidUrlFormatReturnsError() {
    TestDataSetup.copyTestDataToTempDir(warehouseDir);

    final String uri = "http://localhost:" + port + "/fhir";
    final String requestBody = """
        {
          "resourceType": "Bundle",
          "type": "batch",
          "entry": [
            {
              "resource": {
                "resourceType": "Patient",
                "id": "test-patient",
                "name": [{"family": "Test"}]
              },
              "request": {
                "method": "PUT",
                "url": "InvalidUrl"
              }
            }
          ]
        }
        """;

    webTestClient.post()
        .uri(uri)
        .header("Content-Type", "application/fhir+json")
        .header("Accept", "application/fhir+json")
        .bodyValue(requestBody)
        .exchange()
        .expectStatus().is4xxClientError()
        .expectBody()
        .jsonPath("$.issue[0].diagnostics").value(diagnostics ->
            assertThat(diagnostics.toString()).containsIgnoringCase("URL"));

    log.info("Batch with invalid URL format correctly returned error");
  }

  @Test
  void testBatchWithMismatchedResourceTypeReturnsError() {
    TestDataSetup.copyTestDataToTempDir(warehouseDir);

    final String uri = "http://localhost:" + port + "/fhir";
    final String requestBody = """
        {
          "resourceType": "Bundle",
          "type": "batch",
          "entry": [
            {
              "resource": {
                "resourceType": "Patient",
                "id": "test-patient",
                "name": [{"family": "Test"}]
              },
              "request": {
                "method": "PUT",
                "url": "Observation/test-patient"
              }
            }
          ]
        }
        """;

    webTestClient.post()
        .uri(uri)
        .header("Content-Type", "application/fhir+json")
        .header("Accept", "application/fhir+json")
        .bodyValue(requestBody)
        .exchange()
        .expectStatus().is4xxClientError()
        .expectBody()
        .jsonPath("$.issue[0].diagnostics").value(diagnostics ->
            assertThat(diagnostics.toString()).containsIgnoringCase("resource"));

    log.info("Batch with mismatched resource type correctly returned error");
  }

  @Test
  void testBatchWithEmptyBundleReturnsEmptyResponse() {
    TestDataSetup.copyTestDataToTempDir(warehouseDir);

    final String uri = "http://localhost:" + port + "/fhir";
    final String requestBody = """
        {
          "resourceType": "Bundle",
          "type": "batch",
          "entry": []
        }
        """;

    webTestClient.post()
        .uri(uri)
        .header("Content-Type", "application/fhir+json")
        .header("Accept", "application/fhir+json")
        .bodyValue(requestBody)
        .exchange()
        .expectStatus().isOk()
        .expectBody()
        .jsonPath("$.resourceType").isEqualTo("Bundle")
        .jsonPath("$.type").isEqualTo("batch-response")
        // Empty bundles may omit the entry array entirely in FHIR JSON.
        .jsonPath("$.entry").doesNotExist();

    log.info("Batch with empty bundle returned empty response");
  }

}
