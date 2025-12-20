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

package au.csiro.pathling.operations.create;

import static org.assertj.core.api.Assertions.assertThat;

import au.csiro.pathling.util.TestDataSetup;
import java.io.IOException;
import java.nio.file.Path;
import java.util.UUID;
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
 * Integration test for the FHIR create operation.
 *
 * @author John Grimes
 */
@Slf4j
@Tag("IntegrationTest")
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@ActiveProfiles({"integration-test"})
class CreateOperationIT {

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
  void testCreatePatientReturns201() {
    TestDataSetup.copyTestDataToTempDir(warehouseDir);

    final String uri = "http://localhost:" + port + "/fhir/Patient";
    final String requestBody = """
        {
          "resourceType": "Patient",
          "name": [
            {
              "family": "NewFamily",
              "given": ["NewGiven"]
            }
          ],
          "gender": "male"
        }
        """;

    webTestClient.post()
        .uri(uri)
        .header("Content-Type", "application/fhir+json")
        .header("Accept", "application/fhir+json")
        .bodyValue(requestBody)
        .exchange()
        .expectStatus().isCreated()
        .expectBody()
        .jsonPath("$.resourceType").isEqualTo("Patient")
        .jsonPath("$.id").value(id -> {
          assertThat(isValidUuid(id.toString())).isTrue();
        })
        .jsonPath("$.name[0].family").isEqualTo("NewFamily");

    log.info("Create operation completed successfully with 201 status");
  }

  @Test
  void testCreateGeneratesNewUuidIgnoringClientProvidedId() {
    TestDataSetup.copyTestDataToTempDir(warehouseDir);

    final String clientProvidedId = "client-provided-id-123";
    final String uri = "http://localhost:" + port + "/fhir/Patient";
    final String requestBody = String.format("""
        {
          "resourceType": "Patient",
          "id": "%s",
          "name": [
            {
              "family": "TestFamily"
            }
          ]
        }
        """, clientProvidedId);

    webTestClient.post()
        .uri(uri)
        .header("Content-Type", "application/fhir+json")
        .header("Accept", "application/fhir+json")
        .bodyValue(requestBody)
        .exchange()
        .expectStatus().isCreated()
        .expectBody()
        .jsonPath("$.id").value(id -> {
          // The server should generate a new UUID, not use the client-provided ID.
          assertThat(id.toString()).isNotEqualTo(clientProvidedId);
          assertThat(isValidUuid(id.toString())).isTrue();
        });

    log.info("Create correctly ignored client-provided ID and generated new UUID");
  }

  @Test
  void testCreateReturnsLocationHeader() {
    TestDataSetup.copyTestDataToTempDir(warehouseDir);

    final String uri = "http://localhost:" + port + "/fhir/Patient";
    final String requestBody = """
        {
          "resourceType": "Patient",
          "name": [
            {
              "family": "LocationTest"
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
        .expectStatus().isCreated()
        .expectHeader().exists("Location")
        .expectHeader().value("Location", location -> {
          assertThat(location).contains("/fhir/Patient/");
        });

    log.info("Create operation returned Location header");
  }

  @Test
  void testCreatedResourceCanBeRead() {
    TestDataSetup.copyTestDataToTempDir(warehouseDir);

    final String createUri = "http://localhost:" + port + "/fhir/Patient";
    final String requestBody = """
        {
          "resourceType": "Patient",
          "name": [
            {
              "family": "Readable",
              "given": ["Test"]
            }
          ],
          "gender": "female"
        }
        """;

    // Create the resource and extract the generated ID.
    final String createdId = webTestClient.post()
        .uri(createUri)
        .header("Content-Type", "application/fhir+json")
        .header("Accept", "application/fhir+json")
        .bodyValue(requestBody)
        .exchange()
        .expectStatus().isCreated()
        .returnResult(String.class)
        .getResponseBody()
        .blockFirst();

    // Extract the ID from the response body.
    assertThat(createdId).contains("\"id\"");
    final String generatedId = extractIdFromJson(createdId);
    assertThat(isValidUuid(generatedId)).isTrue();

    // Read the resource back to verify it was persisted.
    final String readUri = "http://localhost:" + port + "/fhir/Patient/" + generatedId;
    webTestClient.get()
        .uri(readUri)
        .header("Accept", "application/fhir+json")
        .exchange()
        .expectStatus().isOk()
        .expectBody()
        .jsonPath("$.id").isEqualTo(generatedId)
        .jsonPath("$.name[0].family").isEqualTo("Readable")
        .jsonPath("$.gender").isEqualTo("female");

    log.info("Created resource can be read back successfully");
  }

  @Test
  void testCreateWithMissingResourceReturnsError() {
    TestDataSetup.copyTestDataToTempDir(warehouseDir);

    final String uri = "http://localhost:" + port + "/fhir/Patient";

    webTestClient.post()
        .uri(uri)
        .header("Content-Type", "application/fhir+json")
        .header("Accept", "application/fhir+json")
        .exchange()
        .expectStatus().is4xxClientError();

    log.info("Create with missing resource correctly returned error");
  }

  @Test
  void testCreateMultipleResourcesGeneratesUniqueIds() {
    TestDataSetup.copyTestDataToTempDir(warehouseDir);

    final String uri = "http://localhost:" + port + "/fhir/Patient";

    // Create first resource.
    final String firstId = createAndGetId(uri, "First");

    // Create second resource.
    final String secondId = createAndGetId(uri, "Second");

    // Verify both have valid UUIDs and are different.
    assertThat(isValidUuid(firstId)).isTrue();
    assertThat(isValidUuid(secondId)).isTrue();
    assertThat(firstId).isNotEqualTo(secondId);

    log.info("Multiple creates generated unique IDs");
  }

  private String createAndGetId(final String uri, final String familyName) {
    final String requestBody = String.format("""
        {
          "resourceType": "Patient",
          "name": [
            {
              "family": "%s"
            }
          ]
        }
        """, familyName);

    final String response = webTestClient.post()
        .uri(uri)
        .header("Content-Type", "application/fhir+json")
        .header("Accept", "application/fhir+json")
        .bodyValue(requestBody)
        .exchange()
        .expectStatus().isCreated()
        .returnResult(String.class)
        .getResponseBody()
        .blockFirst();

    return extractIdFromJson(response);
  }

  private boolean isValidUuid(final String str) {
    try {
      UUID.fromString(str);
      return true;
    } catch (final IllegalArgumentException e) {
      return false;
    }
  }

  private String extractIdFromJson(final String json) {
    // Simple extraction of ID from JSON response.
    final int idStart = json.indexOf("\"id\"") + 6;
    final int idEnd = json.indexOf("\"", idStart);
    return json.substring(idStart, idEnd);
  }

}
