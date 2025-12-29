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

package au.csiro.pathling.operations;

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
 * Integration tests verifying that disabled operations are rejected with client error responses.
 *
 * @author John Grimes
 */
@Slf4j
@Tag("IntegrationTest")
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@ActiveProfiles({"integration-test"})
class DisabledOperationIT {

  @LocalServerPort int port;

  @Autowired WebTestClient webTestClient;

  @TempDir private static Path warehouseDir;

  @DynamicPropertySource
  static void configureProperties(final DynamicPropertyRegistry registry) {
    TestDataSetup.copyTestDataToTempDir(warehouseDir);
    registry.add("pathling.storage.warehouseUrl", () -> "file://" + warehouseDir.toAbsolutePath());

    // Disable specific operations for testing.
    registry.add("pathling.operations.createEnabled", () -> "false");
    registry.add("pathling.operations.deleteEnabled", () -> "false");
    registry.add("pathling.operations.importEnabled", () -> "false");
    registry.add("pathling.operations.viewDefinitionRunEnabled", () -> "false");
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

  @Test
  void createReturnsClientErrorWhenDisabled() {
    TestDataSetup.copyTestDataToTempDir(warehouseDir);

    final String uri = "http://localhost:" + port + "/fhir/Patient";
    final String requestBody =
        """
        {
          "resourceType": "Patient",
          "name": [
            {
              "family": "Test"
            }
          ]
        }
        """;

    // When create is disabled, HAPI returns a client error (operation not supported).
    webTestClient
        .post()
        .uri(uri)
        .header("Content-Type", "application/fhir+json")
        .header("Accept", "application/fhir+json")
        .bodyValue(requestBody)
        .exchange()
        .expectStatus()
        .is4xxClientError();

    log.info("Create operation correctly returned client error when disabled");
  }

  @Test
  void deleteReturnsClientErrorWhenDisabled() {
    TestDataSetup.copyTestDataToTempDir(warehouseDir);

    final String uri = "http://localhost:" + port + "/fhir/Patient/some-id";

    // When delete is disabled, HAPI returns a client error (operation not supported).
    webTestClient
        .delete()
        .uri(uri)
        .header("Accept", "application/fhir+json")
        .exchange()
        .expectStatus()
        .is4xxClientError();

    log.info("Delete operation correctly returned client error when disabled");
  }

  @Test
  void importReturnsClientErrorWhenDisabled() {
    TestDataSetup.copyTestDataToTempDir(warehouseDir);

    final String uri = "http://localhost:" + port + "/fhir/$import";
    final String requestBody =
        """
        {
          "resourceType": "Parameters",
          "parameter": [
            {
              "name": "source",
              "valueUri": "file:///nonexistent"
            }
          ]
        }
        """;

    // When import is disabled, HAPI returns a client error (operation not found).
    webTestClient
        .post()
        .uri(uri)
        .header("Content-Type", "application/fhir+json")
        .header("Accept", "application/fhir+json")
        .bodyValue(requestBody)
        .exchange()
        .expectStatus()
        .is4xxClientError();

    log.info("Import operation correctly returned client error when disabled");
  }

  @Test
  void viewDefinitionRunReturnsClientErrorWhenDisabled() {
    TestDataSetup.copyTestDataToTempDir(warehouseDir);

    final String uri = "http://localhost:" + port + "/fhir/$viewdefinition-run";
    final String requestBody =
        """
        {
          "resourceType": "Parameters",
          "parameter": [
            {
              "name": "viewDefinition",
              "resource": {
                "resourceType": "ViewDefinition",
                "name": "test_view",
                "resource": "Patient",
                "select": [
                  {
                    "column": [
                      {"path": "id", "name": "id"}
                    ]
                  }
                ]
              }
            }
          ]
        }
        """;

    // When viewdefinition-run is disabled, HAPI returns a client error (operation not found).
    webTestClient
        .post()
        .uri(uri)
        .header("Content-Type", "application/fhir+json")
        .header("Accept", "application/fhir+json")
        .bodyValue(requestBody)
        .exchange()
        .expectStatus()
        .is4xxClientError();

    log.info("ViewDefinition run operation correctly returned client error when disabled");
  }

  @Test
  void enabledOperationsStillWork() {
    TestDataSetup.copyTestDataToTempDir(warehouseDir);

    // Read operation should still work when create/delete are disabled.
    final String uri = "http://localhost:" + port + "/fhir/Patient";

    webTestClient
        .get()
        .uri(uri)
        .header("Accept", "application/fhir+json")
        .exchange()
        .expectStatus()
        .isOk();

    log.info("Enabled operations still work when other operations are disabled");
  }

  @Test
  void capabilityStatementExcludesDisabledOperations() {
    TestDataSetup.copyTestDataToTempDir(warehouseDir);

    final String uri = "http://localhost:" + port + "/fhir/metadata";

    webTestClient
        .get()
        .uri(uri)
        .header("Accept", "application/fhir+json")
        .exchange()
        .expectStatus()
        .isOk()
        .expectBody()
        // Verify import operation is not in system-level operations.
        .jsonPath("$.rest[0].operation[?(@.name=='import')]")
        .doesNotExist()
        // Verify viewdefinition-run operation is not in system-level operations.
        .jsonPath("$.rest[0].operation[?(@.name=='viewdefinition-run')]")
        .doesNotExist();

    log.info("CapabilityStatement correctly excludes disabled operations");
  }
}
