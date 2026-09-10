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

package au.csiro.pathling.operations;

import au.csiro.pathling.util.TestDataSetup;
import java.nio.file.Path;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
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

  private static final Path warehouseDir = TestDataSetup.getReadOnlyTestDataPath();

  @DynamicPropertySource
  static void configureProperties(final DynamicPropertyRegistry registry) {
    registry.add("pathling.storage.warehouseUrl", () -> "file://" + warehouseDir);

    // Disable specific operations for testing.
    registry.add("pathling.operations.createEnabled", () -> "false");
    registry.add("pathling.operations.deleteEnabled", () -> "false");
    registry.add("pathling.operations.importEnabled", () -> "false");
    registry.add("pathling.operations.sqlRunEnabled", () -> "false");
    registry.add("pathling.operations.sqlExportEnabled", () -> "false");
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

  @Test
  void createReturnsClientErrorWhenDisabled() {

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
  void sqlRunReturnsClientErrorWhenDisabled() {
    final String requestBody =
        """
        {
          "resourceType": "Parameters",
          "parameter": [
            {
              "name": "subjectReference",
              "valueReference": {"reference": "ViewDefinition/anything"}
            }
          ]
        }
        """;

    // When sql-run is disabled, HAPI returns a client error (operation not found).
    postExpectingClientError("/fhir/$sql-run", requestBody);

    log.info("The $sql-run operation correctly returned a client error when disabled");
  }

  @Test
  void sqlExportReturnsClientErrorWhenDisabled() {
    postExpectingClientError(
        "/fhir/$sql-export",
        """
        {"resourceType": "Parameters", "parameter": []}
        """);

    log.info("The $sql-export operation correctly returned a client error when disabled");
  }

  // The four operations these two replaced are gone outright, so they fail as unknown operations
  // whatever the configuration says.
  @Test
  void theReplacedOperationsAreUnknown() {
    final String body =
        """
        {"resourceType": "Parameters", "parameter": []}
        """;
    for (final String path :
        java.util.List.of(
            "/fhir/$viewdefinition-run",
            "/fhir/$viewdefinition-export",
            "/fhir/Library/$sqlquery-run",
            "/fhir/$sqlquery-export")) {
      postExpectingClientError(path, body);
    }

    webTestClient
        .get()
        .uri("http://localhost:" + port + "/fhir/ViewDefinition/anything/$run")
        .header("Accept", "application/fhir+json")
        .exchange()
        .expectStatus()
        .is4xxClientError();
  }

  /** Posts a Parameters body to a path and asserts a client error. */
  private void postExpectingClientError(final String path, final String body) {
    webTestClient
        .post()
        .uri("http://localhost:" + port + path)
        .header("Content-Type", "application/fhir+json")
        .header("Accept", "application/fhir+json")
        .bodyValue(body)
        .exchange()
        .expectStatus()
        .is4xxClientError();
  }

  @Test
  void enabledOperationsStillWork() {
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
        // Verify neither SQL on FHIR data operation is in system-level operations.
        .jsonPath("$.rest[0].operation[?(@.name=='sql-run')]")
        .doesNotExist()
        .jsonPath("$.rest[0].operation[?(@.name=='sql-export')]")
        .doesNotExist();

    log.info("CapabilityStatement correctly excludes disabled operations");
  }
}
