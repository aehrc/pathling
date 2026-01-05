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
 * Integration test for the FHIR update operation.
 *
 * @author John Grimes
 */
@Slf4j
@Tag("IntegrationTest")
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@ActiveProfiles({"integration-test"})
class UpdateOperationIT {

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

  @Test
  void testUpdateExistingPatient() {
    TestDataSetup.copyTestDataToTempDir(warehouseDir);

    final String uri =
        "http://localhost:" + port + "/fhir/Patient/121503c8-9564-4b48-9086-a22df717948e";
    final String requestBody =
        """
        {
          "resourceType": "Patient",
          "id": "121503c8-9564-4b48-9086-a22df717948e",
          "name": [
            {
              "family": "UpdatedFamily",
              "given": ["UpdatedGiven"]
            }
          ]
        }
        """;

    webTestClient
        .put()
        .uri(uri)
        .header("Content-Type", "application/fhir+json")
        .header("Accept", "application/fhir+json")
        .bodyValue(requestBody)
        .exchange()
        .expectStatus()
        .isOk()
        .expectBody()
        .jsonPath("$.resourceType")
        .isEqualTo("Patient")
        .jsonPath("$.id")
        .isEqualTo("121503c8-9564-4b48-9086-a22df717948e")
        .jsonPath("$.name[0].family")
        .isEqualTo("UpdatedFamily");

    log.info("Update operation completed successfully");
  }

  @Test
  void testUpdateCreatesNewPatient() {
    TestDataSetup.copyTestDataToTempDir(warehouseDir);

    final String newPatientId = "new-patient-123";
    final String uri = "http://localhost:" + port + "/fhir/Patient/" + newPatientId;
    final String requestBody =
        String.format(
            """
            {
              "resourceType": "Patient",
              "id": "%s",
              "name": [
                {
                  "family": "NewFamily",
                  "given": ["NewGiven"]
                }
              ]
            }
            """,
            newPatientId);

    webTestClient
        .put()
        .uri(uri)
        .header("Content-Type", "application/fhir+json")
        .header("Accept", "application/fhir+json")
        .bodyValue(requestBody)
        .exchange()
        .expectStatus()
        .isOk()
        .expectBody()
        .jsonPath("$.resourceType")
        .isEqualTo("Patient")
        .jsonPath("$.id")
        .isEqualTo(newPatientId);

    log.info("Update (create) operation completed successfully");
  }

  @Test
  void testUpdateWithMismatchedIdReturnsError() {
    TestDataSetup.copyTestDataToTempDir(warehouseDir);

    final String uri = "http://localhost:" + port + "/fhir/Patient/correct-id";
    final String requestBody =
        """
        {
          "resourceType": "Patient",
          "id": "wrong-id",
          "name": [
            {
              "family": "Test"
            }
          ]
        }
        """;

    webTestClient
        .put()
        .uri(uri)
        .header("Content-Type", "application/fhir+json")
        .header("Accept", "application/fhir+json")
        .bodyValue(requestBody)
        .exchange()
        .expectStatus()
        .is4xxClientError()
        .expectBody()
        .jsonPath("$.issue[0].diagnostics")
        .value(diagnostics -> assertThat(diagnostics.toString()).contains("ID"));

    log.info("Update with mismatched ID correctly returned error");
  }

  @Test
  void testUpdateWithMissingResourceReturnsError() {
    TestDataSetup.copyTestDataToTempDir(warehouseDir);

    final String uri = "http://localhost:" + port + "/fhir/Patient/test-id";

    webTestClient
        .put()
        .uri(uri)
        .header("Content-Type", "application/fhir+json")
        .header("Accept", "application/fhir+json")
        .exchange()
        .expectStatus()
        .is4xxClientError();

    log.info("Update with missing resource correctly returned error");
  }
}
