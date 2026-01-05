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

package au.csiro.pathling.test.integration;

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
 * Integration test that verifies the update operation can create a new resource type not present in
 * the initial dataset, and that the search operation can subsequently find it.
 *
 * @author John Grimes
 */
@Slf4j
@Tag("IntegrationTest")
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@ActiveProfiles({"integration-test"})
class UpdateAndSearchIT {

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
  void testUpdateCreatesNewResourceTypeThenSearchFindsIt() {
    TestDataSetup.copyTestDataToTempDir(warehouseDir);

    final String resourceId = "test-related-person-1";

    // Step 1: Create a new RelatedPerson via PUT (update/upsert)
    // RelatedPerson is not present in the test dataset, so this creates a new Delta table
    final String updateUri = "http://localhost:" + port + "/fhir/RelatedPerson/" + resourceId;
    final String relatedPersonJson =
        """
        {
          "resourceType": "RelatedPerson",
          "id": "%s",
          "patient": {
            "reference": "Patient/121503c8-9564-4b48-9086-a22df717948e"
          },
          "relationship": [
            {
              "coding": [
                {
                  "system": "http://terminology.hl7.org/CodeSystem/v3-RoleCode",
                  "code": "MTH",
                  "display": "mother"
                }
              ]
            }
          ],
          "name": [
            {
              "family": "TestFamily",
              "given": ["TestGiven"]
            }
          ]
        }
        """
            .formatted(resourceId);

    webTestClient
        .put()
        .uri(updateUri)
        .header("Content-Type", "application/fhir+json")
        .header("Accept", "application/fhir+json")
        .bodyValue(relatedPersonJson)
        .exchange()
        .expectStatus()
        .isOk()
        .expectBody()
        .jsonPath("$.resourceType")
        .isEqualTo("RelatedPerson")
        .jsonPath("$.id")
        .isEqualTo(resourceId);

    log.info("Created RelatedPerson via update operation");

    // Step 2: Search for RelatedPerson resources
    final String searchUri = "http://localhost:" + port + "/fhir/RelatedPerson";

    webTestClient
        .get()
        .uri(searchUri)
        .header("Accept", "application/fhir+json")
        .exchange()
        .expectStatus()
        .isOk()
        .expectBody()
        .jsonPath("$.resourceType")
        .isEqualTo("Bundle")
        .jsonPath("$.total")
        .isEqualTo(1)
        .jsonPath("$.entry[0].resource.resourceType")
        .isEqualTo("RelatedPerson")
        .jsonPath("$.entry[0].resource.id")
        .isEqualTo(resourceId)
        .jsonPath("$.entry[0].resource.name[0].family")
        .isEqualTo("TestFamily");

    log.info("Search found the created RelatedPerson");
  }
}
