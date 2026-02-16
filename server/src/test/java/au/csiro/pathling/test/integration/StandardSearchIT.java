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
 * Integration tests for standard FHIR search parameter support. Exercises the full HTTP stack from
 * URL query parameters through to Bundle responses.
 *
 * @author John Grimes
 */
@Slf4j
@Tag("IntegrationTest")
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@ActiveProfiles({"integration-test"})
class StandardSearchIT {

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
  void standardSearchByGenderReturnsFilteredResults() {
    // Reinitialise test data after cleanup.
    TestDataSetup.copyTestDataToTempDir(warehouseDir);

    // Search for male patients using the standard gender search parameter.
    final String searchUri = "http://localhost:" + port + "/fhir/Patient?gender=male";

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
        // Verify that results were returned and total is positive.
        .jsonPath("$.total")
        .isNotEmpty();

    log.info("Standard search by gender returned results successfully");
  }

  @Test
  void fhirPathSearchContinuesToWork() {
    // Reinitialise test data after cleanup.
    TestDataSetup.copyTestDataToTempDir(warehouseDir);

    // FHIRPath-based search should continue to work via _query=fhirPath.
    webTestClient
        .get()
        .uri(
            uriBuilder ->
                uriBuilder
                    .scheme("http")
                    .host("localhost")
                    .port(port)
                    .path("/fhir/Patient")
                    .queryParam("_query", "fhirPath")
                    .queryParam("filter", "gender = 'male'")
                    .build())
        .header("Accept", "application/fhir+json")
        .exchange()
        .expectStatus()
        .isOk()
        .expectBody()
        .jsonPath("$.resourceType")
        .isEqualTo("Bundle")
        .jsonPath("$.total")
        .isNotEmpty();

    log.info("FHIRPath search continues to work");
  }

  @Test
  void unknownSearchParameterReturns400() {
    // Reinitialise test data after cleanup.
    TestDataSetup.copyTestDataToTempDir(warehouseDir);

    // An unknown search parameter should result in a 400 Bad Request.
    final String searchUri = "http://localhost:" + port + "/fhir/Patient?nonexistent-param=value";

    webTestClient
        .get()
        .uri(searchUri)
        .header("Accept", "application/fhir+json")
        .exchange()
        .expectStatus()
        .isBadRequest()
        .expectBody()
        .jsonPath("$.resourceType")
        .isEqualTo("OperationOutcome");

    log.info("Unknown search parameter correctly returned 400 Bad Request");
  }

  @Test
  void searchWithNoParametersReturnsAllResources() {
    // Reinitialise test data after cleanup.
    TestDataSetup.copyTestDataToTempDir(warehouseDir);

    // A search with no parameters should return all resources.
    final String searchUri = "http://localhost:" + port + "/fhir/Patient";

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
        .isNotEmpty();

    log.info("Search with no parameters returned all resources");
  }

  @Test
  void combinedStandardAndFhirPathSearchWorks() {
    // Reinitialise test data after cleanup.
    TestDataSetup.copyTestDataToTempDir(warehouseDir);

    // Combine standard search parameters with FHIRPath filters.
    webTestClient
        .get()
        .uri(
            uriBuilder ->
                uriBuilder
                    .scheme("http")
                    .host("localhost")
                    .port(port)
                    .path("/fhir/Patient")
                    .queryParam("_query", "fhirPath")
                    .queryParam("filter", "active = true")
                    .queryParam("gender", "male")
                    .build())
        .header("Accept", "application/fhir+json")
        .exchange()
        .expectStatus()
        .isOk()
        .expectBody()
        .jsonPath("$.resourceType")
        .isEqualTo("Bundle")
        .jsonPath("$.total")
        .isNotEmpty();

    log.info("Combined standard and FHIRPath search returned results");
  }

  @Test
  void capabilityStatementDeclaresSearchParameters() {
    // Reinitialise test data after cleanup.
    TestDataSetup.copyTestDataToTempDir(warehouseDir);

    // The CapabilityStatement should declare standard search parameters.
    final String metadataUri = "http://localhost:" + port + "/fhir/metadata";

    webTestClient
        .get()
        .uri(metadataUri)
        .header("Accept", "application/fhir+json")
        .exchange()
        .expectStatus()
        .isOk()
        .expectBody()
        .jsonPath("$.resourceType")
        .isEqualTo("CapabilityStatement")
        // Verify that the Patient resource has search parameters declared.
        .jsonPath("$.rest[0].resource[?(@.type=='Patient')].searchParam[?(@.name=='gender')]")
        .isNotEmpty()
        .jsonPath("$.rest[0].resource[?(@.type=='Patient')].searchParam[?(@.name=='filter')]")
        .isNotEmpty();

    log.info("CapabilityStatement declares search parameters");
  }
}
