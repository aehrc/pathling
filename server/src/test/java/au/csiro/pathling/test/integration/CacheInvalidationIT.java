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

import static org.assertj.core.api.Assertions.assertThat;

import au.csiro.pathling.util.TestDataSetup;
import java.io.IOException;
import java.nio.file.Path;
import java.util.concurrent.atomic.AtomicReference;
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
 * Integration tests that verify ETag cache invalidation after resource modifications (create,
 * update, delete). Each test revalidates a search with the ETag it held before the write, straight
 * after the write returns, as a client refreshing its view of the data would.
 *
 * @author John Grimes
 */
@Slf4j
@Tag("IntegrationTest")
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@ActiveProfiles({"integration-test"})
class CacheInvalidationIT {

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
  void etagChangesAfterUpdate() {
    TestDataSetup.copyTestDataToTempDir(warehouseDir);

    // Step 1: Make a search request and capture the ETag.
    final String searchUri = "http://localhost:" + port + "/fhir/Patient";
    final AtomicReference<String> originalEtag = new AtomicReference<>();

    webTestClient
        .get()
        .uri(searchUri)
        .header("Accept", "application/fhir+json")
        .exchange()
        .expectStatus()
        .isOk()
        .expectHeader()
        .exists("ETag")
        .expectHeader()
        .value(
            "ETag",
            etag -> {
              log.info("Original ETag: {}", etag);
              originalEtag.set(etag);
            });

    assertThat(originalEtag.get()).isNotNull();

    // Step 2: Update a Patient resource.
    final String resourceId = "121503c8-9564-4b48-9086-a22df717948e";
    final String updateUri = "http://localhost:" + port + "/fhir/Patient/" + resourceId;
    final String patientJson =
        """
        {
          "resourceType": "Patient",
          "id": "%s",
          "name": [
            {
              "family": "UpdatedFamily",
              "given": ["UpdatedGiven"]
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
        .bodyValue(patientJson)
        .exchange()
        .expectStatus()
        .isOk();

    log.info("Updated Patient resource");

    // Step 3: Revalidate the search with the original ETag and verify that it is now stale.
    assertSearchRevalidatesAsModified(searchUri, originalEtag.get());
  }

  @Test
  void etagChangesAfterCreate() {
    TestDataSetup.copyTestDataToTempDir(warehouseDir);

    // Step 1: Make a search request and capture the ETag.
    final String searchUri = "http://localhost:" + port + "/fhir/Patient";
    final AtomicReference<String> originalEtag = new AtomicReference<>();

    webTestClient
        .get()
        .uri(searchUri)
        .header("Accept", "application/fhir+json")
        .exchange()
        .expectStatus()
        .isOk()
        .expectHeader()
        .exists("ETag")
        .expectHeader()
        .value(
            "ETag",
            etag -> {
              log.info("Original ETag: {}", etag);
              originalEtag.set(etag);
            });

    assertThat(originalEtag.get()).isNotNull();

    // Step 2: Create a new Patient resource.
    final String createUri = "http://localhost:" + port + "/fhir/Patient";
    final String patientJson =
        """
        {
          "resourceType": "Patient",
          "name": [
            {
              "family": "NewPatient",
              "given": ["Test"]
            }
          ]
        }
        """;

    webTestClient
        .post()
        .uri(createUri)
        .header("Content-Type", "application/fhir+json")
        .header("Accept", "application/fhir+json")
        .bodyValue(patientJson)
        .exchange()
        .expectStatus()
        .isCreated();

    log.info("Created new Patient resource");

    // Step 3: Revalidate the search with the original ETag and verify that it is now stale.
    assertSearchRevalidatesAsModified(searchUri, originalEtag.get());
  }

  @Test
  void etagChangesAfterDelete() {
    TestDataSetup.copyTestDataToTempDir(warehouseDir);

    // Step 1: Create a patient to delete.
    final String baseUri = "http://localhost:" + port + "/fhir/Patient";
    final String createBody =
        """
        {
          "resourceType": "Patient",
          "name": [{"family": "ToDelete"}]
        }
        """;

    final AtomicReference<String> resourceId = new AtomicReference<>();
    webTestClient
        .post()
        .uri(baseUri)
        .header("Content-Type", "application/fhir+json")
        .header("Accept", "application/fhir+json")
        .bodyValue(createBody)
        .exchange()
        .expectStatus()
        .isCreated()
        .expectHeader()
        .exists("Location")
        .expectHeader()
        .value(
            "Location",
            location -> {
              final String id = location.substring(location.lastIndexOf("/") + 1);
              log.info("Created patient with ID: {}", id);
              resourceId.set(id);
            });

    assertThat(resourceId.get()).isNotNull();

    // Step 2: Make a search request and capture the ETag.
    final AtomicReference<String> originalEtag = new AtomicReference<>();

    webTestClient
        .get()
        .uri(baseUri)
        .header("Accept", "application/fhir+json")
        .exchange()
        .expectStatus()
        .isOk()
        .expectHeader()
        .exists("ETag")
        .expectHeader()
        .value(
            "ETag",
            etag -> {
              log.info("Original ETag: {}", etag);
              originalEtag.set(etag);
            });

    assertThat(originalEtag.get()).isNotNull();

    // Step 3: Delete the Patient resource.
    final String deleteUri = baseUri + "/" + resourceId.get();

    webTestClient
        .delete()
        .uri(deleteUri)
        .header("Accept", "application/fhir+json")
        .exchange()
        .expectStatus()
        .isNoContent();

    log.info("Deleted Patient resource");

    // Step 4: Revalidate the search with the original ETag and verify that it is now stale.
    assertSearchRevalidatesAsModified(baseUri, originalEtag.get());
  }

  /**
   * Revalidates a search with an ETag obtained before a write, and asserts that the server returns
   * the search afresh with a new ETag rather than 304 Not Modified. This is sent as soon as the
   * write returns, without polling, so it fails if the cache key is refreshed after the response to
   * the write has been sent.
   *
   * @param searchUri the URI of the search to revalidate
   * @param originalEtag the ETag returned by the search before the write
   */
  private void assertSearchRevalidatesAsModified(
      final String searchUri, final String originalEtag) {
    final AtomicReference<String> newEtag = new AtomicReference<>();
    webTestClient
        .get()
        .uri(searchUri)
        .header("Accept", "application/fhir+json")
        .header("If-None-Match", originalEtag)
        .exchange()
        .expectStatus()
        .isOk()
        .expectHeader()
        .value("ETag", newEtag::set);
    log.info("New ETag: {}", newEtag.get());
    assertThat(newEtag.get()).isNotNull().isNotEqualTo(originalEtag);
  }
}
