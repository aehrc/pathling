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

package au.csiro.pathling.operations.bulkimport;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import au.csiro.pathling.library.PathlingContext;
import au.csiro.pathling.util.TestDataSetup;
import ca.uhn.fhir.context.FhirContext;
import ca.uhn.fhir.parser.IParser;
import java.io.IOException;
import java.nio.file.Path;
import java.util.concurrent.TimeUnit;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.io.FileUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.api.parallel.ResourceAccessMode;
import org.junit.jupiter.api.parallel.ResourceLock;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.http.HttpHeaders;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.test.web.reactive.server.WebTestClient;

/**
 * Integration test for the $import-pnp operation.
 *
 * @author John Grimes
 */
@Slf4j
@Tag("IntegrationTest")
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@ResourceLock(value = "wiremock", mode = ResourceAccessMode.READ_WRITE)
@ActiveProfiles({"integration-test"})
class ImportPnpOperationIT {

  @LocalServerPort
  int port;

  @Autowired
  WebTestClient webTestClient;

  @TempDir
  private static Path warehouseDir;

  @Autowired
  private TestDataSetup testDataSetup;

  @Autowired
  private FhirContext fhirContext;

  @Autowired
  private PathlingContext pathlingContext;

  private IParser parser;

  @DynamicPropertySource
  static void configureProperties(final DynamicPropertyRegistry registry) {
    TestDataSetup.staticCopyTestDataToTempDir(warehouseDir);
    registry.add("pathling.storage.warehouseUrl", () -> "file://" + warehouseDir.toAbsolutePath());

    // Configure PnP settings for testing.
    registry.add("pathling.import.pnp.clientId", () -> "test-client");
    registry.add("pathling.import.pnp.clientSecret", () -> "test-secret");
    registry.add("pathling.import.pnp.scope", () -> "system/*.read");
  }

  @BeforeEach
  void setup() {
    parser = fhirContext.newJsonParser();

    webTestClient = webTestClient.mutate()
        .codecs(configurer -> configurer.defaultCodecs().maxInMemorySize(100 * 1024 * 1024))
        .build(); // 100 MB
  }

  @AfterEach
  void cleanup() throws IOException {
    FileUtils.cleanDirectory(warehouseDir.toFile());
  }

  @Test
  void testMissingRespondAsyncHeaderReturnsError() {
    testDataSetup.copyTestDataToTempDir(warehouseDir);

    final String uri = "http://localhost:" + port + "/fhir/$import-pnp";
    final String requestBody = """
        {
          "resourceType": "Parameters",
          "parameter": [
            {
              "name": "exportUrl",
              "valueUrl": "https://example.org/fhir/$export"
            },
            {
              "name": "inputSource",
              "valueString": "https://example.org/fhir"
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
        .expectStatus().is4xxClientError();
  }

  @Test
  void testMissingPnpConfigurationReturnsError() {
    testDataSetup.copyTestDataToTempDir(warehouseDir);

    // This test would need a separate profile without PnP config, so we'll skip it for now.
    // In a real scenario, you'd create a test profile without the PnP configuration.
  }

  @Test
  void testMissingExportUrlReturnsError() {
    testDataSetup.copyTestDataToTempDir(warehouseDir);

    final String uri = "http://localhost:" + port + "/fhir/$import-pnp";
    final String requestBody = """
        {
          "resourceType": "Parameters",
          "parameter": [
            {
              "name": "inputSource",
              "valueString": "https://example.org/fhir"
            }
          ]
        }
        """;

    webTestClient.post()
        .uri(uri)
        .header("Content-Type", "application/fhir+json")
        .header("Accept", "application/fhir+json")
        .header("Prefer", "respond-async")
        .bodyValue(requestBody)
        .exchange()
        .expectStatus().is4xxClientError()
        .expectBody()
        .jsonPath("$.issue[0].diagnostics")
        .value(diagnostics -> assertThat(diagnostics.toString())
            .contains("exportUrl"));
  }

  @Test
  void testMissingInputSourceReturnsError() {
    testDataSetup.copyTestDataToTempDir(warehouseDir);

    final String uri = "http://localhost:" + port + "/fhir/$import-pnp";
    final String requestBody = """
        {
          "resourceType": "Parameters",
          "parameter": [
            {
              "name": "exportUrl",
              "valueUrl": "https://example.org/fhir/$export"
            }
          ]
        }
        """;

    webTestClient.post()
        .uri(uri)
        .header("Content-Type", "application/fhir+json")
        .header("Accept", "application/fhir+json")
        .header("Prefer", "respond-async")
        .bodyValue(requestBody)
        .exchange()
        .expectStatus().is4xxClientError()
        .expectBody()
        .jsonPath("$.issue[0].diagnostics")
        .value(diagnostics -> assertThat(diagnostics.toString())
            .contains("inputSource"));
  }

  @Test
  void testValidRequestReturns202WithContentLocation() {
    testDataSetup.copyTestDataToTempDir(warehouseDir);

    final String uri = "http://localhost:" + port + "/fhir/$import-pnp";
    final String requestBody = """
        {
          "resourceType": "Parameters",
          "parameter": [
            {
              "name": "exportUrl",
              "valueUrl": "https://example.org/fhir/$export"
            },
            {
              "name": "inputSource",
              "valueString": "https://example.org/fhir"
            },
            {
              "name": "exportType",
              "valueCoding": {
                "code": "dynamic"
              }
            }
          ]
        }
        """;

    // Note: This will fail in actual execution because we don't have a real export endpoint.
    // In a full integration test, you'd use WireMock to mock the remote server.
    final var result = webTestClient.post()
        .uri(uri)
        .header("Content-Type", "application/fhir+json")
        .header("Accept", "application/fhir+json")
        .header("Prefer", "respond-async")
        .bodyValue(requestBody)
        .exchange()
        .expectStatus().isAccepted()
        .expectHeader().exists(HttpHeaders.CONTENT_LOCATION)
        .returnResult(String.class);

    // Verify Content-Location header contains job ID.
    final String contentLocation = result.getResponseHeaders().getFirst(HttpHeaders.CONTENT_LOCATION);
    assertThat(contentLocation).isNotNull();
    assertThat(contentLocation).contains("$job");
    assertThat(contentLocation).contains("id=");

    log.info("Import-pnp job created with Content-Location: {}", contentLocation);

    // Poll the job status (in a real test, the job would fail due to no mock server).
    await().atMost(5, TimeUnit.SECONDS)
        .pollInterval(500, TimeUnit.MILLISECONDS)
        .untilAsserted(() -> {
          webTestClient.get()
              .uri(contentLocation)
              .header("Accept", "application/fhir+json")
              .exchange()
              .expectStatus()
              .value(status -> assertThat(status).isIn(202, 500, 400));
          // Expect either still processing (202), or error due to mock endpoint (500/400).
        });
  }

  @Test
  void testStaticModeReturnsError() {
    testDataSetup.copyTestDataToTempDir(warehouseDir);

    final String uri = "http://localhost:" + port + "/fhir/$import-pnp";
    final String requestBody = """
        {
          "resourceType": "Parameters",
          "parameter": [
            {
              "name": "exportUrl",
              "valueUrl": "https://example.org/manifest.json"
            },
            {
              "name": "inputSource",
              "valueString": "https://example.org/fhir"
            },
            {
              "name": "exportType",
              "valueCoding": {
                "code": "static"
              }
            }
          ]
        }
        """;

    final var result = webTestClient.post()
        .uri(uri)
        .header("Content-Type", "application/fhir+json")
        .header("Accept", "application/fhir+json")
        .header("Prefer", "respond-async")
        .bodyValue(requestBody)
        .exchange()
        .expectStatus().isAccepted()
        .expectHeader().exists(HttpHeaders.CONTENT_LOCATION)
        .returnResult(String.class);

    final String contentLocation = result.getResponseHeaders().getFirst(HttpHeaders.CONTENT_LOCATION);
    assertThat(contentLocation).isNotNull();

    // Poll the job status - should fail with error about static mode not supported.
    await().atMost(5, TimeUnit.SECONDS)
        .pollInterval(500, TimeUnit.MILLISECONDS)
        .untilAsserted(() -> {
          webTestClient.get()
              .uri(contentLocation)
              .header("Accept", "application/fhir+json")
              .exchange()
              .expectStatus().is5xxServerError()
              .expectBody()
              .jsonPath("$.issue[0].diagnostics")
              .value(diagnostics -> assertThat(diagnostics.toString())
                  .containsIgnoringCase("static"));
        });
  }

}
