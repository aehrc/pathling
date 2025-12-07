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

import static com.github.tomakehurst.wiremock.client.WireMock.aResponse;
import static com.github.tomakehurst.wiremock.client.WireMock.get;
import static com.github.tomakehurst.wiremock.client.WireMock.head;
import static com.github.tomakehurst.wiremock.client.WireMock.urlEqualTo;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import au.csiro.pathling.util.TestDataSetup;
import com.github.tomakehurst.wiremock.WireMockServer;
import com.github.tomakehurst.wiremock.core.WireMockConfiguration;
import java.io.IOException;
import java.nio.file.Path;
import java.util.concurrent.TimeUnit;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.io.FileUtils;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
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
 * Integration test for the $import operation, testing both JSON manifest (SMART Bulk Data Import)
 * and FHIR Parameters request formats.
 *
 * @author John Grimes
 */
@Slf4j
@Tag("IntegrationTest")
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@ResourceLock(value = "wiremock", mode = ResourceAccessMode.READ_WRITE)
@ActiveProfiles({"integration-test"})
class ImportOperationIT {

  private static WireMockServer wireMockServer;

  @LocalServerPort
  int port;

  @Autowired
  WebTestClient webTestClient;

  @TempDir
  private static Path warehouseDir;

  @BeforeAll
  static void setupWireMock() {
    wireMockServer = new WireMockServer(WireMockConfiguration.options().dynamicPort());
    wireMockServer.start();
    log.info("WireMock server started on port: {}", wireMockServer.port());
  }

  @AfterAll
  static void tearDownWireMock() {
    if (wireMockServer != null && wireMockServer.isRunning()) {
      wireMockServer.stop();
      log.info("WireMock server stopped");
    }
  }

  @DynamicPropertySource
  static void configureProperties(final DynamicPropertyRegistry registry) {
    TestDataSetup.copyTestDataToTempDir(warehouseDir);
    registry.add("pathling.storage.warehouseUrl", () -> "file://" + warehouseDir.toAbsolutePath());
    // Allow imports from WireMock server.
    registry.add("pathling.import.allowableSources",
        () -> "http://localhost:" + wireMockServer.port() + "/");
  }

  @BeforeEach
  void setup() {
    webTestClient = webTestClient.mutate()
        .codecs(configurer -> configurer.defaultCodecs().maxInMemorySize(100 * 1024 * 1024))
        .build();
    wireMockServer.resetAll();
    setupNdjsonFileStubs();
  }

  @AfterEach
  void cleanup() throws IOException {
    FileUtils.cleanDirectory(warehouseDir.toFile());
  }

  /**
   * Sets up WireMock stubs for NDJSON files.
   */
  private void setupNdjsonFileStubs() {
    final String patientNdjson = """
        {"resourceType":"Patient","id":"patient1","name":[{"family":"Smith","given":["John"]}]}
        {"resourceType":"Patient","id":"patient2","name":[{"family":"Jones","given":["Jane"]}]}
        """;
    // HEAD stub for Spark to check file existence.
    wireMockServer.stubFor(head(urlEqualTo("/data/Patient.ndjson"))
        .willReturn(aResponse()
            .withStatus(200)
            .withHeader("Content-Type", "application/fhir+ndjson")
            .withHeader("Content-Length", String.valueOf(patientNdjson.length()))));
    // GET stub for actual file content.
    wireMockServer.stubFor(get(urlEqualTo("/data/Patient.ndjson"))
        .willReturn(aResponse()
            .withStatus(200)
            .withHeader("Content-Type", "application/fhir+ndjson")
            .withBody(patientNdjson)));

    final String observationNdjson = """
        {"resourceType":"Observation","id":"obs1","status":"final","code":{"coding":[{"system":"http://loinc.org","code":"8867-4"}]}}
        """;
    // HEAD stub for Spark to check file existence.
    wireMockServer.stubFor(head(urlEqualTo("/data/Observation.ndjson"))
        .willReturn(aResponse()
            .withStatus(200)
            .withHeader("Content-Type", "application/fhir+ndjson")
            .withHeader("Content-Length", String.valueOf(observationNdjson.length()))));
    // GET stub for actual file content.
    wireMockServer.stubFor(get(urlEqualTo("/data/Observation.ndjson"))
        .willReturn(aResponse()
            .withStatus(200)
            .withHeader("Content-Type", "application/fhir+ndjson")
            .withBody(observationNdjson)));

    log.info("Set up NDJSON file stubs on WireMock server");
  }

  @Test
  void testImportWithFhirParametersFormat() {
    TestDataSetup.copyTestDataToTempDir(warehouseDir);

    final String baseUrl = "http://localhost:" + wireMockServer.port();
    final String uri = "http://localhost:" + port + "/fhir/$import";
    final String requestBody = String.format("""
        {
          "resourceType": "Parameters",
          "parameter": [
            {
              "name": "inputSource",
              "valueString": "https://example.org/source"
            },
            {
              "name": "input",
              "part": [
                {
                  "name": "resourceType",
                  "valueCoding": {
                    "code": "Patient"
                  }
                },
                {
                  "name": "url",
                  "valueUrl": "%s/data/Patient.ndjson"
                }
              ]
            }
          ]
        }
        """, baseUrl);

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

    final String contentLocation = result.getResponseHeaders()
        .getFirst(HttpHeaders.CONTENT_LOCATION);
    assertThat(contentLocation).isNotNull();
    assertThat(contentLocation).contains("$job");

    log.info("Import job created with Content-Location: {}", contentLocation);

    // Poll the job status until completion.
    await().atMost(30, TimeUnit.SECONDS)
        .pollInterval(2, TimeUnit.SECONDS)
        .untilAsserted(() -> {
          webTestClient.get()
              .uri(contentLocation)
              .header("Accept", "application/fhir+json")
              .exchange()
              .expectStatus().isOk();
        });

    log.info("Import job completed successfully with FHIR Parameters format");
  }

  @Test
  void testImportWithJsonManifestFormat() {
    TestDataSetup.copyTestDataToTempDir(warehouseDir);

    final String baseUrl = "http://localhost:" + wireMockServer.port();
    final String uri = "http://localhost:" + port + "/fhir/$import";

    // JSON manifest format (SMART Bulk Data Import specification).
    final String requestBody = String.format("""
        {
          "inputFormat": "application/fhir+ndjson",
          "inputSource": "https://example.org/source",
          "input": [
            {
              "type": "Patient",
              "url": "%s/data/Patient.ndjson"
            }
          ],
          "mode": "overwrite"
        }
        """, baseUrl);

    final var result = webTestClient.post()
        .uri(uri)
        .header("Content-Type", "application/json")  // Note: application/json, not application/fhir+json
        .header("Accept", "application/fhir+json")
        .header("Prefer", "respond-async")
        .bodyValue(requestBody)
        .exchange()
        .expectStatus().isAccepted()
        .expectHeader().exists(HttpHeaders.CONTENT_LOCATION)
        .returnResult(String.class);

    final String contentLocation = result.getResponseHeaders()
        .getFirst(HttpHeaders.CONTENT_LOCATION);
    assertThat(contentLocation).isNotNull();
    assertThat(contentLocation).contains("$job");

    log.info("Import job created with Content-Location: {}", contentLocation);

    // Poll the job status until completion.
    await().atMost(30, TimeUnit.SECONDS)
        .pollInterval(2, TimeUnit.SECONDS)
        .untilAsserted(() -> {
          webTestClient.get()
              .uri(contentLocation)
              .header("Accept", "application/fhir+json")
              .exchange()
              .expectStatus().isOk();
        });

    log.info("Import job completed successfully with JSON manifest format");
  }

  @Test
  void testImportMissingRespondAsyncHeader() {
    TestDataSetup.copyTestDataToTempDir(warehouseDir);

    final String uri = "http://localhost:" + port + "/fhir/$import";
    final String requestBody = """
        {
          "resourceType": "Parameters",
          "parameter": [
            {
              "name": "inputSource",
              "valueString": "https://example.org/source"
            },
            {
              "name": "input",
              "part": [
                {
                  "name": "resourceType",
                  "valueCoding": {
                    "code": "Patient"
                  }
                },
                {
                  "name": "url",
                  "valueUrl": "http://example.org/data/Patient.ndjson"
                }
              ]
            }
          ]
        }
        """;

    webTestClient.post()
        .uri(uri)
        .header("Content-Type", "application/fhir+json")
        .header("Accept", "application/fhir+json")
        // Missing Prefer: respond-async header
        .bodyValue(requestBody)
        .exchange()
        .expectStatus().is4xxClientError();
  }

  @Test
  void testImportMissingInputSource() {
    TestDataSetup.copyTestDataToTempDir(warehouseDir);

    final String uri = "http://localhost:" + port + "/fhir/$import";
    final String requestBody = """
        {
          "resourceType": "Parameters",
          "parameter": [
            {
              "name": "input",
              "part": [
                {
                  "name": "resourceType",
                  "valueCoding": {
                    "code": "Patient"
                  }
                },
                {
                  "name": "url",
                  "valueUrl": "http://example.org/data/Patient.ndjson"
                }
              ]
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

}
