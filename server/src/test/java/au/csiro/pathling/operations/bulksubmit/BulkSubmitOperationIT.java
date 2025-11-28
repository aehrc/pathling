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

package au.csiro.pathling.operations.bulksubmit;

import static com.github.tomakehurst.wiremock.client.WireMock.aResponse;
import static com.github.tomakehurst.wiremock.client.WireMock.get;
import static com.github.tomakehurst.wiremock.client.WireMock.urlEqualTo;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import au.csiro.pathling.util.TestDataSetup;
import com.github.tomakehurst.wiremock.WireMockServer;
import com.github.tomakehurst.wiremock.core.WireMockConfiguration;
import java.io.IOException;
import java.nio.file.Path;
import java.util.UUID;
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
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.web.reactive.server.WebTestClient;

/**
 * Integration test for the $bulk-submit operation.
 *
 * @author John Grimes
 * @see <a href="https://hackmd.io/@argonaut/rJoqHZrPle">Argonaut $bulk-submit Specification</a>
 */
@Slf4j
@Tag("IntegrationTest")
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@ResourceLock(value = "wiremock", mode = ResourceAccessMode.READ_WRITE)
@ActiveProfiles("integration-test")
@TestPropertySource(properties = {
    "pathling.bulk-submit.enabled=true",
    "pathling.bulk-submit.allowable-sources[0]=http://localhost",
    "pathling.bulk-submit.allowed-submitters[0].system=http://example.org/submitters",
    "pathling.bulk-submit.allowed-submitters[0].value=test-submitter"
})
class BulkSubmitOperationIT {

  private static final String SUBMITTER_SYSTEM = "http://example.org/submitters";
  private static final String SUBMITTER_VALUE = "test-submitter";

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
    registry.add("pathling.bulk-submit.staging-location",
        () -> "file://" + warehouseDir.resolve("staging").toAbsolutePath());
  }

  @BeforeEach
  void setup() {
    webTestClient = webTestClient.mutate()
        .codecs(configurer -> configurer.defaultCodecs().maxInMemorySize(100 * 1024 * 1024))
        .build();

    // Reset WireMock stubs before each test.
    wireMockServer.resetAll();
  }

  @AfterEach
  void cleanup() throws IOException {
    FileUtils.cleanDirectory(warehouseDir.toFile());
  }

  /**
   * Sets up WireMock stubs for a successful bulk data manifest and data files.
   */
  private void setupSuccessfulManifestStubs() {
    final String baseUrl = "http://localhost:" + wireMockServer.port();

    // Stub manifest endpoint.
    final String manifest = String.format("""
        {
          "transactionTime": "2025-11-28T00:00:00Z",
          "request": "%s/fhir/$export",
          "requiresAccessToken": false,
          "output": [
            {"type": "Patient", "url": "%s/data/Patient.ndjson"},
            {"type": "Observation", "url": "%s/data/Observation.ndjson"}
          ],
          "error": []
        }
        """, baseUrl, baseUrl, baseUrl);

    wireMockServer.stubFor(get(urlEqualTo("/manifest.json"))
        .willReturn(aResponse()
            .withStatus(200)
            .withHeader("Content-Type", "application/json")
            .withBody(manifest)));

    // Stub Patient NDJSON file.
    final String patientNdjson = """
        {"resourceType":"Patient","id":"patient1","name":[{"family":"Smith","given":["John"]}]}
        {"resourceType":"Patient","id":"patient2","name":[{"family":"Jones","given":["Jane"]}]}
        """;
    wireMockServer.stubFor(get(urlEqualTo("/data/Patient.ndjson"))
        .willReturn(aResponse()
            .withStatus(200)
            .withHeader("Content-Type", "application/fhir+ndjson")
            .withBody(patientNdjson)));

    // Stub Observation NDJSON file.
    final String observationNdjson = """
        {"resourceType":"Observation","id":"obs1","status":"final","code":{"coding":[{"system":"http://loinc.org","code":"8867-4"}]}}
        """;
    wireMockServer.stubFor(get(urlEqualTo("/data/Observation.ndjson"))
        .willReturn(aResponse()
            .withStatus(200)
            .withHeader("Content-Type", "application/fhir+ndjson")
            .withBody(observationNdjson)));

    log.info("Set up successful manifest stubs on WireMock server");
  }

  /**
   * Builds a $bulk-submit request Parameters resource.
   *
   * @param submissionId The submission ID.
   * @param status The submission status (in-progress, complete, or aborted).
   * @param manifestUrl Optional manifest URL (null if not provided).
   * @param fhirBaseUrl Optional FHIR base URL (null if not provided).
   * @return JSON string representation of the Parameters resource.
   */
  private String buildBulkSubmitRequest(final String submissionId, final String status,
      final String manifestUrl, final String fhirBaseUrl) {
    final StringBuilder params = new StringBuilder();
    params.append("""
        {
          "resourceType": "Parameters",
          "parameter": [
            {
              "name": "submitter",
              "valueIdentifier": {
                "system": "%s",
                "value": "%s"
              }
            },
            {"name": "submissionId", "valueString": "%s"},
            {
              "name": "submissionStatus",
              "valueCoding": {"code": "%s"}
            }
        """.formatted(SUBMITTER_SYSTEM, SUBMITTER_VALUE, submissionId, status));

    if (manifestUrl != null) {
      params.append("""
            ,{"name": "manifestUrl", "valueUrl": "%s"}
          """.formatted(manifestUrl));
    }

    if (fhirBaseUrl != null) {
      params.append("""
            ,{"name": "fhirBaseUrl", "valueUrl": "%s"}
          """.formatted(fhirBaseUrl));
    }

    params.append("]}");
    return params.toString();
  }

  @Test
  void testDirectCompleteWorkflow() {
    TestDataSetup.copyTestDataToTempDir(warehouseDir);
    setupSuccessfulManifestStubs();

    final String submissionId = UUID.randomUUID().toString();
    final String manifestUrl = "http://localhost:" + wireMockServer.port() + "/manifest.json";
    final String fhirBaseUrl = "http://localhost:" + wireMockServer.port() + "/fhir";
    final String uri = "http://localhost:" + port + "/fhir/$bulk-submit";

    // Send complete directly with manifest URL.
    final String requestBody = buildBulkSubmitRequest(submissionId, "complete", manifestUrl,
        fhirBaseUrl);

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

    log.info("Direct complete submission created with Content-Location: {}", contentLocation);

    // Poll the job status until completion.
    await().atMost(60, TimeUnit.SECONDS)
        .pollInterval(2, TimeUnit.SECONDS)
        .untilAsserted(() -> {
          webTestClient.get()
              .uri(contentLocation)
              .header("Accept", "application/fhir+json")
              .exchange()
              .expectStatus().isOk();
        });

    log.info("Direct complete workflow finished successfully");
  }

  @Test
  void testInProgressThenCompleteWorkflow() {
    TestDataSetup.copyTestDataToTempDir(warehouseDir);
    setupSuccessfulManifestStubs();

    final String submissionId = UUID.randomUUID().toString();
    final String manifestUrl = "http://localhost:" + wireMockServer.port() + "/manifest.json";
    final String fhirBaseUrl = "http://localhost:" + wireMockServer.port() + "/fhir";
    final String uri = "http://localhost:" + port + "/fhir/$bulk-submit";

    // Step 1: Send in-progress notification.
    final String inProgressRequest = buildBulkSubmitRequest(submissionId, "in-progress", null,
        null);

    final var inProgressResult = webTestClient.post()
        .uri(uri)
        .header("Content-Type", "application/fhir+json")
        .header("Accept", "application/fhir+json")
        .header("Prefer", "respond-async")
        .bodyValue(inProgressRequest)
        .exchange()
        .expectStatus().isAccepted()
        .expectHeader().exists(HttpHeaders.CONTENT_LOCATION)
        .returnResult(String.class);

    final String inProgressContentLocation = inProgressResult.getResponseHeaders()
        .getFirst(HttpHeaders.CONTENT_LOCATION);
    assertThat(inProgressContentLocation).isNotNull();
    log.info("In-progress notification accepted: {}", inProgressContentLocation);

    // Wait for the in-progress job to complete (returns quickly).
    await().atMost(10, TimeUnit.SECONDS)
        .pollInterval(500, TimeUnit.MILLISECONDS)
        .untilAsserted(() -> {
          webTestClient.get()
              .uri(inProgressContentLocation)
              .header("Accept", "application/fhir+json")
              .exchange()
              .expectStatus().isOk();
        });

    // Step 2: Send complete notification with manifest URL.
    final String completeRequest = buildBulkSubmitRequest(submissionId, "complete", manifestUrl,
        fhirBaseUrl);

    final var completeResult = webTestClient.post()
        .uri(uri)
        .header("Content-Type", "application/fhir+json")
        .header("Accept", "application/fhir+json")
        .header("Prefer", "respond-async")
        .bodyValue(completeRequest)
        .exchange()
        .expectStatus().isAccepted()
        .expectHeader().exists(HttpHeaders.CONTENT_LOCATION)
        .returnResult(String.class);

    final String completeContentLocation = completeResult.getResponseHeaders()
        .getFirst(HttpHeaders.CONTENT_LOCATION);
    assertThat(completeContentLocation).isNotNull();
    log.info("Complete notification accepted: {}", completeContentLocation);

    // Poll the complete job until it finishes.
    await().atMost(60, TimeUnit.SECONDS)
        .pollInterval(2, TimeUnit.SECONDS)
        .untilAsserted(() -> {
          webTestClient.get()
              .uri(completeContentLocation)
              .header("Accept", "application/fhir+json")
              .exchange()
              .expectStatus().isOk();
        });

    log.info("In-progress → complete workflow finished successfully");
  }

}
