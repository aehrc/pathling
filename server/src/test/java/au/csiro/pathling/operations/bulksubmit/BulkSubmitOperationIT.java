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

package au.csiro.pathling.operations.bulksubmit;

import static com.github.tomakehurst.wiremock.client.WireMock.aResponse;
import static com.github.tomakehurst.wiremock.client.WireMock.equalTo;
import static com.github.tomakehurst.wiremock.client.WireMock.get;
import static com.github.tomakehurst.wiremock.client.WireMock.getRequestedFor;
import static com.github.tomakehurst.wiremock.client.WireMock.head;
import static com.github.tomakehurst.wiremock.client.WireMock.urlEqualTo;
import static org.awaitility.Awaitility.await;

import au.csiro.pathling.util.TestDataSetup;
import com.github.tomakehurst.wiremock.WireMockServer;
import com.github.tomakehurst.wiremock.core.WireMockConfiguration;
import java.io.IOException;
import java.nio.file.Path;
import java.time.Duration;
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
@TestPropertySource(
    properties = {
      "pathling.async.enabled=true",
      "pathling.bulk-submit.allowable-sources[0]=http://localhost",
      "pathling.bulk-submit.allowed-submitters[0].system=http://example.org/submitters",
      "pathling.bulk-submit.allowed-submitters[0].value=test-submitter"
    })
class BulkSubmitOperationIT {

  private static final String SUBMITTER_SYSTEM = "http://example.org/submitters";
  private static final String SUBMITTER_VALUE = "test-submitter";

  private static WireMockServer wireMockServer;

  @LocalServerPort int port;

  @Autowired WebTestClient webTestClient;

  @TempDir private static Path warehouseDir;

  @TempDir private static Path stagingDir;

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
    // Copy only Condition data - exclude Patient and Observation which will be imported.
    TestDataSetup.copyTestDataToTempDir(warehouseDir, "Condition");
    registry.add("pathling.storage.warehouseUrl", () -> "file://" + warehouseDir.toAbsolutePath());
    registry.add("pathling.bulk-submit.staging-directory", stagingDir::toString);
  }

  @BeforeEach
  void setup() {
    webTestClient =
        webTestClient
            .mutate()
            .codecs(configurer -> configurer.defaultCodecs().maxInMemorySize(100 * 1024 * 1024))
            .responseTimeout(Duration.ofMinutes(2))
            .build();

    // Reset WireMock stubs before each test.
    wireMockServer.resetAll();
  }

  @AfterEach
  void cleanup() throws IOException {
    FileUtils.cleanDirectory(warehouseDir.toFile());
  }

  /** Sets up WireMock stubs for a successful bulk data manifest and data files. */
  private void setupSuccessfulManifestStubs() {
    final String baseUrl = "http://localhost:" + wireMockServer.port();

    // Stub manifest endpoint.
    final String manifest =
        String.format(
            """
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
            """,
            baseUrl, baseUrl, baseUrl);

    wireMockServer.stubFor(
        get(urlEqualTo("/manifest.json"))
            .willReturn(
                aResponse()
                    .withStatus(200)
                    .withHeader("Content-Type", "application/json")
                    .withBody(manifest)));

    // Stub Patient NDJSON file.
    final String patientNdjson =
        """
        {"resourceType":"Patient","id":"patient1","name":[{"family":"Smith","given":["John"]}]}
        {"resourceType":"Patient","id":"patient2","name":[{"family":"Jones","given":["Jane"]}]}
        """;
    wireMockServer.stubFor(
        head(urlEqualTo("/data/Patient.ndjson"))
            .willReturn(
                aResponse()
                    .withStatus(200)
                    .withHeader(
                        "Content-Length", String.valueOf(patientNdjson.getBytes().length))));
    wireMockServer.stubFor(
        get(urlEqualTo("/data/Patient.ndjson"))
            .willReturn(
                aResponse()
                    .withStatus(200)
                    .withHeader("Content-Type", "application/fhir+ndjson")
                    .withBody(patientNdjson)));

    // Stub Observation NDJSON file.
    final String observationNdjson =
        """
        {"resourceType":"Observation","id":"obs1","status":"final","code":{"coding":[{"system":"http://loinc.org","code":"8867-4"}]}}
        """;
    wireMockServer.stubFor(
        head(urlEqualTo("/data/Observation.ndjson"))
            .willReturn(
                aResponse()
                    .withStatus(200)
                    .withHeader(
                        "Content-Length", String.valueOf(observationNdjson.getBytes().length))));
    wireMockServer.stubFor(
        get(urlEqualTo("/data/Observation.ndjson"))
            .willReturn(
                aResponse()
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
  private String buildBulkSubmitRequest(
      final String submissionId,
      final String status,
      final String manifestUrl,
      final String fhirBaseUrl) {
    final StringBuilder params = new StringBuilder();
    params.append(
        """
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
        """
            .formatted(SUBMITTER_SYSTEM, SUBMITTER_VALUE, submissionId, status));

    if (manifestUrl != null) {
      params.append(
          """
            ,{"name": "manifestUrl", "valueString": "%s"}
          """
              .formatted(manifestUrl));
    }

    if (fhirBaseUrl != null) {
      params.append(
          """
            ,{"name": "fhirBaseUrl", "valueString": "%s"}
          """
              .formatted(fhirBaseUrl));
    }

    params.append("]}");
    return params.toString();
  }

  /**
   * Builds a $bulk-submit request with file request headers.
   *
   * @param submissionId The submission ID.
   * @param status The submission status.
   * @param manifestUrl The manifest URL.
   * @param fhirBaseUrl The FHIR base URL.
   * @param headerName The header name to include.
   * @param headerValue The header value to include.
   * @return JSON string representation of the Parameters resource.
   */
  private String buildBulkSubmitRequestWithHeaders(
      final String submissionId,
      final String status,
      final String manifestUrl,
      final String fhirBaseUrl,
      final String headerName,
      final String headerValue) {
    return """
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
        {"name": "submissionStatus", "valueCoding": {"code": "%s"}},
        {"name": "manifestUrl", "valueString": "%s"},
        {"name": "fhirBaseUrl", "valueString": "%s"},
        {
          "name": "fileRequestHeader",
          "part": [
            {"name": "headerName", "valueString": "%s"},
            {"name": "headerValue", "valueString": "%s"}
          ]
        }
      ]
    }
    """
        .formatted(
            SUBMITTER_SYSTEM,
            SUBMITTER_VALUE,
            submissionId,
            status,
            manifestUrl,
            fhirBaseUrl,
            headerName,
            headerValue);
  }

  /**
   * Builds a $bulk-submit-status request.
   *
   * @param submissionId The submission ID to check status for.
   * @return JSON string representation of the Parameters resource.
   */
  private String buildBulkSubmitStatusRequest(final String submissionId) {
    return """
    {
      "resourceType": "Parameters",
      "parameter": [
        {"name": "submissionId", "valueString": "%s"},
        {
          "name": "submitter",
          "valueIdentifier": {
            "system": "%s",
            "value": "%s"
          }
        }
      ]
    }
    """
        .formatted(submissionId, SUBMITTER_SYSTEM, SUBMITTER_VALUE);
  }

  @Test
  void testDirectCompleteWorkflow() {
    // This test verifies the full workflow: send in-progress with manifest, wait for downloads,
    // then send complete to trigger import.
    TestDataSetup.copyTestDataToTempDir(warehouseDir, "Condition");
    setupSuccessfulManifestStubs();

    final String submissionId = UUID.randomUUID().toString();
    final String manifestUrl = "http://localhost:" + wireMockServer.port() + "/manifest.json";
    final String fhirBaseUrl = "http://localhost:" + wireMockServer.port() + "/fhir";
    final String uri = "http://localhost:" + port + "/fhir/$bulk-submit";

    // Step 1: Send in-progress with manifest URL to start downloading.
    final String inProgressRequest =
        buildBulkSubmitRequest(submissionId, "in-progress", manifestUrl, fhirBaseUrl);

    webTestClient
        .post()
        .uri(uri)
        .header("Content-Type", "application/fhir+json")
        .header("Accept", "application/fhir+json")
        .bodyValue(inProgressRequest)
        .exchange()
        .expectStatus()
        .isOk();

    log.info("In-progress with manifest accepted for submission: {}", submissionId);

    // Step 2: Poll $bulk-submit-status until downloads are complete (202 -> 200).
    final String statusUri = "http://localhost:" + port + "/fhir/$bulk-submit-status";
    final String statusRequest = buildBulkSubmitStatusRequest(submissionId);

    // Wait for downloads to complete - status will return 202 while processing.
    await()
        .atMost(60, TimeUnit.SECONDS)
        .pollInterval(2, TimeUnit.SECONDS)
        .untilAsserted(
            () -> {
              webTestClient
                  .post()
                  .uri(statusUri)
                  .header("Content-Type", "application/fhir+json")
                  .header("Accept", "application/fhir+json")
                  .header("Prefer", "respond-async")
                  .bodyValue(statusRequest)
                  .exchange()
                  .expectStatus()
                  .is2xxSuccessful();
            });

    log.info("Downloads complete for submission: {}", submissionId);

    // Step 3: Send complete (without manifest) to trigger import.
    final String completeRequest = buildBulkSubmitRequest(submissionId, "complete", null, null);

    webTestClient
        .post()
        .uri(uri)
        .header("Content-Type", "application/fhir+json")
        .header("Accept", "application/fhir+json")
        .bodyValue(completeRequest)
        .exchange()
        .expectStatus()
        .isOk();

    log.info("Complete workflow finished successfully");
  }

  @Test
  void testInProgressThenCompleteWorkflow() {
    // This test verifies sending an initial in-progress without manifest, then another
    // in-progress with manifest, then complete.
    TestDataSetup.copyTestDataToTempDir(warehouseDir, "Condition");
    setupSuccessfulManifestStubs();

    final String submissionId = UUID.randomUUID().toString();
    final String manifestUrl = "http://localhost:" + wireMockServer.port() + "/manifest.json";
    final String fhirBaseUrl = "http://localhost:" + wireMockServer.port() + "/fhir";
    final String uri = "http://localhost:" + port + "/fhir/$bulk-submit";

    // Step 1: Send initial in-progress notification (without manifest).
    final String initialInProgressRequest =
        buildBulkSubmitRequest(submissionId, "in-progress", null, null);

    webTestClient
        .post()
        .uri(uri)
        .header("Content-Type", "application/fhir+json")
        .header("Accept", "application/fhir+json")
        .bodyValue(initialInProgressRequest)
        .exchange()
        .expectStatus()
        .isOk();

    log.info("Initial in-progress notification accepted for submission: {}", submissionId);

    // Step 2: Send in-progress with manifest URL to start downloading.
    final String manifestInProgressRequest =
        buildBulkSubmitRequest(submissionId, "in-progress", manifestUrl, fhirBaseUrl);

    webTestClient
        .post()
        .uri(uri)
        .header("Content-Type", "application/fhir+json")
        .header("Accept", "application/fhir+json")
        .bodyValue(manifestInProgressRequest)
        .exchange()
        .expectStatus()
        .isOk();

    log.info("In-progress with manifest accepted for submission: {}", submissionId);

    // Step 3: Poll $bulk-submit-status until downloads are complete.
    final String statusUri = "http://localhost:" + port + "/fhir/$bulk-submit-status";
    final String statusRequest = buildBulkSubmitStatusRequest(submissionId);

    await()
        .atMost(60, TimeUnit.SECONDS)
        .pollInterval(2, TimeUnit.SECONDS)
        .untilAsserted(
            () -> {
              webTestClient
                  .post()
                  .uri(statusUri)
                  .header("Content-Type", "application/fhir+json")
                  .header("Accept", "application/fhir+json")
                  .header("Prefer", "respond-async")
                  .bodyValue(statusRequest)
                  .exchange()
                  .expectStatus()
                  .is2xxSuccessful();
            });

    log.info("Downloads complete for submission: {}", submissionId);

    // Step 4: Send complete (without manifest) to trigger import.
    final String completeRequest = buildBulkSubmitRequest(submissionId, "complete", null, null);

    webTestClient
        .post()
        .uri(uri)
        .header("Content-Type", "application/fhir+json")
        .header("Accept", "application/fhir+json")
        .bodyValue(completeRequest)
        .exchange()
        .expectStatus()
        .isOk();

    log.info("In-progress → complete workflow finished successfully");
  }

  @Test
  void testFileRequestHeadersArePassedToServer() {
    // This test verifies that fileRequestHeader parameters from the kickoff request are included
    // when fetching manifest and data files.
    TestDataSetup.copyTestDataToTempDir(warehouseDir, "Condition");
    setupSuccessfulManifestStubs();

    final String submissionId = UUID.randomUUID().toString();
    final String manifestUrl = "http://localhost:" + wireMockServer.port() + "/manifest.json";
    final String fhirBaseUrl = "http://localhost:" + wireMockServer.port() + "/fhir";
    final String uri = "http://localhost:" + port + "/fhir/$bulk-submit";

    // Step 1: Send in-progress with manifest URL and custom Authorization header.
    final String requestBody =
        buildBulkSubmitRequestWithHeaders(
            submissionId,
            "in-progress",
            manifestUrl,
            fhirBaseUrl,
            "Authorization",
            "Bearer test-token-12345");

    webTestClient
        .post()
        .uri(uri)
        .header("Content-Type", "application/fhir+json")
        .header("Accept", "application/fhir+json")
        .bodyValue(requestBody)
        .exchange()
        .expectStatus()
        .isOk();

    log.info("In-progress with custom header accepted: {}", submissionId);

    // Step 2: Poll $bulk-submit-status until downloads complete.
    final String statusUri = "http://localhost:" + port + "/fhir/$bulk-submit-status";
    final String statusRequest = buildBulkSubmitStatusRequest(submissionId);

    await()
        .atMost(60, TimeUnit.SECONDS)
        .pollInterval(2, TimeUnit.SECONDS)
        .untilAsserted(
            () -> {
              webTestClient
                  .post()
                  .uri(statusUri)
                  .header("Content-Type", "application/fhir+json")
                  .header("Accept", "application/fhir+json")
                  .header("Prefer", "respond-async")
                  .bodyValue(statusRequest)
                  .exchange()
                  .expectStatus()
                  .is2xxSuccessful();
            });

    log.info("Downloads complete for submission: {}", submissionId);

    // Step 3: Send complete (without manifest) to trigger import.
    final String completeRequest = buildBulkSubmitRequest(submissionId, "complete", null, null);

    webTestClient
        .post()
        .uri(uri)
        .header("Content-Type", "application/fhir+json")
        .header("Accept", "application/fhir+json")
        .bodyValue(completeRequest)
        .exchange()
        .expectStatus()
        .isOk();

    // Verify that the Authorization header was included in the manifest request.
    wireMockServer.verify(
        getRequestedFor(urlEqualTo("/manifest.json"))
            .withHeader("Authorization", equalTo("Bearer test-token-12345")));

    // Verify that the Authorization header was included in the data file requests.
    wireMockServer.verify(
        getRequestedFor(urlEqualTo("/data/Patient.ndjson"))
            .withHeader("Authorization", equalTo("Bearer test-token-12345")));

    wireMockServer.verify(
        getRequestedFor(urlEqualTo("/data/Observation.ndjson"))
            .withHeader("Authorization", equalTo("Bearer test-token-12345")));

    log.info("Verified that custom headers were passed to manifest and data file requests");
  }
}
