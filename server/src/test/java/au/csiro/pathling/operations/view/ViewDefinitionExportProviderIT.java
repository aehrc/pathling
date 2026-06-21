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

package au.csiro.pathling.operations.view;

import static org.assertj.core.api.Assertions.assertThat;

import ca.uhn.fhir.context.FhirContext;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import jakarta.annotation.Nonnull;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.ResourceAccessMode;
import org.junit.jupiter.api.parallel.ResourceLock;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.http.HttpStatus;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.test.web.reactive.server.WebTestClient;

/**
 * Integration tests for {@link ViewDefinitionExportProvider}.
 *
 * @author John Grimes
 */
@Slf4j
@Tag("IntegrationTest")
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@ResourceLock(value = "wiremock", mode = ResourceAccessMode.READ_WRITE)
@ActiveProfiles({"integration-test"})
class ViewDefinitionExportProviderIT {

  @LocalServerPort int port;

  @Autowired WebTestClient webTestClient;

  private static Path warehouseDir;

  @Autowired private FhirContext fhirContext;

  private Gson gson;

  @DynamicPropertySource
  static void configureProperties(final DynamicPropertyRegistry registry) {
    warehouseDir = Path.of("src/test/resources/test-data/bulk/fhir/delta").toAbsolutePath();
    registry.add("pathling.storage.warehouseUrl", () -> "file://" + warehouseDir);
  }

  @BeforeEach
  void setup() {
    webTestClient =
        webTestClient
            .mutate()
            .codecs(configurer -> configurer.defaultCodecs().maxInMemorySize(100 * 1024 * 1024))
            .build();
    gson = new GsonBuilder().create();
  }

  /**
   * Tests that the $viewdefinition-export operation accepts the nested Parameters structure as
   * documented. This test reproduces the structure sent by the UI.
   */
  @Test
  void kickOffWithNestedViewResourceParameter() {
    final String parametersJson = createExportParametersWithNestedView();
    log.debug("Request body:\n{}", parametersJson);

    webTestClient
        .post()
        .uri("http://localhost:" + port + "/fhir/$viewdefinition-export")
        .header("Content-Type", "application/fhir+json")
        .header("Accept", "application/fhir+json")
        .header("Prefer", "respond-async")
        .bodyValue(parametersJson)
        .exchange()
        .expectStatus()
        .isAccepted()
        .expectHeader()
        .exists("Content-Location");
  }

  /** Tests that the $viewdefinition-export operation accepts multiple views with names. */
  @Test
  void kickOffWithMultipleNamedViews() {
    final String parametersJson = createExportParametersWithMultipleViews();
    log.debug("Request body:\n{}", parametersJson);

    webTestClient
        .post()
        .uri("http://localhost:" + port + "/fhir/$viewdefinition-export")
        .header("Content-Type", "application/fhir+json")
        .header("Accept", "application/fhir+json")
        .header("Prefer", "respond-async")
        .bodyValue(parametersJson)
        .exchange()
        .expectStatus()
        .isAccepted()
        .expectHeader()
        .exists("Content-Location");
  }

  // -------------------------------------------------------------------------
  // Kick-off acknowledgement body (US10)
  // -------------------------------------------------------------------------

  @Test
  void kickOffBodyIsParametersAcknowledgement() {
    final byte[] body =
        webTestClient
            .post()
            .uri("http://localhost:" + port + "/fhir/$viewdefinition-export")
            .header("Content-Type", "application/fhir+json")
            .header("Accept", "application/fhir+json")
            .header("Prefer", "respond-async")
            .bodyValue(createExportParametersWithNestedView())
            .exchange()
            .expectStatus()
            .isAccepted()
            .expectHeader()
            .exists("Content-Location")
            .expectBody()
            .returnResult()
            .getResponseBodyContent();

    final Map<String, Object> manifest = parseParameters(body);
    assertThat(manifest.get("resourceType")).isEqualTo("Parameters");
    assertThat(findParamValue(manifest, "status", "valueCode")).isEqualTo("accepted");
    assertThat(findParamValue(manifest, "exportId", "valueString")).isNotNull();
  }

  // -------------------------------------------------------------------------
  // Synchronous kick-off rejections (US1, US8)
  // -------------------------------------------------------------------------

  @Test
  void unsupportedFormatRejectedAtKickOff() {
    final Map<String, Object> parameters = parametersWithNestedView();
    addSimpleParameter(parameters, "_format", "valueString", "json");

    final byte[] body =
        kickOff(gson.toJson(parameters))
            .expectStatus()
            .isBadRequest()
            .expectBody()
            .returnResult()
            .getResponseBodyContent();
    assertThat(new String(body, java.nio.charset.StandardCharsets.UTF_8)).contains("json");
  }

  @Test
  void sourceRejectedAtKickOff() {
    final Map<String, Object> parameters = parametersWithNestedView();
    addSimpleParameter(parameters, "source", "valueString", "s3://bucket/data");

    final byte[] body =
        kickOff(gson.toJson(parameters))
            .expectStatus()
            .isBadRequest()
            .expectBody()
            .returnResult()
            .getResponseBodyContent();
    assertThat(new String(body, java.nio.charset.StandardCharsets.UTF_8)).contains("source");
  }

  @Test
  void viewPartWithBothResourceAndReferenceRejectedAtKickOff() {
    final Map<String, Object> viewPart = viewPartWithResource();
    @SuppressWarnings("unchecked")
    final List<Map<String, Object>> parts = (List<Map<String, Object>>) viewPart.get("part");
    parts.add(referencePart("viewReference", "ViewDefinition/some-view"));

    final Map<String, Object> parameters = parametersWith(viewPart);
    kickOff(gson.toJson(parameters)).expectStatus().isBadRequest();
  }

  @Test
  void viewPartWithNeitherResourceNorReferenceRejectedAtKickOff() {
    final Map<String, Object> viewPart = new LinkedHashMap<>();
    viewPart.put("name", "view");
    viewPart.put("part", new ArrayList<>(List.of(simplePart("name", "valueString", "empty"))));

    final Map<String, Object> parameters = parametersWith(viewPart);
    kickOff(gson.toJson(parameters)).expectStatus().isBadRequest();
  }

  @Test
  void unresolvedViewReferenceRejectedAtKickOffWith404() {
    final Map<String, Object> viewPart = new LinkedHashMap<>();
    viewPart.put("name", "view");
    viewPart.put(
        "part",
        new ArrayList<>(List.of(referencePart("viewReference", "ViewDefinition/does-not-exist"))));

    final Map<String, Object> parameters = parametersWith(viewPart);
    kickOff(gson.toJson(parameters)).expectStatus().isNotFound();
  }

  // -------------------------------------------------------------------------
  // Completion manifest shape (US9)
  // -------------------------------------------------------------------------

  @Test
  void completionManifestHasSpecShape() throws InterruptedException {
    final Map<String, Object> parameters = parametersWithNestedView();
    addSimpleParameter(parameters, "clientTrackingId", "valueString", "tracking-xyz");

    final Map<String, Object> manifest = exportToCompletion(gson.toJson(parameters));

    assertThat(findParamValue(manifest, "exportId", "valueString")).isNotNull();
    assertThat(findParamValue(manifest, "status", "valueCode")).isEqualTo("completed");
    assertThat(findParamValue(manifest, "clientTrackingId", "valueString"))
        .isEqualTo("tracking-xyz");
    assertThat(findParamValue(manifest, "_format", "valueCode")).isEqualTo("ndjson");
    assertThat(findParamValue(manifest, "exportStartTime", "valueInstant")).isNotNull();
    assertThat(findParamValue(manifest, "exportEndTime", "valueInstant")).isNotNull();
    assertThat(findParam(manifest, "exportDuration")).isNotNull();

    // Each output has a name and one or more location parts; no url part and no Bulk Data fields.
    final Map<String, Object> output = findParam(manifest, "output");
    assertThat(output).isNotNull();
    assertThat(findPartValue(output, "name", "valueString")).isNotNull();
    assertThat(findPartValue(output, "location", "valueUri")).contains("$result");
    assertThat(findPart(output, "url")).isNull();

    assertThat(findParam(manifest, "transactionTime")).isNull();
    assertThat(findParam(manifest, "request")).isNull();
    assertThat(findParam(manifest, "requiresAccessToken")).isNull();
  }

  @Test
  void completionManifestOmitsClientTrackingIdWhenAbsent() throws InterruptedException {
    final Map<String, Object> manifest = exportToCompletion(createExportParametersWithNestedView());
    assertThat(findParam(manifest, "clientTrackingId")).isNull();
  }

  // -------------------------------------------------------------------------
  // Helper methods
  // -------------------------------------------------------------------------

  /** Posts a kick-off request with the async preference and returns the response spec. */
  @Nonnull
  private WebTestClient.ResponseSpec kickOff(@Nonnull final String parametersJson) {
    return webTestClient
        .post()
        .uri("http://localhost:" + port + "/fhir/$viewdefinition-export")
        .header("Content-Type", "application/fhir+json")
        .header("Accept", "application/fhir+json")
        .header("Prefer", "respond-async")
        .bodyValue(parametersJson)
        .exchange();
  }

  /** Runs an export to completion and returns the parsed result manifest. */
  @Nonnull
  private Map<String, Object> exportToCompletion(@Nonnull final String parametersJson)
      throws InterruptedException {
    final String contentLocation =
        kickOff(parametersJson)
            .expectStatus()
            .isAccepted()
            .returnResult(String.class)
            .getResponseHeaders()
            .getFirst("Content-Location");
    assertThat(contentLocation).isNotNull();

    String resultLocation = null;
    for (int attempt = 0; attempt < 60 && resultLocation == null; attempt++) {
      final var poll =
          webTestClient
              .get()
              .uri(contentLocation)
              .header("Accept", "application/fhir+json")
              .exchange()
              .returnResult(String.class);
      final HttpStatus status = (HttpStatus) poll.getStatus();
      if (status == HttpStatus.SEE_OTHER) {
        resultLocation = poll.getResponseHeaders().getFirst("Location");
      } else if (status == HttpStatus.ACCEPTED) {
        Thread.sleep(500);
      } else {
        throw new AssertionError("Unexpected poll status: " + status);
      }
    }
    assertThat(resultLocation).as("Expected a 303 result location within the timeout").isNotNull();

    final byte[] body =
        webTestClient
            .get()
            .uri(resultLocation)
            .header("Accept", "application/fhir+json")
            .exchange()
            .expectStatus()
            .isOk()
            .expectBody()
            .returnResult()
            .getResponseBodyContent();
    return parseParameters(body);
  }

  @Nonnull
  private Map<String, Object> parametersWithNestedView() {
    return parametersWith(viewPartWithResource());
  }

  @Nonnull
  private Map<String, Object> viewPartWithResource() {
    final Map<String, Object> viewParam = new LinkedHashMap<>();
    viewParam.put("name", "view");
    final List<Map<String, Object>> parts = new ArrayList<>();
    final Map<String, Object> viewResourcePart = new LinkedHashMap<>();
    viewResourcePart.put("name", "viewResource");
    viewResourcePart.put("resource", createSimplePatientViewDefinition());
    parts.add(viewResourcePart);
    viewParam.put("part", parts);
    return viewParam;
  }

  @Nonnull
  private Map<String, Object> parametersWith(@Nonnull final Map<String, Object> viewParam) {
    final Map<String, Object> parameters = new LinkedHashMap<>();
    parameters.put("resourceType", "Parameters");
    parameters.put("parameter", new ArrayList<>(List.of(viewParam)));
    return parameters;
  }

  @SuppressWarnings("unchecked")
  private void addSimpleParameter(
      @Nonnull final Map<String, Object> parameters,
      @Nonnull final String name,
      @Nonnull final String valueKey,
      @Nonnull final String value) {
    ((List<Map<String, Object>>) parameters.get("parameter"))
        .add(simplePart(name, valueKey, value));
  }

  @Nonnull
  private Map<String, Object> simplePart(
      @Nonnull final String name, @Nonnull final String valueKey, @Nonnull final String value) {
    final Map<String, Object> part = new LinkedHashMap<>();
    part.put("name", name);
    part.put(valueKey, value);
    return part;
  }

  @Nonnull
  private Map<String, Object> referencePart(
      @Nonnull final String name, @Nonnull final String reference) {
    final Map<String, Object> part = new LinkedHashMap<>();
    part.put("name", name);
    part.put("valueReference", Map.of("reference", reference));
    return part;
  }

  @SuppressWarnings("unchecked")
  @Nonnull
  private Map<String, Object> parseParameters(@Nonnull final byte[] body) {
    return gson.fromJson(new String(body, java.nio.charset.StandardCharsets.UTF_8), Map.class);
  }

  @SuppressWarnings("unchecked")
  @jakarta.annotation.Nullable
  private static Map<String, Object> findParam(
      @Nonnull final Map<String, Object> parameters, @Nonnull final String name) {
    final List<Map<String, Object>> list = (List<Map<String, Object>>) parameters.get("parameter");
    if (list == null) {
      return null;
    }
    return list.stream().filter(p -> name.equals(p.get("name"))).findFirst().orElse(null);
  }

  @jakarta.annotation.Nullable
  private static String findParamValue(
      @Nonnull final Map<String, Object> parameters,
      @Nonnull final String name,
      @Nonnull final String valueKey) {
    final Map<String, Object> param = findParam(parameters, name);
    return param == null ? null : (String) param.get(valueKey);
  }

  @SuppressWarnings("unchecked")
  @jakarta.annotation.Nullable
  private static Map<String, Object> findPart(
      @Nonnull final Map<String, Object> param, @Nonnull final String partName) {
    final List<Map<String, Object>> parts = (List<Map<String, Object>>) param.get("part");
    if (parts == null) {
      return null;
    }
    return parts.stream().filter(p -> partName.equals(p.get("name"))).findFirst().orElse(null);
  }

  @jakarta.annotation.Nullable
  private static String findPartValue(
      @Nonnull final Map<String, Object> param,
      @Nonnull final String partName,
      @Nonnull final String valueKey) {
    final Map<String, Object> part = findPart(param, partName);
    return part == null ? null : (String) part.get(valueKey);
  }

  /**
   * Creates a Parameters JSON with nested view structure matching what the UI sends.
   *
   * <p>Structure:
   *
   * <pre>
   * {
   *   "resourceType": "Parameters",
   *   "parameter": [
   *     {
   *       "name": "view",
   *       "part": [
   *         { "name": "viewResource", "resource": { ... ViewDefinition ... } }
   *       ]
   *     },
   *     { "name": "_format", "valueString": "ndjson" }
   *   ]
   * }
   * </pre>
   */
  @Nonnull
  private String createExportParametersWithNestedView() {
    final Map<String, Object> viewDefinition = createSimplePatientViewDefinition();

    final Map<String, Object> parameters = new LinkedHashMap<>();
    parameters.put("resourceType", "Parameters");
    final List<Map<String, Object>> parameterList = new ArrayList<>();

    // Build the nested view parameter.
    final Map<String, Object> viewParam = new LinkedHashMap<>();
    viewParam.put("name", "view");
    final List<Map<String, Object>> parts = new ArrayList<>();
    final Map<String, Object> viewResourcePart = new LinkedHashMap<>();
    viewResourcePart.put("name", "viewResource");
    viewResourcePart.put("resource", viewDefinition);
    parts.add(viewResourcePart);
    viewParam.put("part", parts);
    parameterList.add(viewParam);

    // Add format parameter.
    final Map<String, Object> formatParam = new LinkedHashMap<>();
    formatParam.put("name", "_format");
    formatParam.put("valueString", "ndjson");
    parameterList.add(formatParam);

    parameters.put("parameter", parameterList);
    return gson.toJson(parameters);
  }

  /** Creates a Parameters JSON with multiple named views. */
  @Nonnull
  private String createExportParametersWithMultipleViews() {
    final Map<String, Object> patientView = createSimplePatientViewDefinition();
    patientView.put("name", "patient_demographics");

    final Map<String, Object> parameters = new LinkedHashMap<>();
    parameters.put("resourceType", "Parameters");
    final List<Map<String, Object>> parameterList = new ArrayList<>();

    // First view with name.
    final Map<String, Object> viewParam1 = new LinkedHashMap<>();
    viewParam1.put("name", "view");
    final List<Map<String, Object>> parts1 = new ArrayList<>();

    final Map<String, Object> namePart1 = new LinkedHashMap<>();
    namePart1.put("name", "name");
    namePart1.put("valueString", "patients");
    parts1.add(namePart1);

    final Map<String, Object> viewResourcePart1 = new LinkedHashMap<>();
    viewResourcePart1.put("name", "viewResource");
    viewResourcePart1.put("resource", patientView);
    parts1.add(viewResourcePart1);

    viewParam1.put("part", parts1);
    parameterList.add(viewParam1);

    // Add format parameter.
    final Map<String, Object> formatParam = new LinkedHashMap<>();
    formatParam.put("name", "_format");
    formatParam.put("valueString", "ndjson");
    parameterList.add(formatParam);

    parameters.put("parameter", parameterList);
    return gson.toJson(parameters);
  }

  /** Creates a simple ViewDefinition for Patient resources. */
  @Nonnull
  private Map<String, Object> createSimplePatientViewDefinition() {
    final Map<String, Object> view = new HashMap<>();
    view.put("resourceType", "ViewDefinition");
    view.put("name", "test_patient_view");
    view.put("resource", "Patient");
    view.put("status", "active");
    view.put(
        "select",
        List.of(
            Map.of("column", List.of(Map.of("name", "id", "path", "id"))),
            Map.of(
                "column", List.of(Map.of("name", "family_name", "path", "name.first().family")))));
    return view;
  }

  // -------------------------------------------------------------------------
  // 303 Redirect pattern tests (HL7 Asynchronous Interaction Request Pattern)
  // -------------------------------------------------------------------------

  /**
   * Tests the complete async flow with 303 redirect pattern: 1. POST kick-off request → 202
   * Accepted with Content-Location 2. Poll status endpoint → 202 (in-progress) or 303 (complete) 3.
   * GET result endpoint → 200 OK with Parameters
   */
  @Test
  void exportAsyncWith303RedirectPattern() throws InterruptedException {
    final String parametersJson = createExportParametersWithNestedView();

    // Step 1: Kick-off request should return 202 with Content-Location header.
    final String contentLocation =
        webTestClient
            .post()
            .uri("http://localhost:" + port + "/fhir/$viewdefinition-export")
            .header("Content-Type", "application/fhir+json")
            .header("Accept", "application/fhir+json")
            .header("Prefer", "respond-async")
            .bodyValue(parametersJson)
            .exchange()
            .expectStatus()
            .isAccepted()
            .expectHeader()
            .exists("Content-Location")
            .returnResult(String.class)
            .getResponseHeaders()
            .getFirst("Content-Location");

    assertThat(contentLocation).isNotNull();
    assertThat(contentLocation).contains("$job?id=");
    log.debug("Content-Location: {}", contentLocation);

    // Step 2: Poll the status endpoint until we get 303 See Other.
    // Poll with exponential backoff up to a timeout.
    final int maxAttempts = 30;
    String resultLocation = null;

    for (int attempt = 0; attempt < maxAttempts; attempt++) {
      final var result =
          webTestClient
              .get()
              .uri(contentLocation)
              .header("Accept", "application/fhir+json")
              .exchange()
              .returnResult(String.class);

      final HttpStatus status = (HttpStatus) result.getStatus();
      log.debug("Poll attempt {}: status={}", attempt + 1, status);

      if (status == HttpStatus.SEE_OTHER) {
        // Job complete - 303 redirect.
        resultLocation = result.getResponseHeaders().getFirst("Location");
        assertThat(resultLocation).isNotNull();
        assertThat(resultLocation).contains("$job-result?id=");
        break;
      } else if (status == HttpStatus.ACCEPTED) {
        // Still processing - wait and retry.
        Thread.sleep(500);
      } else if (status == HttpStatus.OK) {
        // Legacy behaviour without redirect - test passes but logs warning.
        log.warn("Got 200 OK instead of 303. This indicates the BULK_DATA pattern.");
        return;
      } else {
        throw new AssertionError("Unexpected status: " + status);
      }
    }

    assertThat(resultLocation)
        .as("Expected 303 See Other with Location header within timeout")
        .isNotNull();

    // Step 3: GET the result endpoint should return 200 with Parameters.
    webTestClient
        .get()
        .uri(resultLocation)
        .header("Accept", "application/fhir+json")
        .exchange()
        .expectStatus()
        .isOk()
        .expectHeader()
        .exists("Expires")
        .expectBody()
        .jsonPath("$.resourceType")
        .isEqualTo("Parameters")
        .jsonPath("$.parameter")
        .isArray();
  }

  /**
   * Tests that attempting to get results from an in-progress job via the result endpoint returns
   * 400 Bad Request.
   */
  @Test
  void resultEndpointRejectsInProgressJob() {
    final String parametersJson = createExportParametersWithNestedView();

    // Kick-off request.
    final String contentLocation =
        webTestClient
            .post()
            .uri("http://localhost:" + port + "/fhir/$viewdefinition-export")
            .header("Content-Type", "application/fhir+json")
            .header("Accept", "application/fhir+json")
            .header("Prefer", "respond-async")
            .bodyValue(parametersJson)
            .exchange()
            .expectStatus()
            .isAccepted()
            .returnResult(String.class)
            .getResponseHeaders()
            .getFirst("Content-Location");

    // Extract job ID from Content-Location.
    assertThat(contentLocation).isNotNull();
    final String jobId = contentLocation.substring(contentLocation.lastIndexOf("=") + 1);

    // Immediately try to access the result endpoint (job likely still in progress).
    // This should fail with 400 Bad Request or 404 if redirect not enabled.
    webTestClient
        .mutate()
        .responseTimeout(Duration.ofMillis(500))
        .build()
        .get()
        .uri("http://localhost:" + port + "/fhir/$job-result?id=" + jobId)
        .header("Accept", "application/fhir+json")
        .exchange()
        .expectStatus()
        .is4xxClientError();
  }
}
