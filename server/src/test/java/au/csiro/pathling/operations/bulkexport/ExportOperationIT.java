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

package au.csiro.pathling.operations.bulkexport;

import static au.csiro.pathling.util.ExportOperationUtil.doPolling;
import static au.csiro.pathling.util.ExportOperationUtil.kickOffRequest;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.fail;
import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import au.csiro.pathling.library.PathlingContext;
import au.csiro.pathling.library.io.sink.FileInformation;
import au.csiro.pathling.library.io.source.DataSourceBuilder;
import au.csiro.pathling.library.io.source.QueryableDataSource;
import au.csiro.pathling.shaded.com.fasterxml.jackson.databind.JsonNode;
import au.csiro.pathling.shaded.com.fasterxml.jackson.databind.ObjectMapper;
import au.csiro.pathling.util.ExportOperationUtil;
import au.csiro.pathling.util.TestDataSetup;
import ca.uhn.fhir.context.FhirContext;
import ca.uhn.fhir.parser.IParser;
import jakarta.annotation.Nullable;
import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.StreamSupport;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.io.FileUtils;
import org.apache.spark.sql.SparkSession;
import org.hl7.fhir.instance.model.api.IBaseResource;
import org.hl7.fhir.r4.model.Encounter;
import org.hl7.fhir.r4.model.Patient;
import org.hl7.fhir.r4.model.Resource;
import org.junit.jupiter.api.AfterAll;
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
import org.springframework.test.web.reactive.server.EntityExchangeResult;
import org.springframework.test.web.reactive.server.WebTestClient;
import org.springframework.web.util.UriComponentsBuilder;

/**
 * @author Felix Naumann
 */
@Slf4j
@Tag("IntegrationTest")
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@ResourceLock(value = "wiremock", mode = ResourceAccessMode.READ_WRITE)
@ActiveProfiles({"integration-test"})
class ExportOperationIT {

  @LocalServerPort int port;

  @Autowired WebTestClient webTestClient;

  @TempDir private static Path warehouseDir;

  @Autowired private TestDataSetup testDataSetup;

  @Autowired private FhirContext fhirContext;

  @Autowired private PathlingContext pathlingContext;

  private IParser parser;

  @SuppressWarnings("unused")
  @Autowired
  private QueryableDataSource deltaLake;

  @SuppressWarnings("unused")
  @Autowired
  private SparkSession sparkSession;

  @SuppressWarnings("unused")
  @Autowired
  private ExportExecutor exportExecutor;

  @DynamicPropertySource
  static void configureProperties(final DynamicPropertyRegistry registry) {
    TestDataSetup.copyTestDataToTempDir(warehouseDir);
    registry.add("pathling.storage.warehouseUrl", () -> "file://" + warehouseDir.toAbsolutePath());
  }

  @BeforeEach
  void setup() {
    parser = fhirContext.newJsonParser();

    webTestClient =
        webTestClient
            .mutate()
            .codecs(configurer -> configurer.defaultCodecs().maxInMemorySize(100 * 1024 * 1024))
            .build(); // 100 MB
  }

  @AfterEach
  void cleanup() throws IOException {
    // Only clean up the jobs directory, preserving the delta tables for reuse.
    final Path jobsDir = warehouseDir.resolve("delta").resolve("jobs");
    if (jobsDir.toFile().exists()) {
      FileUtils.cleanDirectory(jobsDir.toFile());
    }
  }

  @AfterAll
  static void cleanupAll() throws IOException {
    // Clean the entire temp directory before JUnit's @TempDir cleanup runs.
    // This ensures Spark/Delta file handles don't prevent directory deletion.
    FileUtils.cleanDirectory(warehouseDir.toFile());
  }

  @Test
  void testMissingRespondAsyncHeaderLenientRuns() {
    final String uri =
        "http://localhost:"
            + port
            + "/fhir/$export?_outputFormat=application/fhir+ndjson&_since=2017-01-01T00:00:00Z";
    webTestClient
        .get()
        .uri(uri)
        .header("Accept", "application/fhir+json")
        .header("Prefer", "handling=lenient")
        .exchange()
        .expectStatus()
        .is2xxSuccessful();
  }

  @Test
  void testMissingRespondAsyncHeaderStrictReturnsError() {
    final String uri =
        "http://localhost:"
            + port
            + "/fhir/$export?_outputFormat=application/fhir+ndjson&_since=2017-01-01T00:00:00Z";
    webTestClient
        .get()
        .uri(uri)
        .header("Accept", "application/fhir+json")
        .exchange()
        .expectStatus()
        .is4xxClientError()
        .expectBody();
  }

  @Test
  void testCancellingRequestReturns202() {
    final String uri =
        "http://localhost:"
            + port
            + "/fhir/$export?_outputFormat=application/fhir+ndjson&_since=2017-01-01T00:00:00Z";
    final String pollUrl = kickOffRequest(webTestClient, uri);

    // Send a DELETE request after a brief delay to allow the operation to start.
    await().pollDelay(500, TimeUnit.MILLISECONDS).atMost(2, TimeUnit.SECONDS).until(() -> true);

    webTestClient.delete().uri(pollUrl).exchange().expectStatus().isEqualTo(202);
  }

  @Test
  void testPollingCancelledRequestReturns404() {
    final String uri =
        "http://localhost:"
            + port
            + "/fhir/$export?_outputFormat=application/fhir+ndjson&_since=2017-01-02T00:00:00Z";
    final String pollUrl = kickOffRequest(webTestClient, uri);

    // Send DELETE after a brief delay to allow the operation to start.
    await().pollDelay(500, TimeUnit.MILLISECONDS).atMost(2, TimeUnit.SECONDS).until(() -> true);

    webTestClient.delete().uri(pollUrl).exchange().expectStatus().isAccepted();

    // Now wait for the GET to return 404 (polls until condition is met)
    await()
        .atMost(10, TimeUnit.SECONDS)
        .pollInterval(500, TimeUnit.MILLISECONDS)
        .untilAsserted(
            () -> webTestClient.get().uri(pollUrl).exchange().expectStatus().isEqualTo(404));
  }

  @Test
  void testInvalidKickoffRequest() {
    final String uri =
        "http://localhost:"
            + port
            + "/fhir/$export?_outputFormat=application/fhir+ndjson&_since=2017-01-01T00:00:00Z&_type=Patient,Encounter";
    webTestClient
        .get()
        .uri(uri)
        .header("Accept", "INVALID")
        .header("Prefer", "respond-async")
        .exchange()
        .expectStatus()
        .isBadRequest();
  }

  @Test
  void testExportValid() {
    final String uri =
        "http://localhost:"
            + port
            + "/fhir/$export?_outputFormat=application/fhir+ndjson&_since=2017-01-01T00:00:00Z&_type=Patient,Encounter&_elements=identifier,Patient.name,Encounter.subject";
    final String pollUrl = kickOffRequest(webTestClient, uri);
    await()
        .atMost(30, TimeUnit.SECONDS)
        .pollInterval(1, TimeUnit.SECONDS)
        .until(
            () ->
                doPolling(
                    webTestClient,
                    pollUrl,
                    result -> {
                      try {
                        assertNotNull(result.getResponseBody());
                        assertCompleteResult(
                            uri, result.getResponseBody(), result.getResponseHeaders());
                      } catch (final IOException e) {
                        throw new RuntimeException(e);
                      }
                    }));
  }

  @SuppressWarnings("unchecked")
  private void assertCompleteResult(
      final String originalRequestUri, final String responseBody, final HttpHeaders headers)
      throws IOException {
    final ObjectMapper objectMapper = new ObjectMapper();
    final JsonNode node = objectMapper.readTree(responseBody);

    log.trace("Response headers:");
    headers.forEach((name, values) -> log.trace("  {}: {}", name, String.join(", ", values)));

    assertThat(headers).containsKey("Expires");
    assertThat(headers.getFirst("Content-Type")).isNotNull().startsWith("application/fhir+json");

    // Response is a FHIR Parameters resource.
    assertThat(node.get("resourceType").asText()).isEqualTo("Parameters");
    final JsonNode parameters = node.get("parameter");
    assertThat(parameters).isNotNull();

    // Verify required parameters are present.
    assertThat(findParameter(parameters, "transactionTime")).isNotNull();
    assertThat(getParameterStringValue(parameters, "request")).isEqualTo(originalRequestUri);
    assertThat(getParameterBooleanValue(parameters, "requiresAccessToken")).isFalse();

    // Extract output file information from the output parameters.
    final List<FileInformation> actualFileInfos =
        StreamSupport.stream(parameters.spliterator(), false)
            .filter(param -> "output".equals(param.get("name").asText()))
            .map(
                outputParam -> {
                  final JsonNode parts = outputParam.get("part");
                  String type = null;
                  String url = null;
                  for (final JsonNode part : parts) {
                    final String partName = part.get("name").asText();
                    if ("type".equals(partName)) {
                      type =
                          part.has("valueCode")
                              ? part.get("valueCode").asText()
                              : part.get("valueString").asText();
                    } else if ("url".equals(partName)) {
                      url = part.get("valueUri").asText();
                    }
                  }
                  assertNotNull(type);
                  assertNotNull(url);
                  return new FileInformation(type, url);
                })
            .toList();

    assertThat(actualFileInfos).isNotEmpty();

    // Check that both Patient and Encounter resource types are present.
    final Set<String> resourceTypes =
        actualFileInfos.stream()
            .map(FileInformation::fhirResourceType)
            .collect(java.util.stream.Collectors.toSet());
    assertThat(resourceTypes).containsExactlyInAnyOrder("Patient", "Encounter");

    final Map<String, List<? extends IBaseResource>> downloadedResources = new HashMap<>();
    actualFileInfos.forEach(
        fileInfo -> {
          final EntityExchangeResult<byte[]> result =
              webTestClient
                  .get()
                  .uri(fileInfo.absoluteUrl())
                  .exchange()
                  .expectStatus()
                  .isOk()
                  .expectBody()
                  .returnResult();
          final byte[] responseBytes = result.getResponseBodyContent();
          assertThat(responseBytes).isNotNull();
          final String fileContent =
              new String(responseBytes, java.nio.charset.StandardCharsets.UTF_8);
          final List<Resource> resources =
              ExportOperationUtil.parseNdjson(parser, fileContent, fileInfo.fhirResourceType());
          downloadedResources.put(fileInfo.fhirResourceType(), resources);
        });
    assertThat(downloadedResources).isNotEmpty();

    // It's hard to assert that all other columns are not returned. I carefully checked that the
    // test-data (Patient.ndjson and Encounter.ndjson)
    // have a Patient.active, Encounter.reasonCode, Encounter.serviceProvider values. Now it is
    // checked that they
    // are not present in the returned ndjson

    downloadedResources.forEach(
        (string, iBaseResources) -> {
          switch (string) {
            case "Patient" -> {
              final List<Patient> patients = (List<Patient>) iBaseResources;
              patients.forEach(
                  patient -> {
                    assertThat(patient.hasIdentifier()).isTrue();
                    assertThat(patient.hasName()).isTrue();
                    assertThat(patient.hasGender()).isFalse();
                  });
            }
            case "Encounter" -> {
              final List<Encounter> encounters = (List<Encounter>) iBaseResources;
              encounters.forEach(
                  encounter -> {
                    assertThat(encounter.hasSubject()).isTrue();
                    assertThat(encounter.hasReasonCode()).isFalse();
                    assertThat(encounter.hasServiceProvider()).isFalse();
                  });
            }
            default -> //noinspection ResultOfMethodCallIgnored
                fail("Unexpected resource type %s".formatted(string));
          }
        });

    // Also verify that pathling can read in the downloaded ndjson files
    // Usually the user does not have access to the files on the filesystem directly, instead
    // they can request them through the ExportResultProvider where the contents of the files are
    // returned
    // in the request.
    actualFileInfos.forEach(
        fileInfo -> {
          final Map<String, String> queryParams =
              UriComponentsBuilder.fromUriString(fileInfo.absoluteUrl())
                  .build()
                  .getQueryParams()
                  .toSingleValueMap();
          final String fullFilepath =
              warehouseDir
                  .resolve("delta")
                  .resolve("jobs")
                  .resolve(queryParams.get("job"))
                  .resolve(queryParams.get("file"))
                  .toString();
          assertTrue(
              new File(fullFilepath).exists(),
              "Failed to find %s for pathling ndjson input.".formatted(fullFilepath));
          final String parentPath = Paths.get(fullFilepath).getParent().toString();
          assertThatCode(
                  () ->
                      new DataSourceBuilder(pathlingContext)
                          .ndjson(parentPath)
                          .read(fileInfo.fhirResourceType()))
              .doesNotThrowAnyException();
        });
  }

  /** Finds a parameter by name in a FHIR Parameters resource's parameter array. */
  @Nullable
  private JsonNode findParameter(final JsonNode parameters, final String name) {
    for (final JsonNode param : parameters) {
      if (name.equals(param.get("name").asText())) {
        return param;
      }
    }
    return null;
  }

  /** Gets a string value from a named parameter (checks valueUri, valueString, valueCode). */
  @Nullable
  private String getParameterStringValue(final JsonNode parameters, final String name) {
    final JsonNode param = findParameter(parameters, name);
    if (param == null) {
      return null;
    }
    if (param.has("valueUri")) {
      return param.get("valueUri").asText();
    }
    if (param.has("valueString")) {
      return param.get("valueString").asText();
    }
    if (param.has("valueCode")) {
      return param.get("valueCode").asText();
    }
    return null;
  }

  /** Gets a boolean value from a named parameter. */
  private boolean getParameterBooleanValue(final JsonNode parameters, final String name) {
    final JsonNode param = findParameter(parameters, name);
    if (param == null) {
      return false;
    }
    if (param.has("valueBoolean")) {
      return param.get("valueBoolean").asBoolean();
    }
    return false;
  }

  // _typeFilter integration tests.

  /**
   * Kicks off an export request using the URI builder to properly handle URL encoding of
   * _typeFilter values (which contain '?' and '=' characters).
   */
  private String kickOffExportWithTypeFilter(
      final String basePath, final String typeFilter, @Nullable final String type) {
    final String pollUrl =
        webTestClient
            .get()
            .uri(
                uriBuilder -> {
                  uriBuilder
                      .path(basePath)
                      .queryParam("_outputFormat", "application/fhir+ndjson")
                      .queryParam("_since", "2017-01-01T00:00:00Z")
                      .queryParam("_typeFilter", typeFilter);
                  if (type != null) {
                    uriBuilder.queryParam("_type", type);
                  }
                  return uriBuilder.build();
                })
            .header("Accept", "application/fhir+json")
            .header("Prefer", "respond-async")
            .exchange()
            .expectStatus()
            .is2xxSuccessful()
            .expectHeader()
            .exists("Content-Location")
            .returnResult(String.class)
            .getResponseHeaders()
            .getFirst("Content-Location");
    assertNotNull(pollUrl);
    return pollUrl;
  }

  @Test
  void testExportWithTypeFilterOnSystemLevel() {
    // A system-level export with _typeFilter should only return resources matching the search
    // criteria. Using gender=male to filter Patient resources.
    final String pollUrl =
        kickOffExportWithTypeFilter("/fhir/$export", "Patient?gender=male", "Patient");
    await()
        .atMost(30, TimeUnit.SECONDS)
        .pollInterval(1, TimeUnit.SECONDS)
        .until(
            () ->
                doPolling(
                    webTestClient,
                    pollUrl,
                    result -> {
                      assertNotNull(result.getResponseBody());
                      assertTypeFilterResult(result.getResponseBody(), "Patient", "male");
                    }));
  }

  @Test
  void testExportWithTypeFilterOnPatientLevel() {
    // A patient-level export with _typeFilter should also filter resources appropriately.
    final String pollUrl =
        kickOffExportWithTypeFilter("/fhir/Patient/$export", "Patient?gender=male", "Patient");
    await()
        .atMost(30, TimeUnit.SECONDS)
        .pollInterval(1, TimeUnit.SECONDS)
        .until(
            () ->
                doPolling(
                    webTestClient,
                    pollUrl,
                    result -> {
                      assertNotNull(result.getResponseBody());
                      assertTypeFilterResult(result.getResponseBody(), "Patient", "male");
                    }));
  }

  @Test
  void testExportWithTypeFilterInvalidFormatReturnsError() {
    // A _typeFilter without a '?' separator should be rejected in strict mode.
    webTestClient
        .get()
        .uri(
            uriBuilder ->
                uriBuilder
                    .path("/fhir/$export")
                    .queryParam("_outputFormat", "application/fhir+ndjson")
                    .queryParam("_since", "2017-01-01T00:00:00Z")
                    .queryParam("_typeFilter", "InvalidFormat")
                    .build())
        .header("Accept", "application/fhir+json")
        .header("Prefer", "respond-async")
        .exchange()
        .expectStatus()
        .isBadRequest();
  }

  @Test
  void testExportWithTypeFilterInvalidFormatLenientAlsoFails() {
    // Format validation (missing '?') is always strict, even in lenient mode.
    webTestClient
        .get()
        .uri(
            uriBuilder ->
                uriBuilder
                    .path("/fhir/$export")
                    .queryParam("_outputFormat", "application/fhir+ndjson")
                    .queryParam("_since", "2017-01-01T00:00:00Z")
                    .queryParam("_typeFilter", "InvalidFormat")
                    .build())
        .header("Accept", "application/fhir+json")
        .header("Prefer", "respond-async, handling=lenient")
        .exchange()
        .expectStatus()
        .isBadRequest();
  }

  @Test
  void testExportWithTypeFilterImplicitTypeInclusion() {
    // When _type is absent but _typeFilter is present, resource types should be implicitly included
    // from the _typeFilter values.
    final String pollUrl =
        kickOffExportWithTypeFilter("/fhir/$export", "Patient?gender=male", null);
    await()
        .atMost(30, TimeUnit.SECONDS)
        .pollInterval(1, TimeUnit.SECONDS)
        .until(
            () ->
                doPolling(
                    webTestClient,
                    pollUrl,
                    result -> {
                      assertNotNull(result.getResponseBody());
                      // Only Patient should be in the output since _typeFilter implicitly sets
                      // the type.
                      assertTypeFilterResult(result.getResponseBody(), "Patient", "male");
                    }));
  }

  /**
   * Asserts that a completed export response contains only the specified resource type and that all
   * Patient resources have the expected gender.
   */
  private void assertTypeFilterResult(
      final String responseBody, final String expectedType, final String expectedGender) {
    try {
      final ObjectMapper objectMapper = new ObjectMapper();
      final JsonNode node = objectMapper.readTree(responseBody);

      assertThat(node.get("resourceType").asText()).isEqualTo("Parameters");
      final JsonNode parameters = node.get("parameter");

      // Extract output file information.
      final List<FileInformation> fileInfos =
          StreamSupport.stream(parameters.spliterator(), false)
              .filter(param -> "output".equals(param.get("name").asText()))
              .map(
                  outputParam -> {
                    final JsonNode parts = outputParam.get("part");
                    String type = null;
                    String url = null;
                    for (final JsonNode part : parts) {
                      final String partName = part.get("name").asText();
                      if ("type".equals(partName)) {
                        type =
                            part.has("valueCode")
                                ? part.get("valueCode").asText()
                                : part.get("valueString").asText();
                      } else if ("url".equals(partName)) {
                        url = part.get("valueUri").asText();
                      }
                    }
                    assertNotNull(type);
                    assertNotNull(url);
                    return new FileInformation(type, url);
                  })
              .toList();

      assertThat(fileInfos).isNotEmpty();
      // All output should be for the expected resource type.
      assertThat(fileInfos).allMatch(fi -> fi.fhirResourceType().equals(expectedType));

      // Download and verify the content.
      for (final FileInformation fileInfo : fileInfos) {
        final EntityExchangeResult<byte[]> fileResult =
            webTestClient
                .get()
                .uri(fileInfo.absoluteUrl())
                .exchange()
                .expectStatus()
                .isOk()
                .expectBody()
                .returnResult();
        final byte[] bytes = fileResult.getResponseBodyContent();
        assertThat(bytes).isNotNull();
        final String content = new String(bytes, java.nio.charset.StandardCharsets.UTF_8);
        final List<Resource> resources =
            ExportOperationUtil.parseNdjson(parser, content, expectedType);
        assertThat(resources).isNotEmpty();
        // Verify all patients have the expected gender.
        for (final Resource resource : resources) {
          assertThat(resource).isInstanceOf(Patient.class);
          assertThat(((Patient) resource).getGender().toCode()).isEqualTo(expectedGender);
        }
      }
    } catch (final IOException e) {
      throw new RuntimeException(e);
    }
  }
}
