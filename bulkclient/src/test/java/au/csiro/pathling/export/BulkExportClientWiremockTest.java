/*
 * Copyright 2023 Commonwealth Scientific and Industrial Research
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

package au.csiro.pathling.export;

import static au.csiro.pathling.test.TestResources.getResourceAsString;
import static com.github.tomakehurst.wiremock.client.WireMock.aResponse;
import static com.github.tomakehurst.wiremock.client.WireMock.and;
import static com.github.tomakehurst.wiremock.client.WireMock.anyUrl;
import static com.github.tomakehurst.wiremock.client.WireMock.containing;
import static com.github.tomakehurst.wiremock.client.WireMock.equalTo;
import static com.github.tomakehurst.wiremock.client.WireMock.equalToJson;
import static com.github.tomakehurst.wiremock.client.WireMock.get;
import static com.github.tomakehurst.wiremock.client.WireMock.getRequestedFor;
import static com.github.tomakehurst.wiremock.client.WireMock.post;
import static com.github.tomakehurst.wiremock.client.WireMock.postRequestedFor;
import static com.github.tomakehurst.wiremock.client.WireMock.stubFor;
import static com.github.tomakehurst.wiremock.client.WireMock.urlEqualTo;
import static com.github.tomakehurst.wiremock.client.WireMock.urlPathEqualTo;
import static com.github.tomakehurst.wiremock.client.WireMock.verify;
import static com.github.tomakehurst.wiremock.stubbing.Scenario.STARTED;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import au.csiro.pathling.config.AuthConfiguration;
import au.csiro.pathling.export.BulkExportException.HttpError;
import au.csiro.pathling.export.fhir.Reference;
import au.csiro.pathling.export.ws.AssociatedData;
import au.csiro.pathling.export.ws.BulkExportRequest;
import com.github.tomakehurst.wiremock.junit5.WireMockRuntimeInfo;
import com.github.tomakehurst.wiremock.junit5.WireMockTest;
import com.google.common.base.Charsets;
import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.UUID;
import javax.annotation.Nonnull;
import org.apache.commons.io.FileUtils;
import org.json.JSONObject;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import wiremock.net.minidev.json.JSONArray;

@WireMockTest
class BulkExportClientWiremockTest {

  public static final String RESOURCE_00 = "{}\n{}";
  public static final String RESOURCE_01 = "{}\n{}\n{}";
  public static final String RESOURCE_02 = "{}";
  public static final String FAILURE_OPERATION_OUTCOME = new JSONObject()
      .put("resourceType", "OperationOutcome")
      .put("issue", new JSONArray().appendElement(
          new JSONObject().put("code", "failure")
      ))
      .toString();
  public static final String TRANSIENT_ISSUE_OPERATION_OUTCOME = new JSONObject()
      .put("resourceType", "OperationOutcome")
      .put("issue", new JSONArray().appendElement(
          new JSONObject().put("code", "transient")
      ))
      .toString();
  public static final String BULK_EXPORT_NO_FILES_RESPONSE = new JSONObject()
      .put("transactionTime", 4934344343L)
      .put("request", "http://localhost:8080/$export")
      .put("requiresAccessToken", false)
      .put("output", new JSONArray())
      .toString();

  public static String bulkExportResponse_3_files(
      @Nonnull final WireMockRuntimeInfo wmRuntimeInfo) {
    return new JSONObject()
        .put("transactionTime", "4934344343")
        .put("request", "http://localhost:8080/$export")
        .put("requiresAccessToken", false)
        .put("output", new JSONArray()
            .appendElement(new JSONObject()
                .put("type", "Patient")
                .put("url", wmRuntimeInfo.getHttpBaseUrl() + "/file/00")
                .put("count", 2)
            )
            .appendElement(new JSONObject()
                .put("type", "Condition")
                .put("url", wmRuntimeInfo.getHttpBaseUrl() + "/file/01")
                .put("count", 3)
            )
            .appendElement(new JSONObject()
                .put("type", "Condition")
                .put("url", wmRuntimeInfo.getHttpBaseUrl() + "/file/02")
                .put("count", 1)
            ))
        .toString();
  }


  public static String bulkExportResponse_1_file(@Nonnull final WireMockRuntimeInfo wmRuntimeInfo) {
    return new JSONObject()
        .put("transactionTime", "1970-02-27T02:39:04.343Z")
        .put("request", "http://localhost:8080/$export")
        .put("requiresAccessToken", false)
        .put("output", new JSONArray()
            .appendElement(new JSONObject()
                .put("type", "Patient")
                .put("url", wmRuntimeInfo.getHttpBaseUrl() + "/file/00")
                .put("count", 2)
            ))
        .toString();
  }

  @Nonnull
  File getRandomExportLocation() {
    return Path.of("target", String.format("bulkexport-%s", UUID.randomUUID())).toFile();
  }


  static void assertMarkedSuccess(@Nonnull final File location) {
    assertTrue(new File(location, "_SUCCESS").exists());
  }

  static void assertNotMarkedSuccess(@Nonnull final File location) {
    assertFalse(new File(location, "_SUCCESS").exists());
  }

  @Test
  void testSystemLevelExport(@Nonnull final WireMockRuntimeInfo wmRuntimeInfo) throws Exception {

    stubFor(get(anyUrl()).willReturn(aResponse().withStatus(500)));

    stubFor(get(urlEqualTo(
        "/$export?_outputFormat=application%2Ffhir%2Bndjson&_type=Patient%2CCondition"
            + "&includeAssociatedData=LatestProvenanceResources"))
        .inScenario("bulk-export")
        .whenScenarioStateIs(STARTED)
        .willReturn(
            aResponse().withStatus(202)
                .withHeader("content-location", wmRuntimeInfo.getHttpBaseUrl() + "/pool"))
        .willSetStateTo("in-progress")
    );

    stubFor(get(urlPathEqualTo("/pool"))
        .inScenario("bulk-export")
        .whenScenarioStateIs("in-progress")
        .willReturn(aResponse().withStatus(202))
        .willSetStateTo("done")
    );

    stubFor(get(urlPathEqualTo("/pool"))
        .inScenario("bulk-export")
        .whenScenarioStateIs("done")
        .willReturn(aResponse()
            .withStatus(200)
            .withBody(bulkExportResponse_3_files(wmRuntimeInfo))
        )
    );

    stubFor(get(urlPathEqualTo("/file/00"))
        .willReturn(aResponse()
            .withStatus(200)
            .withBody(RESOURCE_00))
    );

    stubFor(get(urlPathEqualTo("/file/01"))
        .willReturn(aResponse()
            .withStatus(200)
            .withBody(RESOURCE_01))
    );

    stubFor(get(urlPathEqualTo("/file/02"))
        .willReturn(aResponse()
            .withStatus(200)
            .withBody(RESOURCE_02))
    );

    System.out.println("Base URL: " + wmRuntimeInfo.getHttpBaseUrl());
    final File exportDir = getRandomExportLocation();
    System.out.println("Exporting to: " + exportDir);

    final String bulkExportDemoServerEndpoint = wmRuntimeInfo.getHttpBaseUrl();
    BulkExportClient.builder()
        .withFhirEndpointUrl(bulkExportDemoServerEndpoint)
        .withOutputDir(exportDir.getPath())
        .withTypes(List.of("Patient", "Condition"))
        .withIncludeAssociatedData(List.of(AssociatedData.LATEST_PROVENANCE_RESOURCES))
        .build()
        .export();

    assertMarkedSuccess(exportDir);
    assertEquals(RESOURCE_00,
        FileUtils.readFileToString(new File(exportDir, "Patient.0000.ndjson"), Charsets.UTF_8));
    assertEquals(RESOURCE_01,
        FileUtils.readFileToString(new File(exportDir, "Condition.0000.ndjson"), Charsets.UTF_8));
    assertEquals(RESOURCE_02,
        FileUtils.readFileToString(new File(exportDir, "Condition.0001.ndjson"), Charsets.UTF_8));
  }

  @Test
  void testGroupLevelExportWithPatientReferences(@Nonnull final WireMockRuntimeInfo wmRuntimeInfo)
      throws Exception {

    stubFor(get(anyUrl()).willReturn(aResponse().withStatus(500)));
    stubFor(post(urlPathEqualTo("/Group/123/$export"))
        .inScenario("bulk-export")
        .withRequestBody(equalToJson(new JSONObject()
            .put("resourceType", "Parameters")
            .put("parameter", new JSONArray()
                .appendElement(new JSONObject()
                    .put("name", "_outputFormat")
                    .put("valueString", "application/fhir+ndjson")
                )
                .appendElement(new JSONObject()
                    .put("name", "_type")
                    .put("valueString", "Patient,Condition")
                )
                .appendElement(new JSONObject()
                    .put("name", "patient")
                    .put("valueReference", new JSONObject()
                        .put("reference", "Patient/123")
                    )
                )
            ).toString(), true, true))
        .whenScenarioStateIs(STARTED)
        .willReturn(
            aResponse().withStatus(202)
                .withHeader("content-location", wmRuntimeInfo.getHttpBaseUrl() + "/pool"))
        .willSetStateTo("in-progress")
    );

    stubFor(get(urlPathEqualTo("/pool"))
        .inScenario("bulk-export")
        .whenScenarioStateIs("in-progress")
        .willReturn(aResponse().withStatus(202))
        .willSetStateTo("done")
    );

    stubFor(get(urlPathEqualTo("/pool"))
        .inScenario("bulk-export")
        .whenScenarioStateIs("done")
        .willReturn(aResponse()
            .withStatus(200)
            .withBody(bulkExportResponse_1_file(wmRuntimeInfo))
        )
    );

    stubFor(get(urlPathEqualTo("/file/00"))
        .willReturn(aResponse()
            .withStatus(200)
            .withBody(RESOURCE_00))
    );

    System.out.println("Base URL: " + wmRuntimeInfo.getHttpBaseUrl());
    final File exportDir = getRandomExportLocation();
    System.out.println("Exporting to: " + exportDir);

    final String bulkExportDemoServerEndpoint = wmRuntimeInfo.getHttpBaseUrl();
    final BulkExportResult result = BulkExportClient.groupBuilder("123")
        .withFhirEndpointUrl(bulkExportDemoServerEndpoint)
        .withOutputDir(exportDir.getPath())
        .withPatient(Reference.of("Patient/123"))
        .withType("Patient")
        .withType("Condition")
        .build()
        .export();

    assertMarkedSuccess(exportDir);
    assertEquals(RESOURCE_00,
        FileUtils.readFileToString(new File(exportDir, "Patient.0000.ndjson"), Charsets.UTF_8));

    assertEquals(BulkExportResult.of(Instant.parse("1970-02-27T02:39:04.343Z"), List.of(
                BulkExportResult.FileResult.of(
                    URI.create(wmRuntimeInfo.getHttpBaseUrl() + "/file/00"),
                    URI.create(exportDir.toURI() + "Patient.0000.ndjson"), 5)
            )
        ),
        result);
  }


  @Test
  void testPatientLevelExportWithNoPatientReferences(
      @Nonnull final WireMockRuntimeInfo wmRuntimeInfo)
      throws Exception {

    stubFor(get(anyUrl()).willReturn(aResponse().withStatus(500)));
    stubFor(get(urlEqualTo("/Patient/$export?_outputFormat=application%2Ffhir%2Bndjson"))
        .inScenario("bulk-export")
        .whenScenarioStateIs(STARTED)
        .willReturn(
            aResponse().withStatus(202)
                .withHeader("content-location", wmRuntimeInfo.getHttpBaseUrl() + "/pool"))
        .willSetStateTo("in-progress")
    );

    stubFor(get(urlPathEqualTo("/pool"))
        .inScenario("bulk-export")
        .whenScenarioStateIs("in-progress")
        .willReturn(aResponse().withStatus(202))
        .willSetStateTo("done")
    );

    stubFor(get(urlPathEqualTo("/pool"))
        .inScenario("bulk-export")
        .whenScenarioStateIs("done")
        .willReturn(aResponse()
            .withStatus(200)
            .withBody(bulkExportResponse_1_file(wmRuntimeInfo))
        )
    );

    stubFor(get(urlPathEqualTo("/file/00"))
        .willReturn(aResponse()
            .withStatus(200)
            .withBody(RESOURCE_00))
    );

    System.out.println("Base URL: " + wmRuntimeInfo.getHttpBaseUrl());
    final File exportDir = getRandomExportLocation();
    System.out.println("Exporting to: " + exportDir);

    final String bulkExportDemoServerEndpoint = wmRuntimeInfo.getHttpBaseUrl();
    BulkExportClient.builder()
        .withFhirEndpointUrl(bulkExportDemoServerEndpoint)
        .withOutputDir(exportDir.getPath())
        .withLevel(new BulkExportRequest.PatientLevel())
        .build()
        .export();

    assertMarkedSuccess(exportDir);
    assertEquals(RESOURCE_00,
        FileUtils.readFileToString(new File(exportDir, "Patient.0000.ndjson"), Charsets.UTF_8));
  }

  @Test
  void testExportRetriesTransientErrorsInStatusPooling(
      @Nonnull final WireMockRuntimeInfo wmRuntimeInfo) {

    stubFor(get(anyUrl()).willReturn(aResponse().withStatus(500)));

    stubFor(get(urlPathEqualTo("/$export"))
        .inScenario("bulk-export")
        .whenScenarioStateIs(STARTED)
        .willReturn(
            aResponse().withStatus(202)
                .withHeader("content-location", wmRuntimeInfo.getHttpBaseUrl() + "/pool"))
        .willSetStateTo("transient-error")
    );

    stubFor(get(urlPathEqualTo("/pool"))
        .inScenario("bulk-export")
        .whenScenarioStateIs("transient-error")
        .willReturn(aResponse().withStatus(500)
            .withHeader("content-type", "application/json")
            .withBody(TRANSIENT_ISSUE_OPERATION_OUTCOME))
        .willSetStateTo("done")
    );

    stubFor(get(urlPathEqualTo("/pool"))
        .inScenario("bulk-export")
        .whenScenarioStateIs("done")
        .willReturn(aResponse()
            .withStatus(200)
            .withHeader("content-type", "application/json")
            .withBody(BULK_EXPORT_NO_FILES_RESPONSE))
    );

    System.out.println("Base URL: " + wmRuntimeInfo.getHttpBaseUrl());
    final File exportDir = getRandomExportLocation();
    System.out.println("Exporting to: " + exportDir);

    final String bulkExportDemoServerEndpoint = wmRuntimeInfo.getHttpBaseUrl();
    BulkExportClient.builder()
        .withFhirEndpointUrl(bulkExportDemoServerEndpoint)
        .withOutputDir(exportDir.getPath())
        .build()
        .export();

    assertMarkedSuccess(exportDir);
  }


  @Test
  void testExportRetriesTooManyRequest429SatusInPooling(
      @Nonnull final WireMockRuntimeInfo wmRuntimeInfo) {

    stubFor(get(anyUrl()).willReturn(aResponse().withStatus(500)));

    stubFor(get(urlPathEqualTo("/$export"))
        .inScenario("bulk-export")
        .whenScenarioStateIs(STARTED)
        .willReturn(
            aResponse().withStatus(202)
                .withHeader("content-location", wmRuntimeInfo.getHttpBaseUrl() + "/pool"))
        .willSetStateTo("too-many-requests")
    );

    stubFor(get(urlPathEqualTo("/pool"))
        .inScenario("bulk-export")
        .whenScenarioStateIs("too-many-requests")
        .willReturn(aResponse().withStatus(429).withHeader("retry-after", "3"))
        .willSetStateTo("done")
    );

    stubFor(get(urlPathEqualTo("/pool"))
        .inScenario("bulk-export")
        .whenScenarioStateIs("done")
        .willReturn(aResponse()
            .withStatus(200)
            .withHeader("content-type", "application/json")
            .withBody(BULK_EXPORT_NO_FILES_RESPONSE))
    );

    System.out.println("Base URL: " + wmRuntimeInfo.getHttpBaseUrl());
    final File exportDir = getRandomExportLocation();
    System.out.println("Exporting to: " + exportDir);

    final String bulkExportDemoServerEndpoint = wmRuntimeInfo.getHttpBaseUrl();
    BulkExportClient.builder()
        .withFhirEndpointUrl(bulkExportDemoServerEndpoint)
        .withOutputDir(exportDir.getPath())
        .build()
        .export();

    assertMarkedSuccess(exportDir);
  }

  @Test
  void testExportFailOnErrorsInStatusPooling(
      @Nonnull final WireMockRuntimeInfo wmRuntimeInfo) {

    stubFor(get(anyUrl()).willReturn(aResponse().withStatus(500)));

    stubFor(get(urlPathEqualTo("/$export"))
        .inScenario("bulk-export")
        .whenScenarioStateIs(STARTED)
        .willReturn(
            aResponse().withStatus(202)
                .withHeader("content-location", wmRuntimeInfo.getHttpBaseUrl() + "/pool"))
        .willSetStateTo("transient-error")
    );

    stubFor(get(urlPathEqualTo("/pool"))
        .willReturn(aResponse().withStatus(500)
            .withHeader("content-type", "application/json")
            .withBody(FAILURE_OPERATION_OUTCOME))
    );

    final File exportDir = getRandomExportLocation();
    final String bulkExportDemoServerEndpoint = wmRuntimeInfo.getHttpBaseUrl();

    final HttpError ex = Assertions.assertThrows(HttpError.class, () ->
        BulkExportClient.builder()
            .withFhirEndpointUrl(bulkExportDemoServerEndpoint)
            .withOutputDir(exportDir.getPath())
            .build()
            .export()
    );
    assertEquals(500, ex.statusCode);
    assertNotMarkedSuccess(exportDir);
  }

  @Test
  void testExportFailOnPersistingTransientErrorsInStatusPooling(
      @Nonnull final WireMockRuntimeInfo wmRuntimeInfo) {

    stubFor(get(anyUrl()).willReturn(aResponse().withStatus(500)));

    stubFor(get(urlPathEqualTo("/$export"))
        .inScenario("bulk-export")
        .whenScenarioStateIs(STARTED)
        .willReturn(
            aResponse().withStatus(202)
                .withHeader("content-location", wmRuntimeInfo.getHttpBaseUrl() + "/pool"))
        .willSetStateTo("transient-error")
    );

    stubFor(get(urlPathEqualTo("/pool"))
        .willReturn(aResponse().withStatus(500)
            .withHeader("content-type", "application/json")
            .withBody(TRANSIENT_ISSUE_OPERATION_OUTCOME))
    );

    final File exportDir = getRandomExportLocation();
    final String bulkExportDemoServerEndpoint = wmRuntimeInfo.getHttpBaseUrl();

    final HttpError ex = Assertions.assertThrows(HttpError.class, () ->
        BulkExportClient.builder()
            .withFhirEndpointUrl(bulkExportDemoServerEndpoint)
            .withOutputDir(exportDir.getPath())
            .build()
            .export()
    );

    // default retry of 4 + 1
    verify(4, getRequestedFor(urlPathEqualTo("/pool")));

    assertEquals(500, ex.statusCode);
    assertNotMarkedSuccess(exportDir);
  }

  @Test
  void testExportFailOnIfOutputLocationExists(
      @Nonnull final WireMockRuntimeInfo wmRuntimeInfo) {

    final File exportDir = getRandomExportLocation();
    final String bulkExportDemoServerEndpoint = wmRuntimeInfo.getHttpBaseUrl();
    assertTrue(exportDir.mkdirs());

    final BulkExportException ex = Assertions.assertThrows(BulkExportException.class, () ->
        BulkExportClient.builder()
            .withFhirEndpointUrl(bulkExportDemoServerEndpoint)
            .withOutputDir(exportDir.getPath())
            .build()
            .export()
    );
    assertEquals("Destination directory already exists: " + exportDir.getPath(), ex.getMessage());
    assertNotMarkedSuccess(exportDir);
  }


  @Test
  void testExportFailsOnDownloadError(@Nonnull final WireMockRuntimeInfo wmRuntimeInfo) {

    stubFor(get(anyUrl()).willReturn(aResponse().withStatus(500)));
    stubFor(get(urlPathEqualTo("/$export"))
        .inScenario("bulk-export")
        .whenScenarioStateIs(STARTED)
        .willReturn(
            aResponse().withStatus(202)
                .withHeader("content-location", wmRuntimeInfo.getHttpBaseUrl() + "/pool"))
        .willSetStateTo("in-progress")
    );

    stubFor(get(urlPathEqualTo("/pool"))
        .inScenario("bulk-export")
        .whenScenarioStateIs("in-progress")
        .willReturn(aResponse().withStatus(202))
        .willSetStateTo("done")
    );

    stubFor(get(urlPathEqualTo("/pool"))
        .inScenario("bulk-export")
        .whenScenarioStateIs("done")
        .willReturn(aResponse().withStatus(200).withBody(
            new JSONObject()
                .put("transactionTime", 4934344343L)
                .put("request", "http://localhost:8080/$export")
                .put("requiresAccessToken", false)
                .put("output", new JSONArray()
                    .appendElement(new JSONObject()
                        .put("type", "Patient")
                        .put("url", wmRuntimeInfo.getHttpBaseUrl() + "/file/00")
                        .put("count", 2)
                    )
                    .appendElement(new JSONObject()
                        .put("type", "Condition")
                        .put("url", wmRuntimeInfo.getHttpBaseUrl() + "/file/01")
                        .put("count", 3)
                    )
                    .appendElement(new JSONObject()
                        .put("type", "Condition")
                        .put("url", wmRuntimeInfo.getHttpBaseUrl() + "/file/02")
                        .put("count", 1)
                    )
                )
                .toString()
        ))
    );

    stubFor(get(urlPathEqualTo("/file/00"))
        .willReturn(aResponse()
            .withStatus(500))
    );

    stubFor(get(urlPathEqualTo("/file/01"))
        .willReturn(aResponse()
            .withFixedDelay(2_000)
            .withStatus(200)
            .withBody(RESOURCE_01))
    );

    stubFor(get(urlPathEqualTo("/file/02"))
        .willReturn(aResponse()
            .withStatus(200)
            .withBody(RESOURCE_02))
    );

    System.out.println("Base URL: " + wmRuntimeInfo.getHttpBaseUrl());
    final File exportDir = getRandomExportLocation();
    System.out.println("Exporting to: " + exportDir);

    final String bulkExportDemoServerEndpoint = wmRuntimeInfo.getHttpBaseUrl();

    final BulkExportException.DownloadError ex = Assertions.assertThrows(
        BulkExportException.DownloadError.class, () ->
            BulkExportClient.builder()
                .withFhirEndpointUrl(bulkExportDemoServerEndpoint)
                .withOutputDir(exportDir.getPath())
                .build()
                .export()
    );
    assertEquals("Download failed", ex.getMessage());
    assertEquals(String.format("Failed to download: %s/file/00: [statusCode: 500]",
            wmRuntimeInfo.getHttpBaseUrl()),
        ex.getCause().getMessage());
    assertNotMarkedSuccess(exportDir);
  }


  @Test
  void testExportFailsTimeOutInDownload(@Nonnull final WireMockRuntimeInfo wmRuntimeInfo) {

    stubFor(get(anyUrl()).willReturn(aResponse().withStatus(500)));
    stubFor(get(urlPathEqualTo("/$export"))
        .inScenario("bulk-export")
        .whenScenarioStateIs(STARTED)
        .willReturn(
            aResponse().withStatus(202)
                .withHeader("content-location", wmRuntimeInfo.getHttpBaseUrl() + "/pool"))
        .willSetStateTo("in-progress")
    );

    stubFor(get(urlPathEqualTo("/pool"))
        .inScenario("bulk-export")
        .whenScenarioStateIs("in-progress")
        .willReturn(aResponse().withStatus(202))
        .willSetStateTo("done")
    );

    stubFor(get(urlPathEqualTo("/pool"))
        .inScenario("bulk-export")
        .whenScenarioStateIs("done")
        .willReturn(aResponse().withStatus(200).withBody(
            new JSONObject()
                .put("transactionTime", 4934344343L)
                .put("request", "http://localhost:8080/$export")
                .put("requiresAccessToken", false)
                .put("output", new JSONArray()
                    .appendElement(new JSONObject()
                        .put("type", "Patient")
                        .put("url", wmRuntimeInfo.getHttpBaseUrl() + "/file/00")
                        .put("count", 2)
                    )
                    .appendElement(new JSONObject()
                        .put("type", "Condition")
                        .put("url", wmRuntimeInfo.getHttpBaseUrl() + "/file/01")
                        .put("count", 3)
                    )
                    .appendElement(new JSONObject()
                        .put("type", "Condition")
                        .put("url", wmRuntimeInfo.getHttpBaseUrl() + "/file/02")
                        .put("count", 1)
                    )
                )
                .toString()
        ))
    );

    stubFor(get(urlPathEqualTo("/file/00"))
        .willReturn(aResponse()
            .withFixedDelay(2_000)
            .withStatus(200)
            .withBody(RESOURCE_00))
    );

    stubFor(get(urlPathEqualTo("/file/01"))
        .willReturn(aResponse()
            .withFixedDelay(2_000)
            .withStatus(200)
            .withBody(RESOURCE_01))
    );

    stubFor(get(urlPathEqualTo("/file/02"))
        .willReturn(aResponse()
            .withStatus(200)
            .withBody(RESOURCE_02))
    );

    System.out.println("Base URL: " + wmRuntimeInfo.getHttpBaseUrl());
    final File exportDir = getRandomExportLocation();
    System.out.println("Exporting to: " + exportDir);

    final String bulkExportDemoServerEndpoint = wmRuntimeInfo.getHttpBaseUrl();

    final BulkExportException.Timeout ex = Assertions.assertThrows(
        BulkExportException.Timeout.class, () ->
            BulkExportClient.builder()
                .withFhirEndpointUrl(bulkExportDemoServerEndpoint)
                .withOutputDir(exportDir.getPath())
                .withTimeout(Duration.ofSeconds(3))
                .build()
                .export()
    );
    assertTrue(ex.getMessage().startsWith("Download timed out at:"));
    assertNotMarkedSuccess(exportDir);
  }

  @Test
  void testExportWorksWithSMARTSymmetricAuthenticationForKickOffAndDownload(
      @Nonnull final WireMockRuntimeInfo wmRuntimeInfo) throws IOException {

    stubFor(get(anyUrl()).willReturn(aResponse().withStatus(401)));

    stubFor(get(urlPathEqualTo("/.well-known/smart-configuration"))
        .willReturn(
            aResponse().withStatus(200)
                .withHeader("content-type", "application/json")
                .withBody(new JSONObject()
                    .put("token_endpoint", wmRuntimeInfo.getHttpBaseUrl() + "/token")
                    .put("capabilities",
                        new JSONArray().appendElement("client-confidential-symmetric"))
                    .toString())
        )
    );

    stubFor(post(urlPathEqualTo("/token"))
        .withBasicAuth("client_id", "client_secret")
        .withRequestBody(equalTo("grant_type=client_credentials&scope=*.read"))
        .willReturn(
            aResponse().withStatus(200)
                .withHeader("content-type", "application/json")
                .withBody(new JSONObject()
                    .put("access_token", "token-value")
                    .put("expires_in", 300)
                    .toString())
        )
    );

    stubFor(get(urlPathEqualTo("/$export"))
        .withHeader("Authorization", equalTo("Bearer token-value"))
        .willReturn(
            aResponse().withStatus(202)
                .withHeader("content-location", wmRuntimeInfo.getHttpBaseUrl() + "/pool"))
    );

    stubFor(get(urlPathEqualTo("/pool"))
        .withHeader("Authorization", equalTo("Bearer token-value"))
        .willReturn(aResponse()
            .withStatus(200)
            .withHeader("content-type", "application/json")
            .withBody(bulkExportResponse_1_file(wmRuntimeInfo)))
    );

    stubFor(get(urlPathEqualTo("/file/00"))
        .withHeader("Authorization", equalTo("Bearer token-value"))
        .willReturn(aResponse()
            .withStatus(200)
            .withBody(RESOURCE_00))
    );

    System.out.println("Base URL: " + wmRuntimeInfo.getHttpBaseUrl());
    final File exportDir = getRandomExportLocation();
    System.out.println("Exporting to: " + exportDir);

    final String bulkExportDemoServerEndpoint = wmRuntimeInfo.getHttpBaseUrl();

    final AuthConfiguration authConfig = AuthConfiguration.builder()
        .enabled(true)
        .clientId("client_id")
        .clientSecret("client_secret")
        .scope("*.read")
        .build();

    BulkExportClient.builder()
        .withFhirEndpointUrl(bulkExportDemoServerEndpoint)
        .withOutputDir(exportDir.getPath())
        .withAuthConfig(authConfig)
        .build()
        .export();

    assertMarkedSuccess(exportDir);
    assertEquals(RESOURCE_00,
        FileUtils.readFileToString(new File(exportDir, "Patient.0000.ndjson"), Charsets.UTF_8));

    // The token should be requested once and reused for all requests
    verify(1, postRequestedFor(urlPathEqualTo("/token")));
  }

  @Test
  void testExportWorksWithSMARTSymmetricAuthenticationForKickOffAndDownloadRefresingExpiredTokens(
      @Nonnull final WireMockRuntimeInfo wmRuntimeInfo) throws IOException {

    stubFor(get(anyUrl()).willReturn(aResponse().withStatus(401)));

    stubFor(get(urlPathEqualTo("/.well-known/smart-configuration"))
        .willReturn(
            aResponse().withStatus(200)
                .withHeader("content-type", "application/json")
                .withBody(new JSONObject()
                    .put("token_endpoint", wmRuntimeInfo.getHttpBaseUrl() + "/token-asym")
                    .put("capabilities",
                        new JSONArray().appendElement("client-confidential-asymmetric "))
                    .toString())
        )
    );

    stubFor(post(urlPathEqualTo("/token-asym"))
        .withRequestBody(and(
            containing("grant_type=client_credentials"),
            containing("scope=*.read"),
            containing(
                "client_assertion_type=urn%3Aietf%3Aparams%3Aoauth%3Aclient-assertion-type%3Ajwt-bearer"),
            containing("client_assertion=")
        ))
        .willReturn(
            aResponse().withStatus(200)
                .withHeader("content-type", "application/json")
                .withBody(new JSONObject()
                    .put("access_token", "token-value-asym")
                    .put("expires_in", 0)
                    .toString())
        )
    );

    stubFor(get(urlPathEqualTo("/$export"))
        .withHeader("Authorization", equalTo("Bearer token-value-asym"))
        .willReturn(
            aResponse().withStatus(202)
                .withHeader("content-location", wmRuntimeInfo.getHttpBaseUrl() + "/pool"))
    );

    stubFor(get(urlPathEqualTo("/pool"))
        .withHeader("Authorization", equalTo("Bearer token-value-asym"))
        .willReturn(aResponse()
            .withStatus(200)
            .withHeader("content-type", "application/json")
            .withBody(bulkExportResponse_1_file(wmRuntimeInfo)))
    );

    stubFor(get(urlPathEqualTo("/file/00"))
        .withHeader("Authorization", equalTo("Bearer token-value-asym"))
        .willReturn(aResponse()
            .withStatus(200)
            .withBody(RESOURCE_00))
    );

    System.out.println("Base URL: " + wmRuntimeInfo.getHttpBaseUrl());
    final File exportDir = getRandomExportLocation();
    System.out.println("Exporting to: " + exportDir);

    final String bulkExportDemoServerEndpoint = wmRuntimeInfo.getHttpBaseUrl();

    final AuthConfiguration authConfig = AuthConfiguration.builder()
        .enabled(true)
        .clientId("client_id")
        .privateKeyJWK(getResourceAsString("auth/bulk_rs384_priv_jwk.json"))
        .scope("*.read")
        .tokenExpiryTolerance(0)
        .build();

    BulkExportClient.builder()
        .withFhirEndpointUrl(bulkExportDemoServerEndpoint)
        .withOutputDir(exportDir.getPath())
        .withAuthConfig(authConfig)
        .build()
        .export();

    assertMarkedSuccess(exportDir);
    assertEquals(RESOURCE_00,
        FileUtils.readFileToString(new File(exportDir, "Patient.0000.ndjson"), Charsets.UTF_8));

    // The token should be requested for all three requests (kickoff, status pooling, and download)
    verify(3, postRequestedFor(urlPathEqualTo("/token-asym")));
  }

}
