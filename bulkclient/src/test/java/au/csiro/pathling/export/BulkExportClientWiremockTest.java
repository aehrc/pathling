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

import static com.github.tomakehurst.wiremock.client.WireMock.aResponse;
import static com.github.tomakehurst.wiremock.client.WireMock.anyUrl;
import static com.github.tomakehurst.wiremock.client.WireMock.get;
import static com.github.tomakehurst.wiremock.client.WireMock.stubFor;
import static com.github.tomakehurst.wiremock.client.WireMock.urlPathEqualTo;
import static com.github.tomakehurst.wiremock.stubbing.Scenario.STARTED;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import au.csiro.pathling.export.BulkExportException.HttpError;
import com.github.tomakehurst.wiremock.junit5.WireMockRuntimeInfo;
import com.github.tomakehurst.wiremock.junit5.WireMockTest;
import com.google.common.base.Charsets;
import java.io.File;
import java.nio.file.Path;
import java.util.UUID;
import javax.annotation.Nonnull;
import org.apache.commons.io.FileUtils;
import org.json.JSONObject;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Disabled;
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


  @Nonnull
  File getRandomExportLocation() {
    return Path.of("target", String.format("bulkexport-%s", UUID.randomUUID())).toFile();
  }

  @Test
  void testExport(@Nonnull final WireMockRuntimeInfo wmRuntimeInfo) throws Exception {

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
        .build()
        .export();

    assertEquals(RESOURCE_00,
        FileUtils.readFileToString(new File(exportDir, "Patient_0000.ndjson"), Charsets.UTF_8));
    assertEquals(RESOURCE_01,
        FileUtils.readFileToString(new File(exportDir, "Condition_0000.ndjson"), Charsets.UTF_8));
    assertEquals(RESOURCE_02,
        FileUtils.readFileToString(new File(exportDir, "Condition_0001.ndjson"), Charsets.UTF_8));
  }

  @Test
  void testExportRetriesTransientErrorsInStatusPooling(
      @Nonnull final WireMockRuntimeInfo wmRuntimeInfo)
      throws Exception {

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
        .whenScenarioStateIs("in-progress")
        .willReturn(aResponse().withStatus(202))
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
  }

  @Test
  @Disabled
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
    assertEquals(500, ex.statusCode);
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
  }
}
