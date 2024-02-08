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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpHeaders;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.json.JSONObject;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import wiremock.net.minidev.json.JSONArray;

class BulkExportServiceTest {


  @Test
  void computeTimeToSleep() {
    final BulkExportTemplate bulkExportTemplate = new BulkExportTemplate(
        Mockito.mock(HttpClient.class), URI.create("http://example.com"));

    assertEquals(bulkExportTemplate.minPoolingTimeout,
        bulkExportTemplate.computeTimeToSleep(Optional.empty(), Duration.ZERO));

    assertEquals(bulkExportTemplate.minPoolingTimeout,
        bulkExportTemplate.computeTimeToSleep(Optional.of(Duration.ZERO), Duration.ZERO));

    assertEquals(bulkExportTemplate.minPoolingTimeout,
        bulkExportTemplate.computeTimeToSleep(Optional.of(Duration.ofSeconds(5)), Duration.ZERO));

    assertEquals(Duration.ofSeconds(5),
        bulkExportTemplate.computeTimeToSleep(Optional.of(Duration.ofSeconds(5)),
            Duration.ofSeconds(100)));

    assertEquals(bulkExportTemplate.maxPoolingTimeout,
        bulkExportTemplate.computeTimeToSleep(Optional.of(Duration.ofSeconds(15)),
            Duration.ofSeconds(100)));

    assertEquals(Duration.ofSeconds(5),
        bulkExportTemplate.computeTimeToSleep(Optional.of(Duration.ofSeconds(15)),
            Duration.ofSeconds(5)));
  }


  @Test
  void testDefaultRequestUri() throws Exception {
    final URI baseUri = URI.create("http://example.com/fhir");
    assertEquals(URI.create("http://example.com/fhir?_outputFormat=ndjson&_type="),
        BulkExportTemplate.toRequestURI(baseUri, BulkExportRequest.builder().build())
    );
  }

  @Test
  void testNonDefaultRequestUri() throws Exception {
    final URI baseUri = URI.create("http://test.com/fhir");
    final Instant testInstant = Instant.parse("2023-01-11T00:00:00.1234Z");
    assertEquals(URI.create(
            "http://test.com/fhir?_outputFormat=xml&_type=Patient%2CObservation&_since=2023-01-11T00%3A00%3A00.123Z"),
        BulkExportTemplate.toRequestURI(baseUri, BulkExportRequest.builder()
            ._outputFormat("xml")
            ._type(List.of("Patient", "Observation"))
            ._since(testInstant)
            .build())
    );
  }


  private static final JSONObject TRANSIENT_OUTCOME_SINGLE = new JSONObject()
      .put("resourceType", "OperationOutcome")
      .put("issue", new JSONArray().appendElement(
          new JSONObject().put("code", "transient")
      ));


  private static final JSONObject TRANSIENT_OUTCOME_ANY = new JSONObject()
      .put("resourceType", "OperationOutcome")
      .put("issue", new JSONArray()
          .appendElement(new JSONObject().put("code", "other"))
          .appendElement(new JSONObject().put("code", "transient")
          ));

  private static final JSONObject TRANSIENT_OUTCOME_NO_ISSUES = new JSONObject()
      .put("resourceType", "OperationOutcome");

  private static final JSONObject TRANSIENT_OUTCOME_NO_TRANSIENT_ISSUES = new JSONObject()
      .put("resourceType", "OperationOutcome")
      .put("issue", new JSONArray()
          .appendElement(new JSONObject().put("code", "other"))
          .appendElement(new JSONObject().put("code", "fatal")
          ));

  @Test
  void isTransientIfSingleTransientIssue() {

    final HttpResponse<String> response = TestHttpResponse.builder()
        .statusCode(500)
        .headers(
            HttpHeaders.of(Map.of("content-type", List.of("application/json")), (x, y) -> true))
        .body(TRANSIENT_OUTCOME_SINGLE.toString())
        .build();
    assertTrue(BulkExportTemplate.isTransientError(response));
  }

  @Test
  void isTransientIfAnyTransientIssue() {

    final HttpResponse<String> response = TestHttpResponse.builder()
        .statusCode(500)
        .headers(
            HttpHeaders.of(Map.of("content-type", List.of("application/json")), (x, y) -> true))
        .body(TRANSIENT_OUTCOME_ANY.toString())
        .build();
    assertTrue(BulkExportTemplate.isTransientError(response));
  }

  @Test
  void notTransientIfNotJsonContentType() {
    final HttpResponse<String> response = TestHttpResponse.builder()
        .statusCode(500)
        .headers(
            HttpHeaders.of(Map.of("content-type", List.of("application/text")), (x, y) -> true))
        .body(TRANSIENT_OUTCOME_SINGLE.toString())
        .build();
    assertFalse(BulkExportTemplate.isTransientError(response));
  }


  @Test
  void notTransientIfEmptyBody() {
    final HttpResponse<String> response = TestHttpResponse.builder()
        .statusCode(500)
        .headers(
            HttpHeaders.of(Map.of("content-type", List.of("application/json")), (x, y) -> true))
        .body("")
        .build();
    assertFalse(BulkExportTemplate.isTransientError(response));
  }

  @Test
  void notTransientIfNotValidJson() {
    final HttpResponse<String> response = TestHttpResponse.builder()
        .statusCode(500)
        .headers(
            HttpHeaders.of(Map.of("content-type", List.of("application/json")), (x, y) -> true))
        .body("{")
        .build();
    assertFalse(BulkExportTemplate.isTransientError(response));
  }

  @Test
  void notTransientIfNotOperationOutcome() {
    final HttpResponse<String> response = TestHttpResponse.builder()
        .statusCode(500)
        .headers(
            HttpHeaders.of(Map.of("content-type", List.of("application/json")), (x, y) -> true))
        .body("{}")
        .build();
    assertFalse(BulkExportTemplate.isTransientError(response));
  }

  @Test
  void notTransientIfNoTransientIssues() {
    final HttpResponse<String> response = TestHttpResponse.builder()
        .statusCode(500)
        .headers(
            HttpHeaders.of(Map.of("content-type", List.of("application/json")), (x, y) -> true))
        .body(TRANSIENT_OUTCOME_NO_TRANSIENT_ISSUES.toString())
        .build();
    assertFalse(BulkExportTemplate.isTransientError(response));
  }

  @Test
  void notTransientIfNoIssues() {
    final HttpResponse<String> response = TestHttpResponse.builder()
        .statusCode(500)
        .headers(
            HttpHeaders.of(Map.of("content-type", List.of("application/json")), (x, y) -> true))
        .body(TRANSIENT_OUTCOME_NO_ISSUES.toString())
        .build();
    assertFalse(BulkExportTemplate.isTransientError(response));
  }
}
