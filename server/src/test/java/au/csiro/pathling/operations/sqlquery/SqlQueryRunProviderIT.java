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

package au.csiro.pathling.operations.sqlquery;

import static au.csiro.pathling.operations.sqlquery.SqlLibraryParser.LIBRARY_TYPE_SYSTEM;
import static au.csiro.pathling.operations.sqlquery.SqlLibraryParser.SQL_QUERY_TYPE_CODE;
import static org.assertj.core.api.Assertions.assertThat;

import ca.uhn.fhir.context.FhirContext;
import ca.uhn.fhir.parser.IParser;
import com.google.gson.Gson;
import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import lombok.extern.slf4j.Slf4j;
import org.hl7.fhir.r4.model.Attachment;
import org.hl7.fhir.r4.model.CodeableConcept;
import org.hl7.fhir.r4.model.Coding;
import org.hl7.fhir.r4.model.Enumerations.PublicationStatus;
import org.hl7.fhir.r4.model.Library;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.ResourceAccessMode;
import org.junit.jupiter.api.parallel.ResourceLock;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.http.MediaType;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.test.web.reactive.server.EntityExchangeResult;
import org.springframework.test.web.reactive.server.WebTestClient;

/**
 * Integration tests for {@link SqlQueryRunProvider} and {@link SqlQueryInstanceRunProvider} —
 * exercises the full {@code $sqlquery-run} pipeline through HTTP, including the system-level,
 * type-level, and instance-level entry points and the validation error paths.
 *
 * <p>Tests use SQL that does not depend on registered ViewDefinitions ({@code SELECT ... FROM
 * (VALUES ...)}) so they are not affected by the Delta-Lake table-list caching that disables some
 * of the {@link au.csiro.pathling.operations.view.ViewDefinitionRunProvider} integration tests.
 */
@Slf4j
@Tag("IntegrationTest")
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@ResourceLock(value = "wiremock", mode = ResourceAccessMode.READ_WRITE)
@ActiveProfiles({"integration-test"})
class SqlQueryRunProviderIT {

  private static final Gson GSON = new Gson();

  private static final String SELF_CONTAINED_SQL =
      "SELECT * FROM (VALUES (1, 'alice'), (2, 'bob')) AS t(id, name)";

  @LocalServerPort int port;

  @Autowired WebTestClient webTestClient;

  @Autowired private FhirContext fhirContext;

  private IParser jsonParser;

  @DynamicPropertySource
  static void configureProperties(final DynamicPropertyRegistry registry) {
    final Path warehouseDir =
        Path.of("src/test/resources/test-data/bulk/fhir/delta").toAbsolutePath();
    registry.add("pathling.storage.warehouseUrl", () -> "file://" + warehouseDir);
  }

  @BeforeEach
  void setup() {
    webTestClient =
        webTestClient
            .mutate()
            .codecs(configurer -> configurer.defaultCodecs().maxInMemorySize(100 * 1024 * 1024))
            .build();
    jsonParser = fhirContext.newJsonParser();
  }

  @Test
  void systemLevelNdjsonHappyPath() {
    final String parametersJson =
        parametersJson(sqlQueryLibrary(SELF_CONTAINED_SQL), SqlQueryOutputFormat.NDJSON);

    final String body = postOk("/fhir/$sqlquery-run", parametersJson, SqlQueryOutputFormat.NDJSON);

    final String[] lines = body.trim().split("\n");
    assertThat(lines).hasSize(2);
    assertThat(body).contains("\"id\":1").contains("\"name\":\"alice\"");
    assertThat(body).contains("\"id\":2").contains("\"name\":\"bob\"");
  }

  @Test
  void systemLevelCsvWithHeader() {
    final String parametersJson =
        parametersJson(sqlQueryLibrary(SELF_CONTAINED_SQL), SqlQueryOutputFormat.CSV, true);

    final String body = postOk("/fhir/$sqlquery-run", parametersJson, SqlQueryOutputFormat.CSV);

    final String[] lines = body.trim().split("\n");
    assertThat(lines.length).isGreaterThanOrEqualTo(3);
    assertThat(lines[0]).contains("id").contains("name");
    assertThat(lines[1]).contains("alice");
    assertThat(lines[2]).contains("bob");
  }

  @Test
  void systemLevelJsonOutput() {
    final String parametersJson =
        parametersJson(sqlQueryLibrary(SELF_CONTAINED_SQL), SqlQueryOutputFormat.JSON);

    final String body = postOk("/fhir/$sqlquery-run", parametersJson, SqlQueryOutputFormat.JSON);

    assertThat(body.trim()).startsWith("[").endsWith("]");
    assertThat(body).contains("alice").contains("bob");
  }

  @Test
  void typeLevelHappyPath() {
    final String parametersJson =
        parametersJson(sqlQueryLibrary(SELF_CONTAINED_SQL), SqlQueryOutputFormat.NDJSON);

    final String body =
        postOk("/fhir/Library/$sqlquery-run", parametersJson, SqlQueryOutputFormat.NDJSON);

    assertThat(body).contains("alice").contains("bob");
  }

  @Test
  void rejectsRequestWithNeitherQueryResourceNorQueryReference() {
    final Map<String, Object> parameters = new LinkedHashMap<>();
    parameters.put("resourceType", "Parameters");
    parameters.put(
        "parameter",
        List.of(
            Map.of(
                "name", "_format", "valueString", SqlQueryOutputFormat.NDJSON.getContentType())));

    postExpect4xx("/fhir/$sqlquery-run", GSON.toJson(parameters));
  }

  @Test
  void rejectsLibraryWithoutSqlQueryTypeCoding() {
    final Library library = new Library();
    library.setStatus(PublicationStatus.ACTIVE);
    final Attachment content = new Attachment();
    content.setContentType("application/sql");
    content.setData("SELECT 1".getBytes(StandardCharsets.UTF_8));
    library.addContent(content);

    postExpect4xx("/fhir/$sqlquery-run", parametersJson(library, SqlQueryOutputFormat.NDJSON));
  }

  @Test
  void rejectsLibraryWithoutSqlContent() {
    final Library library = sqlQueryLibrary("SELECT 1");
    library.getContent().clear();

    postExpect4xx("/fhir/$sqlquery-run", parametersJson(library, SqlQueryOutputFormat.NDJSON));
  }

  @Test
  void instanceLevelRunWithNonExistentLibraryReturns404() {
    webTestClient
        .post()
        .uri(
            "http://localhost:"
                + port
                + "/fhir/Library/does-not-exist/$sqlquery-run?_format=ndjson")
        .header("Content-Type", "application/fhir+json")
        .header("Accept", SqlQueryOutputFormat.NDJSON.getContentType())
        .bodyValue("{\"resourceType\":\"Parameters\"}")
        .exchange()
        .expectStatus()
        .isNotFound();
  }

  // -------------------------------------------------------------------------
  // source rejection (US1, FR-001) — every level, over POST and GET
  // -------------------------------------------------------------------------

  @Test
  void sourceRejectedAtSystemLevelOverPost() {
    final Map<String, Object> parameters = new LinkedHashMap<>();
    parameters.put("resourceType", "Parameters");
    final Map<String, Object> queryResourceParam = new LinkedHashMap<>();
    queryResourceParam.put("name", "queryResource");
    queryResourceParam.put(
        "resource",
        GSON.fromJson(
            jsonParser.encodeResourceToString(sqlQueryLibrary(SELF_CONTAINED_SQL)), Map.class));
    final Map<String, Object> sourceParam = new LinkedHashMap<>();
    sourceParam.put("name", "source");
    sourceParam.put("valueString", "external");
    parameters.put("parameter", List.of(queryResourceParam, sourceParam));

    sourceRejected("/fhir/$sqlquery-run", GSON.toJson(parameters));
  }

  @Test
  void sourceRejectedAtSystemLevelOverGet() {
    getExpectStatus("/fhir/$sqlquery-run?source=external&_format=ndjson", 400);
  }

  @Test
  void sourceRejectedAtTypeLevelOverGet() {
    getExpectStatus("/fhir/Library/$sqlquery-run?source=external&_format=ndjson", 400);
  }

  @Test
  void sourceRejectedAtInstanceLevelOverGet() {
    // The source rejection precedes the stored-Library read, so it applies even for a missing id.
    getExpectStatus("/fhir/Library/some-id/$sqlquery-run?source=external&_format=ndjson", 400);
  }

  /** Posts a body carrying a source part and asserts a 400 naming source. */
  private void sourceRejected(@Nonnull final String path, @Nonnull final String body) {
    final byte[] content =
        webTestClient
            .post()
            .uri("http://localhost:" + port + path)
            .header("Content-Type", "application/fhir+json")
            .bodyValue(body)
            .exchange()
            .expectStatus()
            .isBadRequest()
            .expectBody()
            .returnResult()
            .getResponseBodyContent();
    assertThat(new String(Objects.requireNonNull(content), StandardCharsets.UTF_8))
        .contains("source");
  }

  // -------------------------------------------------------------------------
  // _format handling (US1, FR-002/FR-003/FR-004)
  // -------------------------------------------------------------------------

  @Test
  void unsupportedExplicitFormatReturns400() {
    final Map<String, Object> parameters = new LinkedHashMap<>();
    parameters.put("resourceType", "Parameters");
    final Map<String, Object> queryResourceParam = new LinkedHashMap<>();
    queryResourceParam.put("name", "queryResource");
    queryResourceParam.put(
        "resource",
        GSON.fromJson(
            jsonParser.encodeResourceToString(sqlQueryLibrary(SELF_CONTAINED_SQL)), Map.class));
    final Map<String, Object> formatParam = new LinkedHashMap<>();
    formatParam.put("name", "_format");
    formatParam.put("valueString", "xml");
    parameters.put("parameter", List.of(queryResourceParam, formatParam));

    final byte[] content =
        webTestClient
            .post()
            .uri("http://localhost:" + port + "/fhir/$sqlquery-run")
            .header("Content-Type", "application/fhir+json")
            .bodyValue(GSON.toJson(parameters))
            .exchange()
            .expectStatus()
            .isBadRequest()
            .expectBody()
            .returnResult()
            .getResponseBodyContent();
    assertThat(new String(Objects.requireNonNull(content), StandardCharsets.UTF_8)).contains("xml");
  }

  @Test
  void acceptHeaderMismatchDefaultsToNdjson() {
    // No _format parameter and an Accept that matches no supported media type: defaults to NDJSON
    // (the explicit-format rejection applies only to _format, not to content negotiation).
    final Map<String, Object> parameters = new LinkedHashMap<>();
    parameters.put("resourceType", "Parameters");
    final Map<String, Object> queryResourceParam = new LinkedHashMap<>();
    queryResourceParam.put("name", "queryResource");
    queryResourceParam.put(
        "resource",
        GSON.fromJson(
            jsonParser.encodeResourceToString(sqlQueryLibrary(SELF_CONTAINED_SQL)), Map.class));
    parameters.put("parameter", List.of(queryResourceParam));

    webTestClient
        .post()
        .uri("http://localhost:" + port + "/fhir/$sqlquery-run")
        .header("Content-Type", "application/fhir+json")
        .header("Accept", "application/xml")
        .bodyValue(GSON.toJson(parameters))
        .exchange()
        .expectStatus()
        .isOk()
        .expectHeader()
        .contentTypeCompatibleWith(MediaType.parseMediaType("application/x-ndjson"));
  }

  // -------------------------------------------------------------------------
  // Conformance canonical (US2, FR-005/FR-006)
  // -------------------------------------------------------------------------

  @Test
  @SuppressWarnings("unchecked")
  void sqlQueryRunDeclaresSpecCanonicalAndForkNotServed() {
    final byte[] metadata =
        webTestClient
            .get()
            .uri("http://localhost:" + port + "/fhir/metadata")
            .header("Accept", "application/fhir+json")
            .exchange()
            .expectStatus()
            .isOk()
            .expectBody()
            .returnResult()
            .getResponseBodyContent();

    final Map<String, Object> capability =
        GSON.fromJson(
            new String(Objects.requireNonNull(metadata), StandardCharsets.UTF_8), Map.class);
    final List<Map<String, Object>> rest = (List<Map<String, Object>>) capability.get("rest");
    final List<Map<String, Object>> operations =
        (List<Map<String, Object>>) rest.get(0).get("operation");

    final String sqlQueryRunDefinition =
        operations.stream()
            .filter(o -> "sqlquery-run".equals(o.get("name")))
            .map(o -> (String) o.get("definition"))
            .findFirst()
            .orElse(null);
    assertThat(sqlQueryRunDefinition)
        .isEqualTo("http://sql-on-fhir.org/OperationDefinition/$sqlquery-run");

    // The Pathling-authored sqlquery-run OperationDefinition is no longer served.
    getExpectStatus("/fhir/OperationDefinition/sqlquery-run", 404);
  }

  // -------------------------------------------------------------------------
  // 422 for an unmappable FHIR column type (US11, FR-036/FR-037)
  // -------------------------------------------------------------------------

  @Test
  void fhirFormatWithUnmappableColumnTypeReturns422() {
    // _format=fhir requires every column to map to a FHIR value; an array column cannot, so the
    // shared streaming helper rejects it up front with 422 before any bytes are written. This
    // regression test guards the existing behaviour in ResultStreamingHelper.
    final String arrayColumnSql = "SELECT 1 AS id, array(1, 2, 3) AS arr";

    final Map<String, Object> parameters = new LinkedHashMap<>();
    parameters.put("resourceType", "Parameters");
    final Map<String, Object> queryResourceParam = new LinkedHashMap<>();
    queryResourceParam.put("name", "queryResource");
    queryResourceParam.put(
        "resource",
        GSON.fromJson(
            jsonParser.encodeResourceToString(sqlQueryLibrary(arrayColumnSql)), Map.class));
    final Map<String, Object> formatParam = new LinkedHashMap<>();
    formatParam.put("name", "_format");
    formatParam.put("valueString", "fhir");
    parameters.put("parameter", List.of(queryResourceParam, formatParam));
    final String body = GSON.toJson(parameters);

    webTestClient
        .post()
        .uri("http://localhost:" + port + "/fhir/$sqlquery-run")
        .header("Content-Type", "application/fhir+json")
        .header("Accept", "application/fhir+json")
        .bodyValue(body)
        .exchange()
        .expectStatus()
        .isEqualTo(422);

    // The same query in a flat format (NDJSON) succeeds.
    final Map<String, Object> ndjsonParameters = new LinkedHashMap<>();
    ndjsonParameters.put("resourceType", "Parameters");
    final Map<String, Object> ndjsonQueryParam = new LinkedHashMap<>();
    ndjsonQueryParam.put("name", "queryResource");
    ndjsonQueryParam.put(
        "resource",
        GSON.fromJson(
            jsonParser.encodeResourceToString(sqlQueryLibrary(arrayColumnSql)), Map.class));
    ndjsonParameters.put("parameter", List.of(ndjsonQueryParam));

    postOk("/fhir/$sqlquery-run", GSON.toJson(ndjsonParameters), SqlQueryOutputFormat.NDJSON);
  }

  /** Issues a GET and asserts the given status code. */
  private void getExpectStatus(@Nonnull final String path, final int status) {
    webTestClient
        .get()
        .uri("http://localhost:" + port + path)
        .header("Accept", "application/fhir+json")
        .exchange()
        .expectStatus()
        .isEqualTo(status);
  }

  @Nonnull
  private String postOk(
      @Nonnull final String path,
      @Nonnull final String body,
      @Nonnull final SqlQueryOutputFormat format) {
    final String contentType = format.getContentType();
    final EntityExchangeResult<byte[]> result =
        webTestClient
            .post()
            .uri("http://localhost:" + port + path)
            .header("Content-Type", "application/fhir+json")
            .header("Accept", contentType)
            .bodyValue(body)
            .exchange()
            .expectStatus()
            .isOk()
            .expectHeader()
            .contentTypeCompatibleWith(MediaType.parseMediaType(contentType))
            .expectBody()
            .returnResult();
    return new String(
        Objects.requireNonNull(result.getResponseBodyContent()), StandardCharsets.UTF_8);
  }

  private void postExpect4xx(@Nonnull final String path, @Nonnull final String body) {
    webTestClient
        .post()
        .uri("http://localhost:" + port + path)
        .header("Content-Type", "application/fhir+json")
        .header("Accept", SqlQueryOutputFormat.NDJSON.getContentType())
        .bodyValue(body)
        .exchange()
        .expectStatus()
        .is4xxClientError();
  }

  @Nonnull
  private Library sqlQueryLibrary(@Nonnull final String sql) {
    final Library library = new Library();
    library.setStatus(PublicationStatus.ACTIVE);
    library.setType(
        new CodeableConcept()
            .addCoding(new Coding().setSystem(LIBRARY_TYPE_SYSTEM).setCode(SQL_QUERY_TYPE_CODE)));
    final Attachment content = new Attachment();
    content.setContentType("application/sql");
    content.setData(sql.getBytes(StandardCharsets.UTF_8));
    library.addContent(content);
    return library;
  }

  @Nonnull
  private String parametersJson(
      @Nonnull final Library library, @Nonnull final SqlQueryOutputFormat format) {
    return parametersJson(library, format, null);
  }

  @Nonnull
  private String parametersJson(
      @Nonnull final Library library,
      @Nonnull final SqlQueryOutputFormat format,
      @Nullable final Boolean includeHeader) {
    final String libraryJson = jsonParser.encodeResourceToString(library);
    final Map<String, Object> parameters = new LinkedHashMap<>();
    parameters.put("resourceType", "Parameters");
    final List<Map<String, Object>> parameterList = new ArrayList<>();

    final Map<String, Object> queryResourceParam = new LinkedHashMap<>();
    queryResourceParam.put("name", "queryResource");
    queryResourceParam.put("resource", GSON.fromJson(libraryJson, Map.class));
    parameterList.add(queryResourceParam);

    final Map<String, Object> formatParam = new LinkedHashMap<>();
    formatParam.put("name", "_format");
    formatParam.put("valueString", format.getCode());
    parameterList.add(formatParam);

    if (includeHeader != null) {
      final Map<String, Object> headerParam = new LinkedHashMap<>();
      headerParam.put("name", "header");
      headerParam.put("valueBoolean", includeHeader);
      parameterList.add(headerParam);
    }

    parameters.put("parameter", parameterList);
    return GSON.toJson(parameters);
  }
}
