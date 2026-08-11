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

package au.csiro.pathling.operations.sql;

import static au.csiro.pathling.operations.sqlquery.SqlLibraryParser.LIBRARY_TYPE_SYSTEM;
import static au.csiro.pathling.operations.sqlquery.SqlLibraryParser.SQL_QUERY_TYPE_CODE;
import static org.assertj.core.api.Assertions.assertThat;

import ca.uhn.fhir.context.FhirContext;
import ca.uhn.fhir.parser.IParser;
import com.google.gson.Gson;
import jakarta.annotation.Nonnull;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.time.Duration;
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
import org.hl7.fhir.r4.model.Patient;
import org.hl7.fhir.r4.model.RelatedArtifact;
import org.hl7.fhir.r4.model.RelatedArtifact.RelatedArtifactType;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.ResourceAccessMode;
import org.junit.jupiter.api.parallel.ResourceLock;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.context.annotation.Import;
import org.springframework.http.MediaType;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.test.web.reactive.server.EntityExchangeResult;
import org.springframework.test.web.reactive.server.WebTestClient;

/**
 * End-to-end integration tests for the system-level {@code $sql-run} operation, driving each way a
 * subject can be supplied over both HTTP verbs.
 *
 * <p>Backed by {@link SqlRunTestConfiguration}, which substitutes an in-memory data source holding
 * the stored ViewDefinition, SQLQuery and SQLView subjects together with the Patient data they
 * project.
 *
 * @author John Grimes
 */
@Slf4j
@Tag("IntegrationTest")
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@ResourceLock(value = "wiremock", mode = ResourceAccessMode.READ_WRITE)
@ActiveProfiles({"integration-test"})
@Import(SqlRunTestConfiguration.class)
class SqlRunProviderIT {

  private static final Gson GSON = new Gson();

  private static final String PATH = "/fhir/$sql-run";

  /** A canonical URL that is not stored on the server, so it must be satisfied by context. */
  private static final String AD_HOC_VIEW_URL = "https://pathling.csiro.au/test/ViewDefinition/Adh";

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
            // Resolving a dependency graph plans a deeper Spark query than the 5s default allows.
            .responseTimeout(Duration.ofSeconds(60))
            .build();
    jsonParser = fhirContext.newJsonParser();
  }

  // -------------------------------------------------------------------------
  // Stored subjects over GET.
  // -------------------------------------------------------------------------

  @Test
  void getsStoredViewByCanonicalAsCsv() {
    // A stored ViewDefinition resolved by canonical URL, rendered as CSV with a header row.
    final String body =
        getOk(
            PATH + "?subjectCanonical=" + SqlRunTestConfiguration.PATIENT_VIEW_URL + "&_format=csv",
            SqlRunFormat.CSV);

    final List<String> lines = body.lines().filter(line -> !line.isBlank()).toList();
    assertThat(lines).hasSize(4);
    assertThat(lines.get(0)).isEqualTo("id,family_name");
    assertThat(lines.subList(1, 4))
        .containsExactlyInAnyOrder("p1,Smith", "p2,Johnson", "p3,Williams");
  }

  @Test
  void getsStoredViewByReferenceWithRepeatedPatientFilter() {
    // A stored ViewDefinition resolved by relative reference, with the projected data restricted to
    // two of the three patients by a repeated patient parameter.
    final String body =
        getOk(
            PATH
                + "?subjectReference=ViewDefinition/"
                + SqlRunTestConfiguration.PATIENT_VIEW_ID
                + "&patient=Patient/p1&patient=Patient/p3&_format=ndjson",
            SqlRunFormat.NDJSON);

    final List<String> lines = body.lines().filter(line -> !line.isBlank()).toList();
    assertThat(lines).hasSize(2);
    assertThat(body).contains("Smith").contains("Williams").doesNotContain("Johnson");
  }

  // -------------------------------------------------------------------------
  // Stored subjects over POST.
  // -------------------------------------------------------------------------

  @Test
  void postsStoredQueryByReferenceWithBoundParameter() {
    // A stored SQLQuery resolved by relative reference, with its declared 'family' parameter bound
    // at request time; only the matching patient is returned.
    final Map<String, Object> parameters = new LinkedHashMap<>();
    parameters.put("resourceType", "Parameters");
    parameters.put(
        "parameter",
        List.of(
            referenceParameter(
                "subjectReference", "Library/" + SqlRunTestConfiguration.PATIENTS_BY_FAMILY_ID),
            stringParameter("_format", SqlRunFormat.NDJSON.getCode()),
            boundParameters("family", "Johnson")));

    final String body = postOk(GSON.toJson(parameters), SqlRunFormat.NDJSON);

    final List<String> lines = body.lines().filter(line -> !line.isBlank()).toList();
    assertThat(lines).hasSize(1);
    assertThat(body).contains("\"family_name\":\"Johnson\"").contains("\"id\":\"p2\"");
  }

  @Test
  void postsStoredSqlViewByCanonical() {
    // A stored SQLView is an admissible subject in its own right, not only as a dependency.
    final Map<String, Object> parameters = new LinkedHashMap<>();
    parameters.put("resourceType", "Parameters");
    parameters.put(
        "parameter",
        List.of(
            stringParameter(
                "subjectCanonical",
                SqlRunTestConfiguration.libraryUrl(SqlRunTestConfiguration.ALL_PATIENTS_ID)),
            stringParameter("_format", SqlRunFormat.NDJSON.getCode())));

    final String body = postOk(GSON.toJson(parameters), SqlRunFormat.NDJSON);

    assertThat(body.lines().filter(line -> !line.isBlank())).hasSize(3);
    assertThat(body).contains("Smith").contains("Johnson").contains("Williams");
  }

  // -------------------------------------------------------------------------
  // Inline subjects.
  // -------------------------------------------------------------------------

  @Test
  void postsInlineQueryWithInlineContext() {
    // An inline SQLQuery depends on a canonical the server does not hold; the request supplies that
    // ViewDefinition inline as context, and the dependency resolves against it.
    final Library library =
        sqlQueryLibrary("SELECT id, family_name FROM adh ORDER BY id", "adh", AD_HOC_VIEW_URL);

    final Map<String, Object> parameters = new LinkedHashMap<>();
    parameters.put("resourceType", "Parameters");
    parameters.put(
        "parameter",
        List.of(
            resourceParameter("subjectResource", jsonParser.encodeResourceToString(library)),
            resourceParameter("context", adHocViewJson()),
            stringParameter("_format", SqlRunFormat.NDJSON.getCode())));

    final String body = postOk(GSON.toJson(parameters), SqlRunFormat.NDJSON);

    assertThat(body.lines().filter(line -> !line.isBlank())).hasSize(3);
    assertThat(body).contains("Smith").contains("Johnson").contains("Williams");
  }

  @Test
  void postsInlineViewOverInlineResourceData() {
    // An inline ViewDefinition projects the resources supplied inline with the request, rather than
    // the data held by the server.
    final Patient inlinePatient = new Patient();
    inlinePatient.setId("inline-1");
    inlinePatient.addName().setFamily("Inline");

    final Map<String, Object> parameters = new LinkedHashMap<>();
    parameters.put("resourceType", "Parameters");
    parameters.put(
        "parameter",
        List.of(
            resourceParameter("subjectResource", adHocViewJson()),
            stringParameter("resource", jsonParser.encodeResourceToString(inlinePatient)),
            stringParameter("_format", SqlRunFormat.NDJSON.getCode())));

    final String body = postOk(GSON.toJson(parameters), SqlRunFormat.NDJSON);

    final List<String> lines = body.lines().filter(line -> !line.isBlank()).toList();
    assertThat(lines).hasSize(1);
    assertThat(body).contains("inline-1").contains("Inline");
    // The server's own patients are not projected.
    assertThat(body).doesNotContain("Smith");
  }

  // -------------------------------------------------------------------------
  // Formats available only to a SQL subject.
  // -------------------------------------------------------------------------

  @Test
  void fhirFormatForSqlSubjectReturnsParametersRows() {
    // The fhir format is available to a SQL subject, and renders each row as a Parameters resource.
    final Library library =
        sqlQueryLibrary("SELECT id, family_name FROM adh ORDER BY id", "adh", AD_HOC_VIEW_URL);

    final Map<String, Object> parameters = new LinkedHashMap<>();
    parameters.put("resourceType", "Parameters");
    parameters.put(
        "parameter",
        List.of(
            resourceParameter("subjectResource", jsonParser.encodeResourceToString(library)),
            resourceParameter("context", adHocViewJson()),
            stringParameter("_format", SqlRunFormat.FHIR.getCode())));

    final String body = postOk(GSON.toJson(parameters), SqlRunFormat.FHIR);

    assertThat(body).contains("Parameters").contains("family_name").contains("Smith");
  }

  // ------------------------------------------------------------------------
  // Parameters that cannot be expressed in a query string.
  // ------------------------------------------------------------------------

  // A resource-carrying parameter has no query-string form, so a GET naming one is refused by the
  // operation itself. Asserted at the wire, because that is the layer where the refusal has to
  // happen: the framework's own binding would otherwise answer first, with a different status.
  @ParameterizedTest
  @ValueSource(strings = {"subjectResource", "parameters", "context", "resource"})
  void rejectsAResourceCarryingParameterOverGet(final String parameterName) {
    final byte[] body =
        webTestClient
            .get()
            .uri(
                "http://localhost:"
                    + port
                    + PATH
                    + "?subjectReference=ViewDefinition/"
                    + SqlRunTestConfiguration.PATIENT_VIEW_ID
                    + "&"
                    + parameterName
                    + "=x")
            .header("Accept", "application/fhir+json")
            .exchange()
            .expectStatus()
            .isBadRequest()
            .expectBody()
            .returnResult()
            .getResponseBodyContent();

    assertThat(new String(body == null ? new byte[0] : body, StandardCharsets.UTF_8))
        .contains("\"code\":\"invalid\"")
        .contains(parameterName);
  }

  // -------------------------------------------------------------------------
  // Analysis failures.
  // -------------------------------------------------------------------------

  // A column that does not exist is a fault Spark's analyser catches, not one SqlValidator does.
  // It is a conformant subject that cannot be processed, so it is a 422 naming the subject, and it
  // carries Spark's own message - including the "did you mean" suggestions, which are what makes
  // the failure actionable rather than opaque.
  @Test
  void reportsAnUnresolvedColumnAsAnUnprocessableSubject() {
    final String body =
        postExpectStatus(
            inlineSqlQueryRequest("SELECT no_such_col FROM adh"), 422, "application/fhir+json");

    assertThat(body)
        .contains("\"code\":\"invalid\"")
        .contains("\"subject\"")
        .contains("UNRESOLVED_COLUMN")
        .contains("no_such_col");
    // The analyser's suggestions name the columns the subject's own dependency declares.
    assertThat(body).contains("family_name");
    // Spark's getMessage would append the unresolved logical plan, naming the internal
    // request-scoped views the dependency graph was materialised under. It is not returned.
    assertThat(body).doesNotContain("SubqueryAlias").doesNotContain("Project [");
  }

  // The unresolved reference is reported whatever wraps it, so a subquery the outer plan never
  // reads from is still analysed rather than being optimised away into a success.
  @Test
  void reportsAnUnresolvedColumnInsideASubquery() {
    final String body =
        postExpectStatus(
            inlineSqlQueryRequest("SELECT * FROM (SELECT no_such_col FROM adh) x LIMIT 0"),
            422,
            "application/fhir+json");

    assertThat(body).contains("\"code\":\"invalid\"").contains("no_such_col");
  }

  // An unknown function name is deliberately left for Spark's analyser by SqlValidator, on the
  // grounds that its message is more helpful than a synthetic rejection. That only holds if the
  // message reaches the caller.
  @Test
  void reportsAnUnknownFunctionAsAnUnprocessableSubject() {
    final String body =
        postExpectStatus(
            inlineSqlQueryRequest("SELECT no_such_fn(id) FROM adh"), 422, "application/fhir+json");

    assertThat(body).contains("\"code\":\"invalid\"").contains("no_such_fn");
  }

  // The faults SqlValidator catches statically keep their existing status. Analysis translation
  // must not pull a parameter-level rejection up or down into a different tier.
  @Test
  void leavesStaticallyDetectedFaultsAsBadRequests() {
    // Malformed SQL, a table the subject never declared, and a disallowed operation.
    postExpectStatus(inlineSqlQueryRequest("SELEKT id FROM adh"), 400, "application/fhir+json");
    postExpectStatus(inlineSqlQueryRequest("SELECT x FROM nope"), 400, "application/fhir+json");
    postExpectStatus(inlineSqlQueryRequest("DROP TABLE adh"), 400, "application/fhir+json");
  }

  @Test
  void fhirFormatIsRejectedForAViewDefinitionSubject() {
    // The fhir format is not available to a ViewDefinition subject, whose result is a flat table.
    getExpectStatus(
        PATH + "?subjectCanonical=" + SqlRunTestConfiguration.PATIENT_VIEW_URL + "&_format=fhir",
        400);
  }

  // -------------------------------------------------------------------------
  // Helpers.
  // -------------------------------------------------------------------------

  /** Builds a POST body carrying an inline SQLQuery Library over the ad-hoc view as its context. */
  @Nonnull
  private String inlineSqlQueryRequest(@Nonnull final String sql) {
    final Map<String, Object> parameters = new LinkedHashMap<>();
    parameters.put("resourceType", "Parameters");
    parameters.put(
        "parameter",
        List.of(
            resourceParameter(
                "subjectResource",
                jsonParser.encodeResourceToString(sqlQueryLibrary(sql, "adh", AD_HOC_VIEW_URL))),
            resourceParameter("context", adHocViewJson()),
            stringParameter("_format", SqlRunFormat.NDJSON.getCode())));
    return GSON.toJson(parameters);
  }

  /** Issues a POST, asserts the given status code, and returns the body. */
  @Nonnull
  private String postExpectStatus(
      @Nonnull final String body, final int status, @Nonnull final String accept) {
    final byte[] content =
        webTestClient
            .post()
            .uri("http://localhost:" + port + PATH)
            .header("Content-Type", "application/fhir+json")
            .header("Accept", accept)
            .bodyValue(body)
            .exchange()
            .expectStatus()
            .isEqualTo(status)
            .expectBody()
            .returnResult()
            .getResponseBodyContent();
    return new String(content == null ? new byte[0] : content, StandardCharsets.UTF_8);
  }

  /** Serialises the ad-hoc Patient ViewDefinition, which is never stored on the server. */
  @Nonnull
  private String adHocViewJson() {
    final Map<String, Object> view = new LinkedHashMap<>();
    view.put("resourceType", "ViewDefinition");
    view.put("url", AD_HOC_VIEW_URL);
    view.put("name", "adhoc_view");
    view.put("status", "active");
    view.put("resource", "Patient");
    view.put(
        "select",
        List.of(
            Map.of(
                "column",
                List.of(
                    Map.of("name", "id", "path", "id"),
                    Map.of("name", "family_name", "path", "name.first().family")))));
    return GSON.toJson(view);
  }

  /** Builds an inline SQLQuery Library with a single depends-on dependency. */
  @Nonnull
  private static Library sqlQueryLibrary(
      @Nonnull final String sql, @Nonnull final String label, @Nonnull final String resource) {
    final Library library = new Library();
    library.setStatus(PublicationStatus.ACTIVE);
    library.setType(
        new CodeableConcept()
            .addCoding(new Coding().setSystem(LIBRARY_TYPE_SYSTEM).setCode(SQL_QUERY_TYPE_CODE)));
    final Attachment content = new Attachment();
    content.setContentType("application/sql");
    content.setData(sql.getBytes(StandardCharsets.UTF_8));
    library.addContent(content);
    library.addRelatedArtifact(
        new RelatedArtifact()
            .setType(RelatedArtifactType.DEPENDSON)
            .setLabel(label)
            .setResource(resource));
    return library;
  }

  /** Builds a Parameters part carrying a string value. */
  @Nonnull
  private static Map<String, Object> stringParameter(
      @Nonnull final String name, @Nonnull final String value) {
    final Map<String, Object> parameter = new LinkedHashMap<>();
    parameter.put("name", name);
    parameter.put("valueString", value);
    return parameter;
  }

  /** Builds a Parameters part carrying a Reference value. */
  @Nonnull
  private static Map<String, Object> referenceParameter(
      @Nonnull final String name, @Nonnull final String reference) {
    final Map<String, Object> parameter = new LinkedHashMap<>();
    parameter.put("name", name);
    parameter.put("valueReference", Map.of("reference", reference));
    return parameter;
  }

  /** Builds a Parameters part carrying a resource, from that resource's JSON. */
  @Nonnull
  private static Map<String, Object> resourceParameter(
      @Nonnull final String name, @Nonnull final String resourceJson) {
    final Map<String, Object> parameter = new LinkedHashMap<>();
    parameter.put("name", name);
    parameter.put("resource", GSON.fromJson(resourceJson, Map.class));
    return parameter;
  }

  /** Builds the nested {@code parameters} part binding a single named value. */
  @Nonnull
  private static Map<String, Object> boundParameters(
      @Nonnull final String name, @Nonnull final String value) {
    final Map<String, Object> nested = new LinkedHashMap<>();
    nested.put("resourceType", "Parameters");
    nested.put("parameter", List.of(stringParameter(name, value)));
    final Map<String, Object> parameter = new LinkedHashMap<>();
    parameter.put("name", "parameters");
    parameter.put("resource", nested);
    return parameter;
  }

  /** Issues a GET and asserts a 200 in the expected format, returning the body. */
  @Nonnull
  private String getOk(@Nonnull final String path, @Nonnull final SqlRunFormat format) {
    final EntityExchangeResult<byte[]> result =
        webTestClient
            .get()
            .uri("http://localhost:" + port + path)
            .header("Accept", format.getContentType())
            .exchange()
            .expectStatus()
            .isOk()
            .expectHeader()
            .contentTypeCompatibleWith(MediaType.parseMediaType(format.getContentType()))
            .expectBody()
            .returnResult();
    return new String(
        Objects.requireNonNull(result.getResponseBodyContent()), StandardCharsets.UTF_8);
  }

  /** Issues a POST and asserts a 200 in the expected format, returning the body. */
  @Nonnull
  private String postOk(@Nonnull final String body, @Nonnull final SqlRunFormat format) {
    final EntityExchangeResult<byte[]> result =
        webTestClient
            .post()
            .uri("http://localhost:" + port + PATH)
            .header("Content-Type", "application/fhir+json")
            .header("Accept", format.getContentType())
            .bodyValue(body)
            .exchange()
            .expectStatus()
            .isOk()
            .expectHeader()
            .contentTypeCompatibleWith(MediaType.parseMediaType(format.getContentType()))
            .expectBody()
            .returnResult();
    return new String(
        Objects.requireNonNull(result.getResponseBodyContent()), StandardCharsets.UTF_8);
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
}
