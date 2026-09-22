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
import static com.github.tomakehurst.wiremock.client.WireMock.aResponse;
import static com.github.tomakehurst.wiremock.client.WireMock.equalTo;
import static com.github.tomakehurst.wiremock.client.WireMock.get;
import static com.github.tomakehurst.wiremock.client.WireMock.getRequestedFor;
import static com.github.tomakehurst.wiremock.client.WireMock.urlPathEqualTo;
import static org.assertj.core.api.Assertions.assertThat;

import au.csiro.pathling.operations.sqlquery.SqlQueryOutputFormat;
import ca.uhn.fhir.context.FhirContext;
import ca.uhn.fhir.parser.IParser;
import com.github.tomakehurst.wiremock.WireMockServer;
import com.github.tomakehurst.wiremock.core.WireMockConfiguration;
import com.github.tomakehurst.wiremock.verification.LoggedRequest;
import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import org.hl7.fhir.r4.model.Attachment;
import org.hl7.fhir.r4.model.CodeableConcept;
import org.hl7.fhir.r4.model.Coding;
import org.hl7.fhir.r4.model.Enumerations.PublicationStatus;
import org.hl7.fhir.r4.model.Library;
import org.hl7.fhir.r4.model.RelatedArtifact;
import org.hl7.fhir.r4.model.RelatedArtifact.RelatedArtifactType;
import org.hl7.fhir.r4.model.UriType;
import org.hl7.fhir.r4.model.ValueSet;
import org.hl7.fhir.r4.model.ValueSet.ValueSetExpansionComponent;
import org.hl7.fhir.r4.model.ValueSet.ValueSetExpansionContainsComponent;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.ResourceLock;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;
import org.springframework.http.MediaType;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.test.web.reactive.server.EntityExchangeResult;

/**
 * End-to-end integration test for value set dependencies in SQL on FHIR queries (spec 061), in
 * SERVER terminology mode over a WireMock terminology server. Follows User Story 1 of the feature:
 * a SQLQuery semi-joins a stored Condition ViewDefinition to a value set resolved through {@code
 * $expand}, selects the relation itself, describes it, and expands a pinned and an unpinned
 * reference with exactly the parameters the specification permits.
 *
 * <p>Backed by {@link SqlValueSetTestConfiguration} for the stored ViewDefinition and the Condition
 * data. The terminology server's HTTP response cache is disabled so that every expansion reaches
 * WireMock and can be verified.
 *
 * @author John Grimes
 */
@Tag("IntegrationTest")
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@ResourceLock("wiremock")
@ActiveProfiles({"integration-test"})
@Import(SqlValueSetTestConfiguration.class)
class SqlValueSetIT extends AbstractAsyncExportIT {

  /** The canonical URL of the cardiovascular disease value set of the specification's example. */
  static final String CVD_URL = "http://example.org/ValueSet/cardiovascular-disease";

  /** The pinned version of the cardiovascular disease value set. */
  static final String CVD_VERSION = "2026";

  /** The canonical URL of a value set whose expansion has no members. */
  static final String EMPTY_URL = "http://example.org/ValueSet/empty";

  /** The ICD-10 system URI. */
  static final String ICD10 = "http://hl7.org/fhir/sid/icd-10";

  /** The SNOMED CT version recorded by the stubbed expansion. */
  static final String SNOMED_VERSION = "http://snomed.info/sct/900000000000207008/version/20260131";

  /** The ICD-10 version recorded by the stubbed expansion. */
  static final String ICD10_VERSION = "2019";

  /** The path of the {@code $expand} operation under the WireMock base. */
  static final String EXPAND_PATH = "/fhir/ValueSet/$expand";

  private static final String FHIR_JSON = "application/fhir+json";

  private static WireMockServer wireMockServer;

  @Autowired private FhirContext fhirContext;

  private IParser jsonParser;

  @BeforeAll
  static void startWireMock() {
    wireMockServer = new WireMockServer(WireMockConfiguration.options().dynamicPort());
    wireMockServer.start();
  }

  @AfterAll
  static void stopWireMock() {
    if (wireMockServer != null && wireMockServer.isRunning()) {
      wireMockServer.stop();
    }
  }

  @DynamicPropertySource
  static void configureProperties(final DynamicPropertyRegistry registry) {
    final Path warehouseDir =
        Path.of("src/test/resources/test-data/bulk/fhir/delta").toAbsolutePath();
    registry.add("pathling.storage.warehouseUrl", () -> "file://" + warehouseDir);
    registry.add(
        "pathling.terminology.serverUrl",
        () -> "http://localhost:" + wireMockServer.port() + "/fhir");
    registry.add("pathling.terminology.cache.enabled", () -> "false");
  }

  @BeforeEach
  void setUpStubs() {
    jsonParser = fhirContext.newJsonParser();
    wireMockServer.resetAll();
    stubExpansion(CVD_URL, CVD_VERSION, cvdExpansion());
    stubExpansion(CVD_URL, null, cvdExpansion());
    stubExpansion(EMPTY_URL, null, emptyExpansion());
  }

  // -------------------------------------------------------------------------
  // Scenario 1: a semi-join keeps the conditions whose code is a member, once each.
  // -------------------------------------------------------------------------

  @Test
  void semiJoinReturnsTheMatchingConditionsOnce() {
    final String body = postOk(parametersJson(semiJoinQuery(CVD_URL + "|" + CVD_VERSION)));

    assertThat(rowsOf(body, "patient_id", "code"))
        .containsExactly(
            "Patient/p1/" + SqlValueSetTestConfiguration.MYOCARDIAL_INFARCTION,
            "Patient/p3/" + SqlValueSetTestConfiguration.MYOCARDIAL_INFARCTION);
  }

  // -------------------------------------------------------------------------
  // Scenario 2: the relation itself, flattened, abstract excluded, inactive null.
  // -------------------------------------------------------------------------

  @Test
  void selectingFromTheValueSetReturnsTheFlattenedMembers() {
    final Library library =
        sqlQueryLibrary(
            "SELECT system, version, code, display, inactive FROM cvd_codes ORDER BY code",
            Map.of("cvd_codes", CVD_URL + "|" + CVD_VERSION));

    final String body = postOk(parametersJson(library));

    final List<Map<String, Object>> rows = rows(body);
    assertThat(rows).hasSize(2);
    assertThat(rows.get(0))
        .containsEntry("system", SqlValueSetTestConfiguration.SNOMED)
        .containsEntry("version", SNOMED_VERSION)
        .containsEntry("code", SqlValueSetTestConfiguration.MYOCARDIAL_INFARCTION)
        .containsEntry("display", "Myocardial infarction");
    assertThat(rows.get(0).get("inactive")).isNull();
    assertThat(rows.get(1))
        .containsEntry("system", ICD10)
        .containsEntry("version", ICD10_VERSION)
        .containsEntry("code", "I21")
        .containsEntry("display", "Acute myocardial infarction");
    assertThat(rows.get(1).get("inactive")).isNull();
  }

  // -------------------------------------------------------------------------
  // Scenarios 4 and 8: the pinned version is sent, and nothing else is.
  // -------------------------------------------------------------------------

  @Test
  void pinnedReferenceSendsTheValueSetVersionAndOnlyThePermittedParameters() {
    postOk(parametersJson(semiJoinQuery(CVD_URL + "|" + CVD_VERSION)));

    final List<LoggedRequest> requests = expandRequests();
    assertThat(requests).hasSize(1);
    final LoggedRequest request = requests.get(0);
    assertThat(request.getQueryParams().keySet())
        .containsExactlyInAnyOrder("url", "valueSetVersion", "count", "offset");
    assertThat(request.queryParameter("url").firstValue()).isEqualTo(CVD_URL);
    assertThat(request.queryParameter("valueSetVersion").firstValue()).isEqualTo(CVD_VERSION);
  }

  @Test
  void unpinnedReferenceSendsNoVersion() {
    postOk(parametersJson(semiJoinQuery(CVD_URL)));

    final List<LoggedRequest> requests = expandRequests();
    assertThat(requests).hasSize(1);
    assertThat(requests.get(0).getQueryParams().keySet())
        .containsExactlyInAnyOrder("url", "count", "offset");
  }

  // -------------------------------------------------------------------------
  // Scenario 5: DESCRIBE lists the five columns.
  // -------------------------------------------------------------------------

  @Test
  void describesTheValueSet() {
    final Library library =
        sqlQueryLibrary("DESCRIBE cvd_codes", Map.of("cvd_codes", CVD_URL + "|" + CVD_VERSION));

    final String body = postOk(parametersJson(library));

    assertThat(rowsOf(body, "col_name", "data_type"))
        .containsExactly(
            "system/string", "version/string", "code/string", "display/string", "inactive/boolean");
  }

  // -------------------------------------------------------------------------
  // Scenario 6: an empty expansion is an empty relation, not an error.
  // -------------------------------------------------------------------------

  @Test
  void emptyExpansionYieldsNoRowsAndNoError() {
    final Library library =
        sqlQueryLibrary("SELECT * FROM empty_codes", Map.of("empty_codes", EMPTY_URL));

    final String body = postOk(parametersJson(library));

    assertThat(body.trim()).isEmpty();
  }

  // -------------------------------------------------------------------------
  // Fixtures
  // -------------------------------------------------------------------------

  /**
   * The specification's example expansion: an abstract grouping entry containing a SNOMED CT and an
   * ICD-10 member, with the expansion parameters recording both code system versions.
   */
  @Nonnull
  private static ValueSet cvdExpansion() {
    final ValueSet valueSet = new ValueSet();
    valueSet.setUrl(CVD_URL);
    valueSet.setVersion(CVD_VERSION);
    final ValueSetExpansionComponent expansion = valueSet.getExpansion();
    expansion.setIdentifier("urn:uuid:5b4a8f0e-6d0c-4b8c-9d3a-7c1e2f3a4b5c");
    expansion.setTimestampElement(new org.hl7.fhir.r4.model.DateTimeType("2026-01-31T00:00:00Z"));
    expansion
        .addParameter()
        .setName("version")
        .setValue(new UriType(SqlValueSetTestConfiguration.SNOMED + "|" + SNOMED_VERSION));
    expansion.addParameter().setName("version").setValue(new UriType(ICD10 + "|" + ICD10_VERSION));
    final ValueSetExpansionContainsComponent group = expansion.addContains();
    group.setAbstract(true);
    group.setDisplay("Cardiovascular disease");
    group
        .addContains()
        .setSystem(SqlValueSetTestConfiguration.SNOMED)
        .setVersion(SNOMED_VERSION)
        .setCode(SqlValueSetTestConfiguration.MYOCARDIAL_INFARCTION)
        .setDisplay("Myocardial infarction");
    group
        .addContains()
        .setSystem(ICD10)
        .setVersion(ICD10_VERSION)
        .setCode("I21")
        .setDisplay("Acute myocardial infarction");
    return valueSet;
  }

  /** An expansion with no members. */
  @Nonnull
  private static ValueSet emptyExpansion() {
    final ValueSet valueSet = new ValueSet();
    valueSet.setUrl(EMPTY_URL);
    valueSet.getExpansion().setTotal(0);
    return valueSet;
  }

  /** Stubs {@code $expand} for the given canonical URL and, where non-null, pinned version. */
  private void stubExpansion(
      @Nonnull final String url, @Nullable final String version, @Nonnull final ValueSet response) {
    var mapping = get(urlPathEqualTo(EXPAND_PATH)).withQueryParam("url", equalTo(url));
    if (version != null) {
      mapping = mapping.withQueryParam("valueSetVersion", equalTo(version));
    }
    wireMockServer.stubFor(
        mapping.willReturn(
            aResponse()
                .withStatus(200)
                .withHeader("Content-Type", FHIR_JSON)
                .withBody(jsonParser.encodeResourceToString(response))));
  }

  @Nonnull
  private static List<LoggedRequest> expandRequests() {
    return wireMockServer.findAll(getRequestedFor(urlPathEqualTo(EXPAND_PATH)));
  }

  // -------------------------------------------------------------------------
  // Request helpers
  // -------------------------------------------------------------------------

  /** Builds the Scenario 1 SQLQuery semi-joining the Condition view to the given value set. */
  @Nonnull
  Library semiJoinQuery(@Nonnull final String valueSetCanonical) {
    final Map<String, String> dependencies = new LinkedHashMap<>();
    dependencies.put("conditions", SqlValueSetTestConfiguration.CONDITION_VIEW_URL);
    dependencies.put("cvd_codes", valueSetCanonical);
    return sqlQueryLibrary(
        "SELECT conditions.patient_id, conditions.code FROM conditions"
            + " WHERE EXISTS (SELECT 1 FROM cvd_codes"
            + " WHERE cvd_codes.system = conditions.system AND cvd_codes.code = conditions.code)"
            + " ORDER BY conditions.id",
        dependencies);
  }

  /** Parses each NDJSON row of a response body into a map, in response order. */
  @Nonnull
  @SuppressWarnings("unchecked")
  List<Map<String, Object>> rows(@Nonnull final String body) {
    return Arrays.stream(body.trim().split("\n"))
        .map(line -> (Map<String, Object>) gson.fromJson(line, Map.class))
        .toList();
  }

  /**
   * Projects each NDJSON row of a response body to {@code <first>/<second>} using the two named
   * columns, in response order.
   */
  @Nonnull
  List<String> rowsOf(
      @Nonnull final String body, @Nonnull final String first, @Nonnull final String second) {
    return rows(body).stream().map(row -> row.get(first) + "/" + row.get(second)).toList();
  }

  @Nonnull
  String postOk(@Nonnull final String body) {
    final EntityExchangeResult<byte[]> result =
        webTestClient
            .post()
            .uri("http://localhost:" + port + "/fhir/$sql-run")
            .header("Content-Type", "application/fhir+json")
            .header("Accept", SqlQueryOutputFormat.NDJSON.getContentType())
            .bodyValue(body)
            .exchange()
            .expectStatus()
            .isOk()
            .expectHeader()
            .contentTypeCompatibleWith(
                MediaType.parseMediaType(SqlQueryOutputFormat.NDJSON.getContentType()))
            .expectBody()
            .returnResult();
    return new String(
        Objects.requireNonNull(result.getResponseBodyContent()), StandardCharsets.UTF_8);
  }

  @Nonnull
  String postExpectStatus(@Nonnull final String body, final int status) {
    final EntityExchangeResult<byte[]> result =
        webTestClient
            .post()
            .uri("http://localhost:" + port + "/fhir/$sql-run")
            .header("Content-Type", "application/fhir+json")
            .header("Accept", SqlQueryOutputFormat.NDJSON.getContentType())
            .bodyValue(body)
            .exchange()
            .expectStatus()
            .isEqualTo(status)
            .expectBody()
            .returnResult();
    final byte[] payload = result.getResponseBodyContent();
    return payload == null ? "" : new String(payload, StandardCharsets.UTF_8);
  }

  /** Builds an inline SQLQuery Library with the given depends-on dependencies (label to URL). */
  @Nonnull
  Library sqlQueryLibrary(
      @Nonnull final String sql, @Nonnull final Map<String, String> dependenciesByLabel) {
    final Library library = new Library();
    library.setStatus(PublicationStatus.ACTIVE);
    library.setType(
        new CodeableConcept()
            .addCoding(new Coding().setSystem(LIBRARY_TYPE_SYSTEM).setCode(SQL_QUERY_TYPE_CODE)));
    final Attachment content = new Attachment();
    content.setContentType("application/sql");
    content.setData(sql.getBytes(StandardCharsets.UTF_8));
    library.addContent(content);
    new LinkedHashMap<>(dependenciesByLabel)
        .forEach(
            (label, resource) ->
                library.addRelatedArtifact(
                    new RelatedArtifact()
                        .setType(RelatedArtifactType.DEPENDSON)
                        .setLabel(label)
                        .setResource(resource)));
    return library;
  }

  /** Encodes a resource as the generic JSON map the Gson-built request bodies carry. */
  @Nonnull
  @SuppressWarnings("unchecked")
  Map<String, Object> resourceMap(@Nonnull final org.hl7.fhir.r4.model.Resource resource) {
    return gson.fromJson(jsonParser.encodeResourceToString(resource), Map.class);
  }

  /**
   * Wraps the Library as the {@code subjectResource} of a {@code $sql-run} Parameters body, along
   * with the NDJSON format and any further parameters.
   */
  @SafeVarargs
  @Nonnull
  final String parametersJson(
      @Nonnull final Library library, @Nonnull final Map<String, Object>... extraParameters) {
    final Map<String, Object> parameters =
        parameters(
            resourcePart("subjectResource", resourceMap(library)),
            simpleParam("_format", "valueString", SqlQueryOutputFormat.NDJSON.getCode()));
    for (final Map<String, Object> extra : extraParameters) {
      addParam(parameters, extra);
    }
    return gson.toJson(parameters);
  }
}
