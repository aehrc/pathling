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
import static com.github.tomakehurst.wiremock.client.WireMock.post;
import static com.github.tomakehurst.wiremock.client.WireMock.postRequestedFor;
import static com.github.tomakehurst.wiremock.client.WireMock.urlPathEqualTo;
import static org.assertj.core.api.Assertions.assertThat;

import au.csiro.pathling.operations.sqlquery.SqlQueryOutputFormat;
import ca.uhn.fhir.context.FhirContext;
import ca.uhn.fhir.parser.IParser;
import com.github.tomakehurst.wiremock.WireMockServer;
import com.github.tomakehurst.wiremock.core.WireMockConfiguration;
import com.github.tomakehurst.wiremock.verification.LoggedRequest;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.ToNumberPolicy;
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
import org.hl7.fhir.r4.model.OperationOutcome;
import org.hl7.fhir.r4.model.OperationOutcome.IssueSeverity;
import org.hl7.fhir.r4.model.OperationOutcome.IssueType;
import org.hl7.fhir.r4.model.OperationOutcome.OperationOutcomeIssueComponent;
import org.hl7.fhir.r4.model.Parameters;
import org.hl7.fhir.r4.model.RelatedArtifact;
import org.hl7.fhir.r4.model.RelatedArtifact.RelatedArtifactType;
import org.hl7.fhir.r4.model.UriType;
import org.hl7.fhir.r4.model.ValueSet;
import org.hl7.fhir.r4.model.ValueSet.ConceptSetComponent;
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
 * reference with exactly the parameters the specification permits. Follows User Story 2: a {@code
 * context} ValueSet carrying an expansion is used as-is and outranks the terminology server, is
 * matched to a pinned dependency only when its version agrees, is expanded through a {@code POST
 * $expand} when it carries only a compose, and is rejected when unmatched or when it shares a URL
 * with another entry. Follows User Story 3: the value set resolves through a stored SQLView in both
 * operations, an export job expands it once for all of its subjects, a value set fault rejects the
 * kick-off before any job exists, two kick-offs inlining different memberships are two jobs, and
 * the {@code patient} filter reaches the FHIR view but never the value set. Follows User Story 4:
 * every fault carries its specified status and an issue naming the label, the canonical URL and the
 * reason, on both operations, with no job created on {@code $sql-export}; the unreachable server is
 * covered by {@link SqlValueSetUnreachableIT}, since the server URL is fixed per context.
 *
 * <p>Backed by {@link SqlValueSetTestConfiguration} for the stored ViewDefinition and SQLView, the
 * Condition data and the Patients. The terminology server's HTTP response cache is disabled so that
 * every expansion reaches WireMock and can be verified.
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
  static final String CVD_URL = SqlValueSetTestConfiguration.CVD_URL;

  /** The pinned version of the cardiovascular disease value set. */
  static final String CVD_VERSION = SqlValueSetTestConfiguration.CVD_VERSION;

  /** The canonical URL of a value set the terminology server does not hold. */
  static final String MISSING_URL = "http://example.org/ValueSet/does-not-exist";

  /** The canonical URL of a value set the terminology server holds but cannot expand. */
  static final String BROKEN_URL = "http://example.org/ValueSet/broken";

  /** The canonical URL of a value set whose expansion has no members. */
  static final String EMPTY_URL = "http://example.org/ValueSet/empty";

  /** The canonical URL of a value set whose first expansion page already exceeds the cap. */
  static final String LARGE_URL = "http://example.org/ValueSet/large";

  /**
   * The membership cap configured for this class: the cardiovascular disease value set sits exactly
   * at it, so every success scenario also proves that a membership at the cap is accepted.
   */
  static final int MAX_MEMBERS = 2;

  /** The ICD-10 system URI. */
  static final String ICD10 = "http://hl7.org/fhir/sid/icd-10";

  /** The SNOMED CT version recorded by the stubbed expansion. */
  static final String SNOMED_VERSION = "http://snomed.info/sct/900000000000207008/version/20260131";

  /** The ICD-10 version recorded by the stubbed expansion. */
  static final String ICD10_VERSION = "2019";

  /** The path of the {@code $expand} operation under the WireMock base. */
  static final String EXPAND_PATH = "/fhir/ValueSet/$expand";

  private static final String FHIR_JSON = "application/fhir+json";

  /** Parses encoded resources into maps with whole numbers kept as longs. */
  private static final Gson RESOURCE_GSON =
      new GsonBuilder().setObjectToNumberStrategy(ToNumberPolicy.LONG_OR_DOUBLE).create();

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
    registry.add("pathling.sqlQuery.valueSetMaxMembers", () -> String.valueOf(MAX_MEMBERS));
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
  // US2 scenario 1: a supplied expansion is used and the terminology server is not consulted.
  // -------------------------------------------------------------------------

  @Test
  void suppliedExpansionIsUsedWithoutAnyExpandRequest() {
    final String body =
        postOk(
            parametersJson(
                semiJoinQuery(CVD_URL + "|" + CVD_VERSION),
                resourcePart("context", resourceMap(cvdExpansion()))));

    assertThat(rowsOf(body, "patient_id", "code"))
        .containsExactly(
            "Patient/p1/" + SqlValueSetTestConfiguration.MYOCARDIAL_INFARCTION,
            "Patient/p3/" + SqlValueSetTestConfiguration.MYOCARDIAL_INFARCTION);
    assertThat(expandRequests()).isEmpty();
    assertThat(postExpandRequests()).isEmpty();
  }

  // -------------------------------------------------------------------------
  // US2 scenario 2: a supplied expansion outranks a canonical the server could resolve.
  // -------------------------------------------------------------------------

  @Test
  void suppliedExpansionOutranksAResolvableCanonical() {
    // The server-side expansion of CVD_URL would hold the myocardial infarction and I21 members;
    // the supplied one names diabetes instead, and it is the supplied membership that is used.
    final ValueSet supplied = suppliedValueSet(CVD_URL, null);
    supplied
        .getExpansion()
        .addContains()
        .setSystem(SqlValueSetTestConfiguration.SNOMED)
        .setCode(SqlValueSetTestConfiguration.DIABETES_MELLITUS);

    final String body =
        postOk(
            parametersJson(semiJoinQuery(CVD_URL), resourcePart("context", resourceMap(supplied))));

    assertThat(rowsOf(body, "patient_id", "code"))
        .containsExactly("Patient/p2/" + SqlValueSetTestConfiguration.DIABETES_MELLITUS);
    assertThat(expandRequests()).isEmpty();
  }

  // -------------------------------------------------------------------------
  // US2 scenario 3: a pinned dependency matches a supplied ValueSet only when versions agree.
  // -------------------------------------------------------------------------

  @Test
  void pinnedDependencyMatchesASuppliedValueSetWithTheSameVersion() {
    final ValueSet supplied = suppliedValueSet(CVD_URL, CVD_VERSION);
    supplied
        .getExpansion()
        .addContains()
        .setSystem(SqlValueSetTestConfiguration.SNOMED)
        .setCode(SqlValueSetTestConfiguration.DIABETES_MELLITUS);

    final String body =
        postOk(
            parametersJson(
                semiJoinQuery(CVD_URL + "|" + CVD_VERSION),
                resourcePart("context", resourceMap(supplied))));

    assertThat(rowsOf(body, "patient_id", "code"))
        .containsExactly("Patient/p2/" + SqlValueSetTestConfiguration.DIABETES_MELLITUS);
    assertThat(expandRequests()).isEmpty();
  }

  @Test
  void pinnedDependencyIgnoresASuppliedValueSetAtAnotherVersionAndConsultsTheServer() {
    // The supplied 2025 ValueSet satisfies the dependency pinned to 2025 only; the dependency
    // pinned to 2026 falls through to the terminology server, which is asked for 2026.
    final ValueSet supplied = suppliedValueSet(CVD_URL, "2025");
    supplied
        .getExpansion()
        .addContains()
        .setSystem(SqlValueSetTestConfiguration.SNOMED)
        .setCode("OLD-2025");
    final Map<String, String> dependencies = new LinkedHashMap<>();
    dependencies.put("cvd_codes", CVD_URL + "|" + CVD_VERSION);
    dependencies.put("old_codes", CVD_URL + "|2025");
    final Library library =
        sqlQueryLibrary(
            "SELECT code FROM cvd_codes UNION ALL SELECT code FROM old_codes ORDER BY code",
            dependencies);

    final String body =
        postOk(parametersJson(library, resourcePart("context", resourceMap(supplied))));

    assertThat(rows(body))
        .extracting(row -> row.get("code"))
        .containsExactly(SqlValueSetTestConfiguration.MYOCARDIAL_INFARCTION, "I21", "OLD-2025");
    final List<LoggedRequest> requests = expandRequests();
    assertThat(requests).hasSize(1);
    assertThat(requests.get(0).queryParameter("valueSetVersion").firstValue())
        .isEqualTo(CVD_VERSION);
  }

  // -------------------------------------------------------------------------
  // US2 scenario 4: a compose-only ValueSet is expanded through one POST $expand.
  // -------------------------------------------------------------------------

  @Test
  void composeOnlyValueSetIsExpandedThroughOnePostCarryingTheResource() {
    stubPostExpansion(cvdExpansion());
    final ValueSet supplied = suppliedValueSet(CVD_URL, CVD_VERSION);
    final ConceptSetComponent include = supplied.getCompose().addInclude();
    include.setSystem(SqlValueSetTestConfiguration.SNOMED);
    include.addConcept().setCode(SqlValueSetTestConfiguration.MYOCARDIAL_INFARCTION);

    final String body =
        postOk(
            parametersJson(
                semiJoinQuery(CVD_URL + "|" + CVD_VERSION),
                resourcePart("context", resourceMap(supplied))));

    assertThat(rowsOf(body, "patient_id", "code"))
        .containsExactly(
            "Patient/p1/" + SqlValueSetTestConfiguration.MYOCARDIAL_INFARCTION,
            "Patient/p3/" + SqlValueSetTestConfiguration.MYOCARDIAL_INFARCTION);
    assertThat(expandRequests()).isEmpty();
    final List<LoggedRequest> posts = postExpandRequests();
    assertThat(posts).hasSize(1);
    final Parameters sent = (Parameters) jsonParser.parseResource(posts.get(0).getBodyAsString());
    assertThat(sent.getParameter())
        .extracting(Parameters.ParametersParameterComponent::getName)
        .containsExactly("valueSet", "count", "offset");
    final ValueSet sentValueSet = (ValueSet) sent.getParameter().get(0).getResource();
    assertThat(sentValueSet.getUrl()).isEqualTo(CVD_URL);
    assertThat(sentValueSet.getCompose().getIncludeFirstRep().getConceptFirstRep().getCode())
        .isEqualTo(SqlValueSetTestConfiguration.MYOCARDIAL_INFARCTION);
  }

  // -------------------------------------------------------------------------
  // US2 scenario 6: a supplied ValueSet matching no dependency is a 400.
  // -------------------------------------------------------------------------

  @Test
  void unmatchedSuppliedValueSetIsRejected() {
    final ValueSet unrelated = suppliedValueSet("http://example.org/ValueSet/unrelated", null);
    unrelated
        .getExpansion()
        .addContains()
        .setSystem(SqlValueSetTestConfiguration.SNOMED)
        .setCode(SqlValueSetTestConfiguration.MYOCARDIAL_INFARCTION);

    final String body =
        postExpectStatus(
            parametersJson(
                semiJoinQuery(CVD_URL + "|" + CVD_VERSION),
                resourcePart("context", resourceMap(unrelated))),
            400);

    assertThat(body)
        .contains("match no dependency of any subject", "http://example.org/ValueSet/unrelated");
  }

  // -------------------------------------------------------------------------
  // US2 scenario 7: two context entries sharing a URL are a 400, whatever their kinds.
  // -------------------------------------------------------------------------

  @Test
  void aViewDefinitionAndAValueSetSharingAUrlAreRejected() {
    final String body =
        postExpectStatus(
            parametersJson(
                semiJoinQuery(CVD_URL + "|" + CVD_VERSION),
                resourcePart("context", viewDefinitionAt(CVD_URL)),
                resourcePart("context", resourceMap(cvdExpansion()))),
            400);

    assertThat(body).contains("share the canonical URL", CVD_URL);
  }

  // -------------------------------------------------------------------------
  // US3 scenario 1: a SQLQuery over a stored SQLView that depends on the value set.
  // -------------------------------------------------------------------------

  @Test
  void sqlQueryOverAStoredSqlViewReflectsTheMembership() {
    final String body = postOk(parametersJson(queryOverStoredSqlView()));

    assertThat(rowsOf(body, "patient_id", "code"))
        .containsExactly(
            "Patient/p1/" + SqlValueSetTestConfiguration.MYOCARDIAL_INFARCTION,
            "Patient/p3/" + SqlValueSetTestConfiguration.MYOCARDIAL_INFARCTION);
    assertThat(expandRequests()).hasSize(1);
  }

  // -------------------------------------------------------------------------
  // US3 scenario 2: the stored SQLView as an export subject.
  // -------------------------------------------------------------------------

  @Test
  void exportsTheStoredSqlViewAsASubject() throws InterruptedException {
    final Map<String, Object> body =
        parameters(
            subject(
                nameOf("cvd"),
                simpleParam(
                    "subjectCanonical",
                    "valueCanonical",
                    SqlValueSetTestConfiguration.CVD_CONDITIONS_URL)),
            simpleParam("_format", "valueString", SqlQueryOutputFormat.NDJSON.getCode()));

    final Map<String, Object> manifest = exportToCompletion(systemLevelUri(), body);

    assertThat(findParamValue(manifest, "status", "valueCode")).isEqualTo("completed");
    final List<Map<String, Object>> outputs = paramsByName(manifest, "output");
    assertThat(outputs).hasSize(1);
    assertThat(rowsOf(downloadAll(outputs.get(0)), "patient_id", "code"))
        .containsExactlyInAnyOrder(
            "Patient/p1/" + SqlValueSetTestConfiguration.MYOCARDIAL_INFARCTION,
            "Patient/p3/" + SqlValueSetTestConfiguration.MYOCARDIAL_INFARCTION);
  }

  // -------------------------------------------------------------------------
  // US3 scenario 3: two subjects reaching one value set expand it once and agree.
  // -------------------------------------------------------------------------

  @Test
  void exportWithTwoSubjectsReachingTheValueSetExpandsItOnce() throws InterruptedException {
    // The SQLView reaches the value set through its own dependencies, and the inline SQLQuery
    // reaches it directly, under the same pinned canonical.
    final Map<String, Object> body =
        parameters(
            subject(
                nameOf("stored"),
                simpleParam(
                    "subjectCanonical",
                    "valueCanonical",
                    SqlValueSetTestConfiguration.CVD_CONDITIONS_URL)),
            subject(
                nameOf("inline"),
                resourcePart(
                    "subjectResource", resourceMap(semiJoinQuery(CVD_URL + "|" + CVD_VERSION)))),
            simpleParam("_format", "valueString", SqlQueryOutputFormat.NDJSON.getCode()));

    final Map<String, Object> manifest = exportToCompletion(systemLevelUri(), body);

    assertThat(expandRequests()).hasSize(1);
    final List<Map<String, Object>> outputs = paramsByName(manifest, "output");
    assertThat(outputs).hasSize(2);
    final List<String> expected =
        List.of(
            "Patient/p1/" + SqlValueSetTestConfiguration.MYOCARDIAL_INFARCTION,
            "Patient/p3/" + SqlValueSetTestConfiguration.MYOCARDIAL_INFARCTION);
    assertThat(rowsOf(downloadAll(outputNamed(outputs, "stored")), "patient_id", "code"))
        .containsExactlyInAnyOrderElementsOf(expected);
    assertThat(rowsOf(downloadAll(outputNamed(outputs, "inline")), "patient_id", "code"))
        .containsExactlyInAnyOrderElementsOf(expected);
  }

  // -------------------------------------------------------------------------
  // US3 scenario 4: a value set fault rejects the kick-off and creates no job.
  // -------------------------------------------------------------------------

  @Test
  void kickOffWithAnUnresolvableValueSetIsA404AndCreatesNoJob() {
    stubExpansionFailure(MISSING_URL, 404, "ValueSet not found");
    final int jobsBefore = jobCount();

    final String body =
        kickOffExpectStatus(
            parameters(
                subject(
                    nameOf("inline"),
                    resourcePart("subjectResource", resourceMap(semiJoinQuery(MISSING_URL))))),
            404);

    assertThat(body)
        .contains("cvd_codes", MISSING_URL)
        .contains("no ViewDefinition, SQLView, external table, concept map or value set matches");
    assertThat(jobCount()).as("A rejected kick-off must not register a job").isEqualTo(jobsBefore);
  }

  @Test
  void kickOffWithAnUndeterminableValueSetIsA422AndCreatesNoJob() {
    stubExpansionFailure(BROKEN_URL, 422, "Unable to expand: too many codes");
    final int jobsBefore = jobCount();

    final String body =
        kickOffExpectStatus(
            parameters(
                subject(
                    nameOf("inline"),
                    resourcePart("subjectResource", resourceMap(semiJoinQuery(BROKEN_URL))))),
            422);

    assertThat(body).contains("cvd_codes", BROKEN_URL, "could not be determined", "too many codes");
    assertThat(jobCount()).as("A rejected kick-off must not register a job").isEqualTo(jobsBefore);
  }

  // -------------------------------------------------------------------------
  // US3 scenario 5: kick-offs differing only in the inline ValueSet are two jobs.
  // -------------------------------------------------------------------------

  @Test
  void kickOffsDifferingOnlyInTheInlineValueSetAreTwoJobs() throws InterruptedException {
    final ValueSet diabetes = suppliedValueSet(CVD_URL, CVD_VERSION);
    diabetes
        .getExpansion()
        .addContains()
        .setSystem(SqlValueSetTestConfiguration.SNOMED)
        .setCode(SqlValueSetTestConfiguration.DIABETES_MELLITUS);
    final Map<String, Object> cardiovascular = exportOverInlineValueSet(cvdExpansion());
    final Map<String, Object> alternative = exportOverInlineValueSet(diabetes);

    final String firstStatusUrl = contentLocationOf(systemLevelUri(), cardiovascular);
    final String secondStatusUrl = contentLocationOf(systemLevelUri(), alternative);
    assertThat(secondStatusUrl)
        .as("A kick-off inlining a different membership must get its own job")
        .isNotEqualTo(firstStatusUrl);

    // Each job ran the membership its own request supplied.
    final Map<String, Object> firstManifest = exportToCompletion(systemLevelUri(), cardiovascular);
    assertThat(
            rowsOf(
                downloadAll(outputNamed(paramsByName(firstManifest, "output"), "inline")),
                "patient_id",
                "code"))
        .containsExactlyInAnyOrder(
            "Patient/p1/" + SqlValueSetTestConfiguration.MYOCARDIAL_INFARCTION,
            "Patient/p3/" + SqlValueSetTestConfiguration.MYOCARDIAL_INFARCTION);
    final Map<String, Object> secondManifest = exportToCompletion(systemLevelUri(), alternative);
    assertThat(
            rowsOf(
                downloadAll(outputNamed(paramsByName(secondManifest, "output"), "inline")),
                "patient_id",
                "code"))
        .containsExactly("Patient/p2/" + SqlValueSetTestConfiguration.DIABETES_MELLITUS);
    assertThat(expandRequests()).isEmpty();
  }

  // -------------------------------------------------------------------------
  // US3 scenario 6: the patient filter narrows the FHIR side of the join and never the value set.
  // -------------------------------------------------------------------------

  @Test
  void patientFilterNarrowsTheJoinThroughTheView() {
    final String body =
        postOk(parametersJson(queryOverStoredSqlView(), referencePart("patient", "Patient/p3")));

    assertThat(rowsOf(body, "patient_id", "code"))
        .containsExactly("Patient/p3/" + SqlValueSetTestConfiguration.MYOCARDIAL_INFARCTION);
  }

  @Test
  void patientFilterLeavesTheValueSetItselfUntouched() {
    final Library library =
        sqlQueryLibrary(
            "SELECT * FROM cvd_codes ORDER BY code",
            Map.of("cvd_codes", CVD_URL + "|" + CVD_VERSION));

    final String body = postOk(parametersJson(library, referencePart("patient", "Patient/p3")));

    assertThat(rows(body))
        .extracting(row -> row.get("code"))
        .containsExactly(SqlValueSetTestConfiguration.MYOCARDIAL_INFARCTION, "I21");
  }

  // -------------------------------------------------------------------------
  // US4 scenario 1: a canonical nothing resolves is a 404 naming the label and the reference.
  // -------------------------------------------------------------------------

  @Test
  void unresolvableValueSetIsA404NamingTheLabelAndTheReference() {
    stubExpansionFailure(MISSING_URL, 404, "ValueSet not found");

    final String body = postExpectStatus(parametersJson(semiJoinQuery(MISSING_URL)), 404);

    assertThat(singleIssue(body).getDiagnostics())
        .isEqualTo(
            "Failed to resolve the dependency for label 'cvd_codes' with reference"
                + " 'http://example.org/ValueSet/does-not-exist': no ViewDefinition, SQLView,"
                + " external table, concept map or value set matches that canonical URL");
  }

  // -------------------------------------------------------------------------
  // US4 scenario 2: a value set the server cannot expand is a 422 carrying its diagnostics.
  // -------------------------------------------------------------------------

  @Test
  void undeterminableValueSetIsA422CarryingTheServersDiagnostics() {
    stubExpansionFailure(BROKEN_URL, 422, "Unable to expand: too many codes");

    final String body = postExpectStatus(parametersJson(semiJoinQuery(BROKEN_URL)), 422);

    assertIssue(
        body,
        SubjectResolver.SUBJECT_EXPRESSION,
        "The membership of the value set for label 'cvd_codes' (canonical URL"
            + " 'http://example.org/ValueSet/broken') could not be determined: the terminology"
            + " server returned HTTP 422: Unable to expand: too many codes");
  }

  // -------------------------------------------------------------------------
  // US4 scenario 3: a membership over the cap is a 422 naming the maximum, after one page only.
  // -------------------------------------------------------------------------

  @Test
  void membershipOverTheCapIsA422NamingTheMaximumAfterOnePage() {
    // The first page holds three of a reported five members, which already exceeds the cap of two;
    // the second page must never be requested.
    stubExpansion(LARGE_URL, null, largeExpansionFirstPage());
    final Library library =
        sqlQueryLibrary("SELECT * FROM cvd_codes", Map.of("cvd_codes", LARGE_URL));

    final String body = postExpectStatus(parametersJson(library), 422);

    assertIssue(
        body,
        SubjectResolver.SUBJECT_EXPRESSION,
        "The value set for label 'cvd_codes' (canonical URL 'http://example.org/ValueSet/large')"
            + " has more than the maximum of 2 members permitted by"
            + " pathling.sqlQuery.valueSetMaxMembers");
    assertThat(expandRequests()).as("No page beyond the one revealing the excess").hasSize(1);
  }

  @Test
  void membershipExactlyAtTheCapIsAccepted() {
    final Library library =
        sqlQueryLibrary(
            "SELECT code FROM cvd_codes ORDER BY code",
            Map.of("cvd_codes", CVD_URL + "|" + CVD_VERSION));

    final String body = postOk(parametersJson(library));

    assertThat(rows(body)).hasSize(MAX_MEMBERS);
  }

  // -------------------------------------------------------------------------
  // US4 scenario 4: a supplied expansion with an offset, or a total over its entries, is
  // incomplete.
  // -------------------------------------------------------------------------

  @Test
  void suppliedExpansionWithAnOffsetIsA422NamingTheContext() {
    final ValueSet supplied = suppliedValueSet(CVD_URL, null);
    supplied.getExpansion().setOffset(0);
    supplied
        .getExpansion()
        .addContains()
        .setSystem(SqlValueSetTestConfiguration.SNOMED)
        .setCode(SqlValueSetTestConfiguration.MYOCARDIAL_INFARCTION);

    final String body =
        postExpectStatus(
            parametersJson(selectFromCvdCodes(), resourcePart("context", resourceMap(supplied))),
            422);

    assertIssue(
        body,
        SuppliedArtefacts.CONTEXT_EXPRESSION,
        "The membership of the supplied value set for label 'cvd_codes' (canonical URL"
            + " 'http://example.org/ValueSet/cardiovascular-disease') could not be determined: the"
            + " expansion is incomplete: it carries an offset");
    assertThat(expandRequests()).isEmpty();
  }

  @Test
  void suppliedExpansionWithTotalOverItsEntriesIsA422NamingTheContext() {
    final ValueSet supplied = suppliedValueSet(CVD_URL, null);
    supplied.getExpansion().setTotal(5);
    supplied
        .getExpansion()
        .addContains()
        .setSystem(SqlValueSetTestConfiguration.SNOMED)
        .setCode(SqlValueSetTestConfiguration.MYOCARDIAL_INFARCTION);
    supplied.getExpansion().addContains().setSystem(ICD10).setCode("I21");

    final String body =
        postExpectStatus(
            parametersJson(selectFromCvdCodes(), resourcePart("context", resourceMap(supplied))),
            422);

    assertIssue(
        body,
        SuppliedArtefacts.CONTEXT_EXPRESSION,
        "The membership of the supplied value set for label 'cvd_codes' (canonical URL"
            + " 'http://example.org/ValueSet/cardiovascular-disease') could not be determined: the"
            + " expansion is incomplete: total 5 exceeds 2 entries");
  }

  // -------------------------------------------------------------------------
  // US4 scenario 5: a supplied entry lacking code, or lacking system, does not identify a member.
  // -------------------------------------------------------------------------

  @Test
  void suppliedEntryWithoutCodeIsA422NamingTheContext() {
    final ValueSet supplied = suppliedValueSet(CVD_URL, null);
    supplied.getExpansion().addContains().setSystem(SqlValueSetTestConfiguration.SNOMED);

    final String body =
        postExpectStatus(
            parametersJson(selectFromCvdCodes(), resourcePart("context", resourceMap(supplied))),
            422);

    assertIssue(
        body,
        SuppliedArtefacts.CONTEXT_EXPRESSION,
        "The membership of the supplied value set for label 'cvd_codes' (canonical URL"
            + " 'http://example.org/ValueSet/cardiovascular-disease') could not be determined: an"
            + " entry does not identify a member: no code");
  }

  @Test
  void suppliedEntryWithoutSystemIsA422NamingTheContextAndTheCode() {
    final ValueSet supplied = suppliedValueSet(CVD_URL, null);
    supplied
        .getExpansion()
        .addContains()
        .setCode(SqlValueSetTestConfiguration.MYOCARDIAL_INFARCTION);

    final String body =
        postExpectStatus(
            parametersJson(selectFromCvdCodes(), resourcePart("context", resourceMap(supplied))),
            422);

    assertIssue(
        body,
        SuppliedArtefacts.CONTEXT_EXPRESSION,
        "The membership of the supplied value set for label 'cvd_codes' (canonical URL"
            + " 'http://example.org/ValueSet/cardiovascular-disease') could not be determined: an"
            + " entry does not identify a member: no system for code '"
            + SqlValueSetTestConfiguration.MYOCARDIAL_INFARCTION
            + "'");
  }

  // -------------------------------------------------------------------------
  // US4 scenario 6: a supplied ValueSet with neither expansion nor compose defines no membership.
  // -------------------------------------------------------------------------

  @Test
  void suppliedValueSetWithNeitherExpansionNorComposeIsA422NamingTheContext() {
    final ValueSet supplied = suppliedValueSet(CVD_URL, null);

    final String body =
        postExpectStatus(
            parametersJson(selectFromCvdCodes(), resourcePart("context", resourceMap(supplied))),
            422);

    assertIssue(
        body,
        SuppliedArtefacts.CONTEXT_EXPRESSION,
        "The supplied value set for label 'cvd_codes' (canonical URL"
            + " 'http://example.org/ValueSet/cardiovascular-disease') defines no membership: it"
            + " carries neither an expansion nor a compose");
    assertThat(expandRequests()).isEmpty();
  }

  // -------------------------------------------------------------------------
  // US4 scenario 7: a supplied ValueSet without a url is a 400, as for any context entry.
  // -------------------------------------------------------------------------

  @Test
  void suppliedValueSetWithoutAUrlIsA400() {
    final ValueSet supplied = suppliedValueSet(CVD_URL, null);
    supplied.setUrl(null);
    supplied
        .getExpansion()
        .addContains()
        .setSystem(SqlValueSetTestConfiguration.SNOMED)
        .setCode(SqlValueSetTestConfiguration.MYOCARDIAL_INFARCTION);

    final String body =
        postExpectStatus(
            parametersJson(selectFromCvdCodes(), resourcePart("context", resourceMap(supplied))),
            400);

    assertIssue(
        body,
        SuppliedArtefacts.CONTEXT_EXPRESSION,
        "A 'context' ValueSet must carry a url, since entries are matched to dependencies by"
            + " canonical URL.");
  }

  // -------------------------------------------------------------------------
  // US4 scenario 9: the same faults on a $sql-export kick-off, with no job created. The 404 case
  // is kickOffWithAnUnresolvableValueSetIsA404AndCreatesNoJob above.
  // -------------------------------------------------------------------------

  @Test
  void kickOffOverTheCapIsA422NamingTheMaximumAndCreatesNoJob() {
    stubExpansion(LARGE_URL, null, largeExpansionFirstPage());
    final Library library =
        sqlQueryLibrary("SELECT * FROM cvd_codes", Map.of("cvd_codes", LARGE_URL));
    final int jobsBefore = jobCount();

    final String body =
        kickOffExpectStatus(
            parameters(
                subject(nameOf("inline"), resourcePart("subjectResource", resourceMap(library)))),
            422);

    assertIssue(
        body,
        SubjectResolver.SUBJECT_EXPRESSION,
        "The value set for label 'cvd_codes' (canonical URL 'http://example.org/ValueSet/large')"
            + " has more than the maximum of 2 members permitted by"
            + " pathling.sqlQuery.valueSetMaxMembers");
    assertThat(expandRequests()).hasSize(1);
    assertThat(jobCount()).as("A rejected kick-off must not register a job").isEqualTo(jobsBefore);
  }

  @Test
  void kickOffWithAnIncompleteSuppliedExpansionIsA422AndCreatesNoJob() {
    final ValueSet supplied = suppliedValueSet(CVD_URL, null);
    supplied.getExpansion().setOffset(0);
    supplied
        .getExpansion()
        .addContains()
        .setSystem(SqlValueSetTestConfiguration.SNOMED)
        .setCode(SqlValueSetTestConfiguration.MYOCARDIAL_INFARCTION);
    final int jobsBefore = jobCount();

    final String body =
        kickOffExpectStatus(
            parameters(
                subject(
                    nameOf("inline"),
                    resourcePart("subjectResource", resourceMap(selectFromCvdCodes()))),
                resourcePart("context", resourceMap(supplied))),
            422);

    assertIssue(
        body,
        SuppliedArtefacts.CONTEXT_EXPRESSION,
        "The membership of the supplied value set for label 'cvd_codes' (canonical URL"
            + " 'http://example.org/ValueSet/cardiovascular-disease') could not be determined: the"
            + " expansion is incomplete: it carries an offset");
    assertThat(expandRequests()).isEmpty();
    assertThat(jobCount()).as("A rejected kick-off must not register a job").isEqualTo(jobsBefore);
  }

  // -------------------------------------------------------------------------
  // US4 scenario 10: the value set is reachable by its declared label and nothing else.
  // -------------------------------------------------------------------------

  @Test
  void namingTheValueSetByAnUndeclaredIdentifierIsA400() {
    final Library library =
        sqlQueryLibrary(
            "SELECT * FROM cardiovascular", Map.of("cvd_codes", CVD_URL + "|" + CVD_VERSION));

    final String body = postExpectStatus(parametersJson(library), 400);

    assertThat(singleIssue(body).getDiagnostics())
        .isEqualTo("SQL references an undeclared table: cardiovascular");
  }

  @Test
  void namingADataSourceShortNameIsA400() {
    final Library library =
        sqlQueryLibrary(
            "SELECT * FROM Condition", Map.of("cvd_codes", CVD_URL + "|" + CVD_VERSION));

    final String body = postExpectStatus(parametersJson(library), 400);

    assertThat(singleIssue(body).getDiagnostics())
        .isEqualTo("SQL references an undeclared table: Condition");
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

  /**
   * The first page of an expansion reporting five members and carrying three, one more than the
   * cap, so that the excess is known before any second page is fetched.
   */
  @Nonnull
  private static ValueSet largeExpansionFirstPage() {
    final ValueSet valueSet = new ValueSet();
    valueSet.setUrl(LARGE_URL);
    final ValueSetExpansionComponent expansion = valueSet.getExpansion();
    expansion.setTotal(5);
    for (int i = 1; i <= MAX_MEMBERS + 1; i++) {
      expansion
          .addContains()
          .setSystem(SqlValueSetTestConfiguration.SNOMED)
          .setCode(String.valueOf(i));
    }
    return valueSet;
  }

  /** A ValueSet resource carrying the given URL and version and no content, for a context entry. */
  @Nonnull
  private static ValueSet suppliedValueSet(
      @Nonnull final String url, @Nullable final String version) {
    final ValueSet valueSet = new ValueSet();
    valueSet.setUrl(url);
    valueSet.setVersion(version);
    return valueSet;
  }

  /** A minimal ViewDefinition over Condition at the given URL, as a generic JSON map. */
  @Nonnull
  private static Map<String, Object> viewDefinitionAt(@Nonnull final String url) {
    return Map.of(
        "resourceType",
        "ViewDefinition",
        "url",
        url,
        "name",
        "colliding_view",
        "status",
        "active",
        "resource",
        "Condition",
        "select",
        List.of(Map.of("column", List.of(Map.of("name", "id", "path", "id")))));
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

  /** Stubs {@code POST $expand} to return the given expansion for any supplied resource. */
  private void stubPostExpansion(@Nonnull final ValueSet response) {
    wireMockServer.stubFor(
        post(urlPathEqualTo(EXPAND_PATH))
            .willReturn(
                aResponse()
                    .withStatus(200)
                    .withHeader("Content-Type", FHIR_JSON)
                    .withBody(jsonParser.encodeResourceToString(response))));
  }

  @Nonnull
  private static List<LoggedRequest> postExpandRequests() {
    return wireMockServer.findAll(postRequestedFor(urlPathEqualTo(EXPAND_PATH)));
  }

  /**
   * Stubs {@code $expand} for the given canonical URL to fail with the given status and an
   * OperationOutcome carrying the given diagnostics.
   */
  private void stubExpansionFailure(
      @Nonnull final String url, final int status, @Nonnull final String diagnostics) {
    final OperationOutcome outcome = new OperationOutcome();
    outcome
        .addIssue()
        .setSeverity(IssueSeverity.ERROR)
        .setCode(status == 404 ? IssueType.NOTFOUND : IssueType.PROCESSING)
        .setDiagnostics(diagnostics);
    wireMockServer.stubFor(
        get(urlPathEqualTo(EXPAND_PATH))
            .withQueryParam("url", equalTo(url))
            .willReturn(
                aResponse()
                    .withStatus(status)
                    .withHeader("Content-Type", FHIR_JSON)
                    .withBody(jsonParser.encodeResourceToString(outcome))));
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

  /**
   * Builds a SQLQuery selecting everything from the stored SQLView that depends on the value set.
   */
  @Nonnull
  Library queryOverStoredSqlView() {
    return sqlQueryLibrary(
        "SELECT * FROM cvd ORDER BY patient_id",
        Map.of("cvd", SqlValueSetTestConfiguration.CVD_CONDITIONS_URL));
  }

  /** Builds a SQLQuery selecting the whole relation of the unpinned cardiovascular value set. */
  @Nonnull
  Library selectFromCvdCodes() {
    return sqlQueryLibrary("SELECT * FROM cvd_codes", Map.of("cvd_codes", CVD_URL));
  }

  /** Parses an error body as an OperationOutcome and returns its single issue. */
  @Nonnull
  OperationOutcomeIssueComponent singleIssue(@Nonnull final String body) {
    final OperationOutcome outcome = (OperationOutcome) jsonParser.parseResource(body);
    assertThat(outcome.getIssue()).hasSize(1);
    return outcome.getIssueFirstRep();
  }

  /**
   * Asserts that an error body carries exactly one issue with the given expression and diagnostics.
   */
  void assertIssue(
      @Nonnull final String body,
      @Nonnull final String expression,
      @Nonnull final String diagnostics) {
    final OperationOutcomeIssueComponent issue = singleIssue(body);
    assertThat(issue.getExpression())
        .extracting(value -> value.getValue())
        .containsExactly(expression);
    assertThat(issue.getDiagnostics()).isEqualTo(diagnostics);
  }

  /**
   * Builds an export body whose one subject is the inline semi-join over the pinned value set, with
   * the given ValueSet supplied through {@code context}.
   */
  @Nonnull
  Map<String, Object> exportOverInlineValueSet(@Nonnull final ValueSet supplied) {
    return parameters(
        subject(
            nameOf("inline"),
            resourcePart(
                "subjectResource", resourceMap(semiJoinQuery(CVD_URL + "|" + CVD_VERSION)))),
        resourcePart("context", resourceMap(supplied)),
        simpleParam("_format", "valueString", SqlQueryOutputFormat.NDJSON.getCode()));
  }

  /** The {@code name} part of an export subject. */
  @Nonnull
  Map<String, Object> nameOf(@Nonnull final String name) {
    return simpleParam("name", "valueString", name);
  }

  /** The manifest output with the given name. */
  @Nonnull
  static Map<String, Object> outputNamed(
      @Nonnull final List<Map<String, Object>> outputs, @Nonnull final String name) {
    return outputs.stream()
        .filter(output -> name.equals(partValue(output, "name", "valueString")))
        .findFirst()
        .orElseThrow(() -> new AssertionError("No output named " + name));
  }

  /** Kicks off an export expected to be rejected, asserting the status and no status URL. */
  @Nonnull
  String kickOffExpectStatus(@Nonnull final Map<String, Object> body, final int status) {
    final byte[] payload =
        kickOff(systemLevelUri(), body)
            .expectStatus()
            .isEqualTo(status)
            .expectHeader()
            .doesNotExist("Content-Location")
            .expectBody()
            .returnResult()
            .getResponseBodyContent();
    return payload == null ? "" : new String(payload, StandardCharsets.UTF_8);
  }

  /** The number of jobs the {@code $jobs} listing currently holds. */
  int jobCount() {
    final byte[] body =
        webTestClient
            .get()
            .uri("http://localhost:" + port + "/fhir/$jobs")
            .header("Accept", "application/fhir+json")
            .exchange()
            .expectStatus()
            .isOk()
            .expectBody()
            .returnResult()
            .getResponseBodyContent();
    return paramsByName(parse(body), "job").size();
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

  /**
   * Encodes a resource as the generic JSON map the Gson-built request bodies carry. Whole numbers
   * are kept as longs rather than Gson's default doubles, so that an {@code offset} or {@code
   * total} of a supplied expansion reaches the server as the integer FHIR requires and not as
   * {@code 0.0}.
   */
  @Nonnull
  @SuppressWarnings("unchecked")
  Map<String, Object> resourceMap(@Nonnull final org.hl7.fhir.r4.model.Resource resource) {
    return RESOURCE_GSON.fromJson(jsonParser.encodeResourceToString(resource), Map.class);
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
