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

import static au.csiro.pathling.operations.sql.SqlConceptMapTestConfiguration.ACUTE_MYOCARDIAL_INFARCTION;
import static au.csiro.pathling.operations.sql.SqlConceptMapTestConfiguration.DIABETES_MELLITUS;
import static au.csiro.pathling.operations.sql.SqlConceptMapTestConfiguration.FIT_AND_WELL;
import static au.csiro.pathling.operations.sql.SqlConceptMapTestConfiguration.ICD10;
import static au.csiro.pathling.operations.sql.SqlConceptMapTestConfiguration.MYOCARDIAL_INFARCTION;
import static au.csiro.pathling.operations.sql.SqlConceptMapTestConfiguration.SCT_TO_ICD10_URL;
import static au.csiro.pathling.operations.sql.SqlConceptMapTestConfiguration.SNOMED;
import static au.csiro.pathling.operations.sqlquery.SqlLibraryParser.LIBRARY_TYPE_SYSTEM;
import static au.csiro.pathling.operations.sqlquery.SqlLibraryParser.SQL_QUERY_TYPE_CODE;
import static com.github.tomakehurst.wiremock.client.WireMock.aResponse;
import static com.github.tomakehurst.wiremock.client.WireMock.absent;
import static com.github.tomakehurst.wiremock.client.WireMock.anyRequestedFor;
import static com.github.tomakehurst.wiremock.client.WireMock.anyUrl;
import static com.github.tomakehurst.wiremock.client.WireMock.equalTo;
import static com.github.tomakehurst.wiremock.client.WireMock.get;
import static com.github.tomakehurst.wiremock.client.WireMock.getRequestedFor;
import static com.github.tomakehurst.wiremock.client.WireMock.urlPathEqualTo;
import static com.github.tomakehurst.wiremock.client.WireMock.urlPathMatching;
import static org.assertj.core.api.Assertions.assertThat;

import au.csiro.pathling.operations.sqlquery.SqlQueryOutputFormat;
import ca.uhn.fhir.context.FhirContext;
import ca.uhn.fhir.parser.IParser;
import com.github.tomakehurst.wiremock.WireMockServer;
import com.github.tomakehurst.wiremock.core.WireMockConfiguration;
import com.github.tomakehurst.wiremock.verification.LoggedRequest;
import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.hl7.fhir.r4.model.Attachment;
import org.hl7.fhir.r4.model.Bundle;
import org.hl7.fhir.r4.model.Bundle.BundleType;
import org.hl7.fhir.r4.model.CodeableConcept;
import org.hl7.fhir.r4.model.Coding;
import org.hl7.fhir.r4.model.ConceptMap;
import org.hl7.fhir.r4.model.ConceptMap.ConceptMapGroupComponent;
import org.hl7.fhir.r4.model.ConceptMap.ConceptMapGroupUnmappedMode;
import org.hl7.fhir.r4.model.ConceptMap.SourceElementComponent;
import org.hl7.fhir.r4.model.Enumerations.ConceptMapEquivalence;
import org.hl7.fhir.r4.model.Enumerations.PublicationStatus;
import org.hl7.fhir.r4.model.Library;
import org.hl7.fhir.r4.model.OperationOutcome;
import org.hl7.fhir.r4.model.OperationOutcome.IssueSeverity;
import org.hl7.fhir.r4.model.OperationOutcome.IssueType;
import org.hl7.fhir.r4.model.RelatedArtifact;
import org.hl7.fhir.r4.model.RelatedArtifact.RelatedArtifactType;
import org.hl7.fhir.r4.model.ValueSet;
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
 * End-to-end integration test for concept map dependencies in SQL on FHIR queries (spec 062), in
 * SERVER terminology mode over a WireMock terminology server. Follows User Story 1 of the feature:
 * a SQLQuery left-joins a stored Condition ViewDefinition to a concept map found through a
 * ConceptMap search and read, selects the relation itself, converts every R4 equivalence, chooses
 * the pinned or the latest version, describes it, reads an empty map, translates in reverse, and
 * deduplicates and flattens content as the specification states. Every concept map is preceded by
 * one {@code $expand} of its URL, which WireMock answers {@code 404} because nothing stubs it.
 *
 * <p>Also proves that value set dependencies are unaffected: a value set that expands makes no
 * ConceptMap request, and neither does an implicit value set URL whose expansion fails.
 *
 * <p>Follows User Story 2 for a ConceptMap supplied as {@code context}: it is used without any
 * request to the terminology server, outranks a canonical the server could resolve, is matched to a
 * pinned dependency only when the versions agree, and is rejected with a {@code 400} when it
 * matches no dependency, shares its URL with another entry, or carries no URL.
 *
 * <p>Backed by {@link SqlConceptMapTestConfiguration} for the stored ViewDefinition, the Condition
 * data and the Patients. The terminology server's HTTP response cache is disabled so that every
 * request reaches WireMock and can be verified.
 *
 * @author John Grimes
 */
@Tag("IntegrationTest")
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@ResourceLock("wiremock")
@ActiveProfiles({"integration-test"})
@Import(SqlConceptMapTestConfiguration.class)
class SqlConceptMapIT extends AbstractAsyncExportIT {

  /** The pinned version of the worked example concept map. */
  static final String VERSION_2026 = "2026";

  /** An earlier version of the worked example concept map, whose diabetes target is edited. */
  static final String VERSION_2025 = "2025";

  /** The diabetes target of the edited {@code 2025} version. */
  static final String EDITED_2025_TARGET = "E11";

  /** The logical id of version {@code 2026} of the worked example on the terminology server. */
  static final String ID_2026 = "sct-to-icd10";

  /** The logical id of version {@code 2025} of the worked example on the terminology server. */
  static final String ID_2025 = "sct-to-icd10-2025";

  /** The canonical URL of a concept map whose targets carry each of the ten R4 equivalences. */
  static final String ALL_EQUIVALENCES_URL = "http://example.org/ConceptMap/all-equivalences";

  /** The canonical URL of a concept map with no groups. */
  static final String EMPTY_URL = "http://example.org/ConceptMap/empty";

  /** The canonical URL of a concept map that repeats an element and a target. */
  static final String DUPLICATES_URL = "http://example.org/ConceptMap/duplicates";

  /** The canonical URL of a concept map with unmapped, targetless and target-less content. */
  static final String SPARSE_URL = "http://example.org/ConceptMap/sparse";

  /** The canonical URL of the cardiovascular disease value set of feature 061's example. */
  static final String CVD_URL = "http://example.org/ValueSet/cardiovascular-disease";

  /** A SNOMED CT implicit value set URL. */
  static final String IMPLICIT_SNOMED_URL = SNOMED + "?fhir_vs=isa/" + DIABETES_MELLITUS;

  /** A VCL implicit value set URL. */
  static final String VCL_URL =
      "http://fhir.org/VCL?v1="
          + URLEncoder.encode("(" + SNOMED + ")inactive = true", StandardCharsets.UTF_8);

  /** The nine columns of a concept map relation, in order. */
  static final List<String> COLUMNS =
      List.of(
          "source_system",
          "source_version",
          "source_code",
          "source_display",
          "target_system",
          "target_version",
          "target_code",
          "target_display",
          "relationship");

  /** The canonical URL of a concept map the terminology server does not hold. */
  static final String LOCAL_ONLY_URL = "http://example.org/ConceptMap/local-only";

  /** The path of the {@code $expand} operation under the WireMock base. */
  static final String EXPAND_PATH = "/fhir/ValueSet/$expand";

  /** The path of the ConceptMap search under the WireMock base. */
  static final String CONCEPT_MAP_PATH = "/fhir/ConceptMap";

  private static final String FHIR_JSON = "application/fhir+json";

  private static final String SUMMARY_BUNDLE_RESOURCE =
      "/conceptmap/sct-to-icd10.summary.Bundle.json";

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
    // Version 2026 of the worked example, pinned: the summary searchset fixture, then the read.
    stubSearch(SCT_TO_ICD10_URL, VERSION_2026, jsonParser.parseResource(summaryFixture()));
    stubRead(ID_2026, workedExample2026());
  }

  // -------------------------------------------------------------------------
  // Scenario 1: a left join translates each condition, or leaves the map columns null.
  // -------------------------------------------------------------------------

  @Test
  void leftJoinTranslatesEachConditionOnceAndLeavesTheOthersNull() {
    final String body = postOk(parametersJson(leftJoinQuery(SCT_TO_ICD10_URL + "|2026")));

    final List<Map<String, Object>> rows = rows(body);
    assertThat(rows)
        .extracting(
            row ->
                row.get("patient_id")
                    + "/"
                    + row.get("code")
                    + "/"
                    + row.get("target_code")
                    + "/"
                    + row.get("relationship"))
        .containsExactly(
            "Patient/p1/" + MYOCARDIAL_INFARCTION + "/I21/equivalent",
            "Patient/p2/" + ACUTE_MYOCARDIAL_INFARCTION + "/null/null",
            "Patient/p3/" + DIABETES_MELLITUS + "/E14/source-is-broader-than-target",
            "Patient/p4/" + FIT_AND_WELL + "/null/null");
    assertSingleConceptMapResolution(SCT_TO_ICD10_URL, VERSION_2026, ID_2026);
  }

  // -------------------------------------------------------------------------
  // Scenario 2: the relation itself, one row per target and one per unmatched element.
  // -------------------------------------------------------------------------

  @Test
  void selectingTheNineColumnsReturnsTheWorkedExampleRows() {
    final String body =
        postOk(
            parametersJson(
                sqlQueryLibrary(
                    "SELECT "
                        + String.join(", ", COLUMNS)
                        + " FROM sct_to_icd10 ORDER BY source_code",
                    Map.of("sct_to_icd10", SCT_TO_ICD10_URL + "|2026"))));

    final List<Map<String, Object>> rows = rows(body);
    assertThat(rows).hasSize(3);
    assertThat(rows.get(0))
        .containsEntry("source_system", SNOMED)
        .containsEntry("source_code", FIT_AND_WELL)
        .containsEntry("source_display", "Fit and well")
        .containsEntry("target_system", ICD10)
        .containsEntry("target_version", "2019");
    assertThat(rows.get(0).get("target_code")).isNull();
    assertThat(rows.get(0).get("target_display")).isNull();
    assertThat(rows.get(0).get("relationship")).isNull();
    assertThat(rows.get(1))
        .containsEntry("source_system", SNOMED)
        .containsEntry("source_code", MYOCARDIAL_INFARCTION)
        .containsEntry("source_display", "Myocardial infarction")
        .containsEntry("target_system", ICD10)
        .containsEntry("target_version", "2019")
        .containsEntry("target_code", "I21")
        .containsEntry("target_display", "Acute myocardial infarction")
        .containsEntry("relationship", "equivalent");
    assertThat(rows.get(2))
        .containsEntry("source_code", DIABETES_MELLITUS)
        .containsEntry("source_display", "Diabetes mellitus")
        .containsEntry("target_code", "E14")
        .containsEntry("target_display", "Unspecified diabetes mellitus")
        .containsEntry("relationship", "source-is-broader-than-target");
    for (final Map<String, Object> row : rows) {
      assertThat(row.get("source_version")).isNull();
    }
  }

  // -------------------------------------------------------------------------
  // Scenario 3: every R4 equivalence converts to its relationship.
  // -------------------------------------------------------------------------

  @Test
  void everyEquivalenceIsConvertedToItsRelationship() {
    final ConceptMap conceptMap = conceptMap(ALL_EQUIVALENCES_URL, "1");
    final ConceptMapGroupComponent group = conceptMap.addGroup().setSource(SNOMED).setTarget(ICD10);
    for (final ConceptMapEquivalence equivalence :
        List.of(
            ConceptMapEquivalence.EQUAL,
            ConceptMapEquivalence.EQUIVALENT,
            ConceptMapEquivalence.WIDER,
            ConceptMapEquivalence.SUBSUMES,
            ConceptMapEquivalence.NARROWER,
            ConceptMapEquivalence.SPECIALIZES,
            ConceptMapEquivalence.RELATEDTO,
            ConceptMapEquivalence.INEXACT,
            ConceptMapEquivalence.DISJOINT)) {
      group
          .addElement()
          .setCode(equivalence.toCode())
          .addTarget()
          .setCode("T")
          .setEquivalence(equivalence);
    }
    group
        .addElement()
        .setCode("unmatched")
        .addTarget()
        .setCode("ignored")
        .setEquivalence(ConceptMapEquivalence.UNMATCHED);
    stubMap(conceptMap, "all-equivalences");

    final String body =
        postOk(
            parametersJson(
                sqlQueryLibrary(
                    "SELECT source_code, target_code, relationship FROM m",
                    Map.of("m", ALL_EQUIVALENCES_URL))));

    final Map<Object, Map<String, Object>> bySource =
        rows(body).stream()
            .collect(Collectors.toMap(row -> row.get("source_code"), Function.identity()));
    assertThat(bySource).hasSize(10);
    assertThat(relationshipsOf(bySource))
        .containsEntry("equal", "equivalent")
        .containsEntry("equivalent", "equivalent")
        .containsEntry("wider", "source-is-narrower-than-target")
        .containsEntry("subsumes", "source-is-narrower-than-target")
        .containsEntry("narrower", "source-is-broader-than-target")
        .containsEntry("specializes", "source-is-broader-than-target")
        .containsEntry("relatedto", "related-to")
        .containsEntry("inexact", "related-to")
        .containsEntry("disjoint", "not-related-to")
        .containsEntry("unmatched", "null");
    assertThat(bySource.get("unmatched").get("target_code")).isNull();
    assertThat(bySource.get("equal").get("target_code")).isEqualTo("T");
  }

  // -------------------------------------------------------------------------
  // Scenario 5: a pinned reference reads exactly that version.
  // -------------------------------------------------------------------------

  @Test
  void pinnedReferenceReadsThatVersionAndNoOther() {
    stubSearch(SCT_TO_ICD10_URL, VERSION_2025, searchset(summaryOf(workedExample2025(), ID_2025)));
    stubRead(ID_2025, workedExample2025());

    final String body = postOk(parametersJson(selectTargets(SCT_TO_ICD10_URL + "|2025")));

    assertThat(rowsOf(body, "source_code", "target_code"))
        .containsExactly(
            FIT_AND_WELL + "/null",
            MYOCARDIAL_INFARCTION + "/I21",
            DIABETES_MELLITUS + "/" + EDITED_2025_TARGET);
    assertSingleConceptMapResolution(SCT_TO_ICD10_URL, VERSION_2025, ID_2025);
    assertThat(readRequests(ID_2026)).isEmpty();
  }

  // -------------------------------------------------------------------------
  // Scenario 6: an unpinned reference reads the latest version only.
  // -------------------------------------------------------------------------

  @Test
  void unpinnedReferenceReadsTheLatestVersionOnly() {
    stubSearch(
        SCT_TO_ICD10_URL,
        null,
        searchset(
            summaryOf(workedExample2025(), ID_2025), summaryOf(workedExample2026(), ID_2026)));
    stubRead(ID_2025, workedExample2025());

    final String body = postOk(parametersJson(selectTargets(SCT_TO_ICD10_URL)));

    assertThat(rowsOf(body, "source_code", "target_code"))
        .containsExactly(
            FIT_AND_WELL + "/null", MYOCARDIAL_INFARCTION + "/I21", DIABETES_MELLITUS + "/E14");
    assertSingleConceptMapResolution(SCT_TO_ICD10_URL, null, ID_2026);
    assertThat(readRequests(ID_2025)).as("No read of the unchosen version").isEmpty();
  }

  // -------------------------------------------------------------------------
  // Scenario 7: DESCRIBE lists the nine string columns.
  // -------------------------------------------------------------------------

  @Test
  void describesTheConceptMap() {
    final String body =
        postOk(
            parametersJson(
                sqlQueryLibrary(
                    "DESCRIBE sct_to_icd10", Map.of("sct_to_icd10", SCT_TO_ICD10_URL + "|2026"))));

    assertThat(rowsOf(body, "col_name", "data_type"))
        .containsExactlyElementsOf(COLUMNS.stream().map(column -> column + "/string").toList());
  }

  // -------------------------------------------------------------------------
  // Scenario 8: a concept map with no mappings is an empty relation, not an error.
  // -------------------------------------------------------------------------

  @Test
  void conceptMapWithNoMappingsYieldsNoRowsAndNoError() {
    stubMap(conceptMap(EMPTY_URL, "1"), "empty");

    final String body =
        postOk(
            parametersJson(
                sqlQueryLibrary("SELECT * FROM empty_map", Map.of("empty_map", EMPTY_URL))));

    assertThat(body.trim()).isEmpty();
  }

  // -------------------------------------------------------------------------
  // Scenario 9: joining on the target columns translates in reverse from the same relation.
  // -------------------------------------------------------------------------

  @Test
  void joiningOnTheTargetColumnsTranslatesInReverse() {
    final Map<String, String> dependencies = new LinkedHashMap<>();
    dependencies.put("conditions", SqlConceptMapTestConfiguration.CONDITION_VIEW_URL);
    dependencies.put("sct_to_icd10", SCT_TO_ICD10_URL + "|2026");
    final Library library =
        sqlQueryLibrary(
            "SELECT conditions.patient_id, sct_to_icd10.source_code FROM conditions"
                + " JOIN sct_to_icd10"
                + " ON sct_to_icd10.target_system = conditions.system"
                + " AND sct_to_icd10.target_code = conditions.code",
            dependencies);

    final String body = postOk(parametersJson(library));

    assertThat(rowsOf(body, "patient_id", "source_code"))
        .containsExactly("Patient/p2/" + MYOCARDIAL_INFARCTION);
  }

  // -------------------------------------------------------------------------
  // Scenario 10: a value set that expands makes no ConceptMap request.
  // -------------------------------------------------------------------------

  @Test
  void valueSetThatExpandsMakesNoConceptMapRequest() {
    stubExpansion(CVD_URL, cvdExpansion());
    final Map<String, String> dependencies = new LinkedHashMap<>();
    dependencies.put("conditions", SqlConceptMapTestConfiguration.CONDITION_VIEW_URL);
    dependencies.put("cvd_codes", CVD_URL);
    final Library library =
        sqlQueryLibrary(
            "SELECT conditions.patient_id, conditions.code FROM conditions WHERE EXISTS (SELECT 1"
                + " FROM cvd_codes WHERE cvd_codes.system = conditions.system AND cvd_codes.code ="
                + " conditions.code) ORDER BY conditions.id",
            dependencies);

    final String body = postOk(parametersJson(library));

    assertThat(rowsOf(body, "patient_id", "code"))
        .containsExactly(
            "Patient/p1/" + MYOCARDIAL_INFARCTION, "Patient/p2/" + ACUTE_MYOCARDIAL_INFARCTION);
    assertThat(expandRequests(CVD_URL)).hasSize(1);
    assertThat(conceptMapRequests()).isEmpty();
  }

  // -------------------------------------------------------------------------
  // Scenario 11: a repeated element and a repeated target are one row, first displays kept.
  // -------------------------------------------------------------------------

  @Test
  void repeatedElementsAndTargetsAreOneRowKeepingTheFirstDisplays() {
    final ConceptMap conceptMap = conceptMap(DUPLICATES_URL, "1");
    final ConceptMapGroupComponent group = conceptMap.addGroup().setSource(SNOMED).setTarget(ICD10);
    final SourceElementComponent first =
        group.addElement().setCode(MYOCARDIAL_INFARCTION).setDisplay("First display");
    first
        .addTarget()
        .setCode("I21")
        .setDisplay("First target display")
        .setEquivalence(ConceptMapEquivalence.EQUIVALENT);
    first
        .addTarget()
        .setCode("I21")
        .setDisplay("Second target display")
        .setEquivalence(ConceptMapEquivalence.EQUIVALENT);
    group
        .addElement()
        .setCode(MYOCARDIAL_INFARCTION)
        .setDisplay("Second display")
        .addTarget()
        .setCode("I21")
        .setDisplay("Third target display")
        .setEquivalence(ConceptMapEquivalence.EQUIVALENT);
    stubMap(conceptMap, "duplicates");

    final String body =
        postOk(parametersJson(sqlQueryLibrary("SELECT * FROM m", Map.of("m", DUPLICATES_URL))));

    final List<Map<String, Object>> rows = rows(body);
    assertThat(rows).hasSize(1);
    assertThat(rows.get(0))
        .containsEntry("source_code", MYOCARDIAL_INFARCTION)
        .containsEntry("source_display", "First display")
        .containsEntry("target_code", "I21")
        .containsEntry("target_display", "First target display");
  }

  // -------------------------------------------------------------------------
  // Scenario 12: unmapped and targetless elements add nothing; a group without a target is null.
  // -------------------------------------------------------------------------

  @Test
  void unmappedAndTargetlessContentAddNoRowsAndATargetlessGroupHasNullTargetColumns() {
    final ConceptMap conceptMap = conceptMap(SPARSE_URL, "1");
    final ConceptMapGroupComponent withTarget =
        conceptMap.addGroup().setSource(SNOMED).setTarget(ICD10).setTargetVersion("2019");
    withTarget.getUnmapped().setMode(ConceptMapGroupUnmappedMode.FIXED).setCode("R69");
    withTarget.addElement().setCode(FIT_AND_WELL);
    withTarget
        .addElement()
        .setCode(MYOCARDIAL_INFARCTION)
        .addTarget()
        .setCode("I21")
        .setEquivalence(ConceptMapEquivalence.EQUIVALENT);
    conceptMap
        .addGroup()
        .setSource(SNOMED)
        .addElement()
        .setCode(DIABETES_MELLITUS)
        .addTarget()
        .setCode("44054006")
        .setEquivalence(ConceptMapEquivalence.WIDER);
    stubMap(conceptMap, "sparse");

    final String body =
        postOk(
            parametersJson(
                sqlQueryLibrary("SELECT * FROM m ORDER BY source_code", Map.of("m", SPARSE_URL))));

    final List<Map<String, Object>> rows = rows(body);
    assertThat(rows)
        .extracting(row -> row.get("source_code"))
        .containsExactly(MYOCARDIAL_INFARCTION, DIABETES_MELLITUS);
    assertThat(rows.get(0))
        .containsEntry("target_system", ICD10)
        .containsEntry("target_version", "2019");
    assertThat(rows.get(1).get("target_system")).isNull();
    assertThat(rows.get(1).get("target_version")).isNull();
    assertThat(rows.get(1)).containsEntry("target_code", "44054006");
  }

  // -------------------------------------------------------------------------
  // Scenario 13: an implicit value set URL is never searched for as a concept map.
  // -------------------------------------------------------------------------

  @Test
  void implicitSnomedValueSetWhoseExpansionIsNotFoundIsA404WithNoConceptMapSearch() {
    postExpectStatus(parametersJson(selectAll("t2dm", IMPLICIT_SNOMED_URL)), 404);

    assertThat(expandRequests(IMPLICIT_SNOMED_URL)).hasSize(1);
    assertThat(conceptMapRequests()).isEmpty();
  }

  @Test
  void vclValueSetWhoseExpansionIsNotFoundIsA404WithNoConceptMapSearch() {
    postExpectStatus(parametersJson(selectAll("inactive_codes", VCL_URL)), 404);

    assertThat(expandRequests(VCL_URL)).hasSize(1);
    assertThat(conceptMapRequests()).isEmpty();
  }

  @Test
  void implicitSnomedValueSetWhoseExpansionFailsIsA422WithNoConceptMapSearch() {
    stubExpansionFailure(IMPLICIT_SNOMED_URL, 500, "boom");

    postExpectStatus(parametersJson(selectAll("t2dm", IMPLICIT_SNOMED_URL)), 422);

    assertThat(conceptMapRequests()).isEmpty();
  }

  @Test
  void vclValueSetWhoseExpansionFailsIsA422WithNoConceptMapSearch() {
    stubExpansionFailure(VCL_URL, 400, "Unable to expand");

    postExpectStatus(parametersJson(selectAll("inactive_codes", VCL_URL)), 422);

    assertThat(conceptMapRequests()).isEmpty();
  }

  // -------------------------------------------------------------------------
  // US2 scenario 1: a supplied concept map is used and the terminology layer is not consulted.
  // -------------------------------------------------------------------------

  @Test
  void suppliedConceptMapIsUsedWithoutAnyTerminologyRequest() {
    final ConceptMap supplied = workedExample2026();
    supplied.setId((String) null);
    supplied.setUrl(LOCAL_ONLY_URL);

    final String body =
        postOk(
            parametersJson(
                selectTargets(LOCAL_ONLY_URL), resourcePart("context", resourceMap(supplied))));

    assertThat(rowsOf(body, "source_code", "target_code"))
        .containsExactly(
            FIT_AND_WELL + "/null", MYOCARDIAL_INFARCTION + "/I21", DIABETES_MELLITUS + "/E14");
    assertThat(wireMockServer.findAll(anyRequestedFor(anyUrl())))
        .as("No request to the terminology server")
        .isEmpty();
  }

  // -------------------------------------------------------------------------
  // US2 scenario 2: a supplied concept map outranks a canonical the server could resolve.
  // -------------------------------------------------------------------------

  @Test
  void suppliedConceptMapOutranksAResolvableCanonical() {
    // The server holds version 2026 of the worked example, whose diabetes target is E14; the
    // supplied copy maps diabetes to E11 instead, and it is the supplied content that is used.
    final ConceptMap supplied = editedWorkedExample(VERSION_2026);

    final String body =
        postOk(
            parametersJson(
                selectTargets(SCT_TO_ICD10_URL + "|" + VERSION_2026),
                resourcePart("context", resourceMap(supplied))));

    assertThat(rowsOf(body, "source_code", "target_code"))
        .containsExactly(
            FIT_AND_WELL + "/null",
            MYOCARDIAL_INFARCTION + "/I21",
            DIABETES_MELLITUS + "/" + EDITED_2025_TARGET);
    assertThat(wireMockServer.findAll(anyRequestedFor(anyUrl())))
        .as("No request to the terminology server")
        .isEmpty();
  }

  // -------------------------------------------------------------------------
  // US2 scenario 3: a pinned dependency matches a supplied map only when the versions agree.
  // -------------------------------------------------------------------------

  @Test
  void pinnedDependencyMatchesASuppliedConceptMapWithTheSameVersion() {
    final ConceptMap supplied = editedWorkedExample(VERSION_2025);

    final String body =
        postOk(
            parametersJson(
                selectTargets(SCT_TO_ICD10_URL + "|" + VERSION_2025),
                resourcePart("context", resourceMap(supplied))));

    assertThat(rowsOf(body, "source_code", "target_code"))
        .contains(DIABETES_MELLITUS + "/" + EDITED_2025_TARGET);
    assertThat(conceptMapRequests()).isEmpty();
    assertThat(expandRequests(SCT_TO_ICD10_URL)).isEmpty();
  }

  @Test
  void pinnedDependencyIgnoresASuppliedConceptMapAtAnotherVersionAndConsultsTheServer() {
    // The supplied 2025 map satisfies the dependency pinned to 2025 only; the dependency pinned to
    // 2026 falls through to the terminology server, which is asked for 2026.
    final ConceptMap supplied = editedWorkedExample(VERSION_2025);
    final Map<String, String> dependencies = new LinkedHashMap<>();
    dependencies.put("latest_map", SCT_TO_ICD10_URL + "|" + VERSION_2026);
    dependencies.put("earlier_map", SCT_TO_ICD10_URL + "|" + VERSION_2025);
    final Library library =
        sqlQueryLibrary(
            "SELECT 'latest' AS source, target_code FROM latest_map"
                + " WHERE source_code = '"
                + DIABETES_MELLITUS
                + "' UNION ALL SELECT 'earlier' AS source, target_code FROM earlier_map"
                + " WHERE source_code = '"
                + DIABETES_MELLITUS
                + "' ORDER BY source",
            dependencies);

    final String body =
        postOk(parametersJson(library, resourcePart("context", resourceMap(supplied))));

    assertThat(rowsOf(body, "source", "target_code"))
        .containsExactly("earlier/" + EDITED_2025_TARGET, "latest/E14");
    assertSingleConceptMapResolution(SCT_TO_ICD10_URL, VERSION_2026, ID_2026);
  }

  // -------------------------------------------------------------------------
  // US2 scenario 5: a supplied concept map matching no dependency is a 400.
  // -------------------------------------------------------------------------

  @Test
  void unmatchedSuppliedConceptMapIsRejected() {
    final ConceptMap unrelated = conceptMap(LOCAL_ONLY_URL, "1");

    final String body =
        postExpectStatus(
            parametersJson(
                selectTargets(SCT_TO_ICD10_URL + "|" + VERSION_2026),
                resourcePart("context", resourceMap(unrelated))),
            400);

    assertThat(body).contains("match no dependency of any subject", LOCAL_ONLY_URL);
  }

  // -------------------------------------------------------------------------
  // US2 scenario 6: a ValueSet and a ConceptMap sharing a URL are a 400.
  // -------------------------------------------------------------------------

  @Test
  void aValueSetAndAConceptMapSharingAUrlAreRejected() {
    final ValueSet valueSet = new ValueSet();
    valueSet.setUrl(SCT_TO_ICD10_URL);
    valueSet.getExpansion().addContains().setSystem(SNOMED).setCode(MYOCARDIAL_INFARCTION);

    final String body =
        postExpectStatus(
            parametersJson(
                selectTargets(SCT_TO_ICD10_URL),
                resourcePart("context", resourceMap(valueSet)),
                resourcePart("context", resourceMap(editedWorkedExample(VERSION_2026)))),
            400);

    assertThat(body).contains("share the canonical URL", SCT_TO_ICD10_URL);
    assertThat(wireMockServer.findAll(anyRequestedFor(anyUrl()))).isEmpty();
  }

  // -------------------------------------------------------------------------
  // US2 scenario 7: a supplied concept map without a url is a 400.
  // -------------------------------------------------------------------------

  @Test
  void suppliedConceptMapWithoutAUrlIsRejected() {
    final ConceptMap supplied = editedWorkedExample(VERSION_2026);
    supplied.setUrl(null);

    final String body =
        postExpectStatus(
            parametersJson(
                selectTargets(SCT_TO_ICD10_URL + "|" + VERSION_2026),
                resourcePart("context", resourceMap(supplied))),
            400);

    assertThat(body)
        .contains(
            "A 'context' ConceptMap must carry a url, since entries are matched to dependencies by"
                + " canonical URL.");
    assertThat(wireMockServer.findAll(anyRequestedFor(anyUrl()))).isEmpty();
  }

  // -------------------------------------------------------------------------
  // Fixtures
  // -------------------------------------------------------------------------

  /** Version {@code 2026} of the worked example, as the terminology server holds it. */
  @Nonnull
  private ConceptMap workedExample2026() {
    final ConceptMap conceptMap = SqlConceptMapTestConfiguration.workedExample(fhirContext);
    conceptMap.setId(ID_2026);
    return conceptMap;
  }

  /**
   * Version {@code 2025} of the worked example, whose diabetes element maps to a different target,
   * so that the rows show which version was read.
   */
  @Nonnull
  private ConceptMap workedExample2025() {
    final ConceptMap conceptMap = SqlConceptMapTestConfiguration.workedExample(fhirContext);
    conceptMap.setId(ID_2025);
    conceptMap.setVersion(VERSION_2025);
    conceptMap.getGroupFirstRep().getElement().stream()
        .filter(element -> DIABETES_MELLITUS.equals(element.getCode()))
        .findFirst()
        .orElseThrow()
        .getTargetFirstRep()
        .setCode(EDITED_2025_TARGET);
    return conceptMap;
  }

  /**
   * A copy of the worked example at the given version, as a client would supply it, whose diabetes
   * element maps to {@link #EDITED_2025_TARGET} so that the rows show the supplied content was
   * used.
   */
  @Nonnull
  private ConceptMap editedWorkedExample(@Nonnull final String version) {
    final ConceptMap conceptMap = workedExample2025();
    conceptMap.setId((String) null);
    conceptMap.setVersion(version);
    return conceptMap;
  }

  /** The summary searchset fixture for the pinned {@code 2026} search. */
  @Nonnull
  private static String summaryFixture() {
    try (final InputStream stream =
        SqlConceptMapIT.class.getResourceAsStream(SUMMARY_BUNDLE_RESOURCE)) {
      return new String(Objects.requireNonNull(stream).readAllBytes(), StandardCharsets.UTF_8);
    } catch (final IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  /** A ConceptMap carrying the given URL and version and no groups. */
  @Nonnull
  private static ConceptMap conceptMap(@Nonnull final String url, @Nonnull final String version) {
    final ConceptMap conceptMap = new ConceptMap();
    conceptMap.setUrl(url);
    conceptMap.setVersion(version);
    conceptMap.setStatus(PublicationStatus.ACTIVE);
    return conceptMap;
  }

  /** The summary form of a ConceptMap under the given logical id: identity only, no groups. */
  @Nonnull
  private static ConceptMap summaryOf(@Nonnull final ConceptMap full, @Nonnull final String id) {
    final ConceptMap summary = conceptMap(full.getUrl(), full.getVersion());
    summary.setId(id);
    return summary;
  }

  /** A searchset Bundle holding the given resources as matches. */
  @Nonnull
  private static Bundle searchset(@Nonnull final ConceptMap... summaries) {
    final Bundle bundle = new Bundle();
    bundle.setType(BundleType.SEARCHSET);
    bundle.setTotal(summaries.length);
    for (final ConceptMap summary : summaries) {
      bundle.addEntry().setResource(summary);
    }
    return bundle;
  }

  /** The cardiovascular disease expansion of feature 061's example, without its grouping entry. */
  @Nonnull
  private static ValueSet cvdExpansion() {
    final ValueSet valueSet = new ValueSet();
    valueSet.setUrl(CVD_URL);
    valueSet.getExpansion().addContains().setSystem(SNOMED).setCode(MYOCARDIAL_INFARCTION);
    valueSet.getExpansion().addContains().setSystem(ICD10).setCode(ACUTE_MYOCARDIAL_INFARCTION);
    return valueSet;
  }

  /** Stubs a concept map reached unpinned: its one-entry summary search and its read. */
  private void stubMap(@Nonnull final ConceptMap conceptMap, @Nonnull final String id) {
    conceptMap.setId(id);
    stubSearch(conceptMap.getUrl(), null, searchset(summaryOf(conceptMap, id)));
    stubRead(id, conceptMap);
  }

  /**
   * Stubs the ConceptMap summary search for the given URL and, where non-null, version; where the
   * version is null, the stub matches only a search that carries none.
   */
  private void stubSearch(
      @Nonnull final String url,
      @Nullable final String version,
      @Nonnull final org.hl7.fhir.instance.model.api.IBaseResource bundle) {
    var mapping =
        get(urlPathEqualTo(CONCEPT_MAP_PATH))
            .withQueryParam("url", equalTo(url))
            .withQueryParam("_summary", equalTo("true"));
    mapping =
        version == null
            ? mapping.withQueryParam("version", absent())
            : mapping.withQueryParam("version", equalTo(version));
    wireMockServer.stubFor(
        mapping.willReturn(
            aResponse()
                .withStatus(200)
                .withHeader("Content-Type", FHIR_JSON)
                .withBody(jsonParser.encodeResourceToString(bundle))));
  }

  /** Stubs the read of one ConceptMap by logical id. */
  private void stubRead(@Nonnull final String id, @Nonnull final ConceptMap conceptMap) {
    wireMockServer.stubFor(
        get(urlPathEqualTo(CONCEPT_MAP_PATH + "/" + id))
            .willReturn(
                aResponse()
                    .withStatus(200)
                    .withHeader("Content-Type", FHIR_JSON)
                    .withBody(jsonParser.encodeResourceToString(conceptMap))));
  }

  /** Stubs {@code $expand} of the given canonical URL to return the given expansion. */
  private void stubExpansion(@Nonnull final String url, @Nonnull final ValueSet response) {
    wireMockServer.stubFor(
        get(urlPathEqualTo(EXPAND_PATH))
            .withQueryParam("url", equalTo(url))
            .willReturn(
                aResponse()
                    .withStatus(200)
                    .withHeader("Content-Type", FHIR_JSON)
                    .withBody(jsonParser.encodeResourceToString(response))));
  }

  /**
   * Stubs {@code $expand} of the given canonical URL to fail with the given status and an
   * OperationOutcome carrying the given diagnostics.
   */
  private void stubExpansionFailure(
      @Nonnull final String url, final int status, @Nonnull final String diagnostics) {
    final OperationOutcome outcome = new OperationOutcome();
    outcome
        .addIssue()
        .setSeverity(IssueSeverity.ERROR)
        .setCode(IssueType.PROCESSING)
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
  // Request log helpers
  // -------------------------------------------------------------------------

  /**
   * Asserts that the terminology server saw exactly the requests one concept map resolution makes:
   * one {@code $expand} of its URL, one summary search carrying exactly the URL, the version where
   * pinned and {@code _summary=true}, and one read, of the chosen resource.
   */
  private static void assertSingleConceptMapResolution(
      @Nonnull final String url, @Nullable final String version, @Nonnull final String readId) {
    assertThat(expandRequests(url)).as("One $expand of the map's URL").hasSize(1);
    final List<LoggedRequest> searches = searchRequests();
    assertThat(searches).as("One summary search").hasSize(1);
    final LoggedRequest search = searches.get(0);
    if (version == null) {
      assertThat(search.getQueryParams().keySet()).containsExactlyInAnyOrder("url", "_summary");
    } else {
      assertThat(search.getQueryParams().keySet())
          .containsExactlyInAnyOrder("url", "version", "_summary");
      assertThat(search.queryParameter("version").firstValue()).isEqualTo(version);
    }
    assertThat(search.queryParameter("url").firstValue()).isEqualTo(url);
    assertThat(search.queryParameter("_summary").firstValue()).isEqualTo("true");
    assertThat(readRequests(readId)).as("One read of the chosen map").hasSize(1);
    assertThat(wireMockServer.findAll(anyRequestedFor(anyUrl()))).as("No other request").hasSize(3);
  }

  @Nonnull
  private static List<LoggedRequest> expandRequests(@Nonnull final String url) {
    return wireMockServer.findAll(
        getRequestedFor(urlPathEqualTo(EXPAND_PATH)).withQueryParam("url", equalTo(url)));
  }

  @Nonnull
  private static List<LoggedRequest> searchRequests() {
    return wireMockServer.findAll(getRequestedFor(urlPathEqualTo(CONCEPT_MAP_PATH)));
  }

  @Nonnull
  private static List<LoggedRequest> readRequests(@Nonnull final String id) {
    return wireMockServer.findAll(getRequestedFor(urlPathEqualTo(CONCEPT_MAP_PATH + "/" + id)));
  }

  /** Every request to the ConceptMap endpoint, searches and reads alike. */
  @Nonnull
  private static List<LoggedRequest> conceptMapRequests() {
    return wireMockServer.findAll(anyRequestedFor(urlPathMatching(CONCEPT_MAP_PATH + "(/.*)?")));
  }

  // -------------------------------------------------------------------------
  // Request helpers
  // -------------------------------------------------------------------------

  /** Builds the Scenario 1 SQLQuery left-joining the Condition view to the given concept map. */
  @Nonnull
  Library leftJoinQuery(@Nonnull final String conceptMapCanonical) {
    final Map<String, String> dependencies = new LinkedHashMap<>();
    dependencies.put("conditions", SqlConceptMapTestConfiguration.CONDITION_VIEW_URL);
    dependencies.put("sct_to_icd10", conceptMapCanonical);
    return sqlQueryLibrary(
        "SELECT conditions.patient_id, conditions.code, sct_to_icd10.target_code,"
            + " sct_to_icd10.relationship"
            + " FROM conditions"
            + " LEFT JOIN sct_to_icd10"
            + " ON sct_to_icd10.source_system = conditions.system"
            + " AND sct_to_icd10.source_code = conditions.code"
            + " AND (sct_to_icd10.relationship IS NULL"
            + " OR sct_to_icd10.relationship <> 'not-related-to')"
            + " ORDER BY conditions.id",
        dependencies);
  }

  /** Builds a SQLQuery selecting each source and target code of the given concept map. */
  @Nonnull
  Library selectTargets(@Nonnull final String conceptMapCanonical) {
    return sqlQueryLibrary(
        "SELECT source_code, target_code FROM sct_to_icd10 ORDER BY source_code",
        Map.of("sct_to_icd10", conceptMapCanonical));
  }

  /** Builds a SQLQuery selecting everything from the given dependency. */
  @Nonnull
  Library selectAll(@Nonnull final String label, @Nonnull final String canonical) {
    return sqlQueryLibrary("SELECT * FROM " + label, Map.of(label, canonical));
  }

  /** Maps each row's source code to its relationship, rendering a null relationship as "null". */
  @Nonnull
  private static Map<Object, String> relationshipsOf(
      @Nonnull final Map<Object, Map<String, Object>> bySource) {
    return bySource.entrySet().stream()
        .collect(
            Collectors.toMap(
                Map.Entry::getKey, entry -> String.valueOf(entry.getValue().get("relationship"))));
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
