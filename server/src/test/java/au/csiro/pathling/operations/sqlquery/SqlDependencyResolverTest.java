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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import au.csiro.pathling.config.AuthorizationConfiguration;
import au.csiro.pathling.config.ExternalTableConfiguration;
import au.csiro.pathling.config.ServerConfiguration;
import au.csiro.pathling.config.SqlQueryConfiguration;
import au.csiro.pathling.operations.sql.SubjectResolver;
import au.csiro.pathling.operations.sql.SuppliedArtefact;
import au.csiro.pathling.operations.sql.SuppliedArtefacts;
import au.csiro.pathling.terminology.conceptmap.ConceptMapContent;
import au.csiro.pathling.terminology.conceptmap.ConceptMapRelationship;
import au.csiro.pathling.terminology.conceptmap.ConceptMapping;
import au.csiro.pathling.terminology.expand.ValueSetExpansion;
import au.csiro.pathling.terminology.expand.ValueSetMember;
import au.csiro.pathling.views.FhirView;
import ca.uhn.fhir.rest.server.exceptions.BaseServerResponseException;
import ca.uhn.fhir.rest.server.exceptions.InvalidRequestException;
import ca.uhn.fhir.rest.server.exceptions.ResourceNotFoundException;
import ca.uhn.fhir.rest.server.exceptions.UnprocessableEntityException;
import jakarta.annotation.Nonnull;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.hl7.fhir.r4.model.ConceptMap;
import org.hl7.fhir.r4.model.Library;
import org.hl7.fhir.r4.model.OperationOutcome;
import org.hl7.fhir.r4.model.OperationOutcome.IssueType;
import org.hl7.fhir.r4.model.OperationOutcome.OperationOutcomeIssueComponent;
import org.hl7.fhir.r4.model.StringType;
import org.hl7.fhir.r4.model.ValueSet;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.InOrder;

/**
 * Unit tests for {@link SqlDependencyResolver} covering canonical-URL resolution, the resolved
 * graph shape for a {@code SQLQuery -> SQLView -> ViewDefinition} chain, supplied-artefact
 * precedence and traversal, diamond de-duplication (including bare-url vs {@code url|version}),
 * configured external tables, value sets and then concept maps resolved through the terminology
 * layer as a last resort, the found, absent and unknown outcomes of those lookups and the kept
 * failures they report, value sets supplied inline through {@code context}, and the structural
 * rejections (cycles, depth, ambiguity, not-found, and wrong-typed dependencies).
 *
 * @author John Grimes
 */
class SqlDependencyResolverTest {

  private static final String PATIENT_VIEW_URL =
      SqlLibraryFixtures.viewDefinitionUrl("patient-view");

  private static final String TABLE_URL = "https://example.org/data/cohorts";

  private static final String TABLE_PATH = "file:///data/reference/cohorts";

  private static final String VALUE_SET_URL = "http://example.org/ValueSet/cardiovascular-disease";

  private static final String CONCEPT_MAP_URL = "http://example.org/ConceptMap/sct-to-icd10";

  private static final String VALUE_SET_ISSUE = "expanding it as a value set failed";

  private static final String CONCEPT_MAP_ISSUE = "searching for it as a concept map failed";

  private ViewResolver viewResolver;
  private LibraryReferenceResolver libraryReferenceResolver;
  private ValueSetMembershipResolver valueSetResolver;
  private ConceptMapResolver conceptMapResolver;
  private ServerConfiguration serverConfiguration;
  private SqlDependencyResolver resolver;

  @BeforeEach
  void setUp() {
    viewResolver = mock(ViewResolver.class);
    libraryReferenceResolver = mock(LibraryReferenceResolver.class);
    valueSetResolver = mock(ValueSetMembershipResolver.class);
    when(valueSetResolver.resolveCanonical(any(), any())).thenReturn(Optional.empty());
    conceptMapResolver = mock(ConceptMapResolver.class);
    when(conceptMapResolver.resolveCanonical(any(), any())).thenReturn(Optional.empty());
    serverConfiguration = new ServerConfiguration();
    final AuthorizationConfiguration auth = new AuthorizationConfiguration();
    auth.setEnabled(false);
    serverConfiguration.setAuth(auth);
    serverConfiguration.setSqlQuery(new SqlQueryConfiguration());
    resolver = newResolver();
  }

  // ---------------------------------------------------------------------------
  // Canonical-URL resolution (US1).
  // ---------------------------------------------------------------------------

  @Test
  void resolvesAViewDefinitionByUrl() {
    stubStoredViewDefinition(PATIENT_VIEW_URL, PATIENT_VIEW_URL, "Patient");

    final ResolvedDependencyGraph graph =
        resolver.resolve(
            sqlQuery("SELECT * FROM p", "p", PATIENT_VIEW_URL), SuppliedArtefacts.empty());

    assertThat(graph.getTopLevelKeysByLabel()).containsEntry("p", PATIENT_VIEW_URL);
    assertThat(graph.getNodesByKey().get(PATIENT_VIEW_URL))
        .isInstanceOf(ResolvedViewDefinition.class);
  }

  @Test
  void resolvesASqlViewByUrl() {
    final String baseUrl = SqlLibraryFixtures.sqlViewUrl("base");
    stubSqlView(baseUrl, "SELECT * FROM pv", "pv", PATIENT_VIEW_URL);
    stubStoredViewDefinition(PATIENT_VIEW_URL, PATIENT_VIEW_URL, "Patient");

    final ResolvedDependencyGraph graph =
        resolver.resolve(sqlQuery("SELECT * FROM b", "b", baseUrl), SuppliedArtefacts.empty());

    assertThat(graph.getTopLevelKeysByLabel()).containsEntry("b", baseUrl);
    assertThat(graph.getNodesByKey().get(baseUrl)).isInstanceOf(ResolvedSqlView.class);
    assertThat(graph.getNodesByKey().get(PATIENT_VIEW_URL))
        .isInstanceOf(ResolvedViewDefinition.class);
  }

  @Test
  void recursesThroughASqlViewUrlDependencies() {
    // SQLQuery -> v1 (SQLView) -> v2 (SQLView) -> ViewDefinition, all by canonical URL.
    final String v1Url = SqlLibraryFixtures.sqlViewUrl("v1");
    final String v2Url = SqlLibraryFixtures.sqlViewUrl("v2");
    stubSqlView(v1Url, "SELECT * FROM x", "x", v2Url);
    stubSqlView(v2Url, "SELECT * FROM pv", "pv", PATIENT_VIEW_URL);
    stubStoredViewDefinition(PATIENT_VIEW_URL, PATIENT_VIEW_URL, "Patient");

    final ResolvedDependencyGraph graph =
        resolver.resolve(sqlQuery("SELECT * FROM v", "v", v1Url), SuppliedArtefacts.empty());

    assertThat(graph.getOrderedNodes()).hasSize(3);
    // Dependencies precede dependents: VD, then v2, then v1.
    assertThat(graph.getOrderedNodes().get(0).getCanonicalKey()).isEqualTo(PATIENT_VIEW_URL);
    assertThat(graph.getOrderedNodes().get(1).getCanonicalKey()).isEqualTo(v2Url);
    assertThat(graph.getOrderedNodes().get(2).getCanonicalKey()).isEqualTo(v1Url);
  }

  @Test
  void buildsTopologicallyOrderedTwoNodeGraph() {
    final String baseUrl = SqlLibraryFixtures.sqlViewUrl("base");
    stubSqlView(baseUrl, "SELECT * FROM pv", "pv", PATIENT_VIEW_URL);
    stubStoredViewDefinition(PATIENT_VIEW_URL, PATIENT_VIEW_URL, "Patient");

    final ResolvedDependencyGraph graph =
        resolver.resolve(sqlQuery("SELECT * FROM b", "b", baseUrl), SuppliedArtefacts.empty());

    assertThat(graph.getOrderedNodes()).hasSize(2);
    assertThat(graph.getOrderedNodes().get(0).getCanonicalKey()).isEqualTo(PATIENT_VIEW_URL);
    assertThat(graph.getOrderedNodes().get(1).getCanonicalKey()).isEqualTo(baseUrl);

    final ResolvedSqlView sqlView = (ResolvedSqlView) graph.getNodesByKey().get(baseUrl);
    assertThat(sqlView.getChildKeysByLabel()).containsEntry("pv", PATIENT_VIEW_URL);
  }

  @Test
  void deduplicatesABareUrlAndAVersionedReferenceToTheSameResource() {
    // Both the bare url and url|2 resolve to the same stored ViewDefinition (version 2), so they
    // normalise to the same canonical key and materialise once.
    final String versionedKey = PATIENT_VIEW_URL + "|2";
    stubStoredViewDefinition(PATIENT_VIEW_URL, versionedKey, "Patient");
    stubStoredViewDefinition(PATIENT_VIEW_URL + "|2", versionedKey, "Patient");

    final ResolvedDependencyGraph graph =
        resolver.resolve(
            sqlQueryWithDeps(
                "SELECT * FROM a JOIN b",
                Map.of("a", PATIENT_VIEW_URL, "b", PATIENT_VIEW_URL + "|2")),
            SuppliedArtefacts.empty());

    assertThat(graph.getOrderedNodes()).hasSize(1);
    assertThat(graph.getNodesByKey()).containsKey(versionedKey);
    assertThat(graph.getTopLevelKeysByLabel())
        .containsEntry("a", versionedKey)
        .containsEntry("b", versionedKey);
  }

  @Test
  void resolvesADiamondSharedNodeOnce() {
    final String leftUrl = SqlLibraryFixtures.sqlViewUrl("left");
    final String rightUrl = SqlLibraryFixtures.sqlViewUrl("right");
    final String sharedUrl = SqlLibraryFixtures.sqlViewUrl("shared");
    stubSqlView(leftUrl, "SELECT * FROM s", "s", sharedUrl);
    stubSqlView(rightUrl, "SELECT * FROM s", "s", sharedUrl);
    stubSqlView(sharedUrl, "SELECT * FROM pv", "pv", PATIENT_VIEW_URL);
    stubStoredViewDefinition(PATIENT_VIEW_URL, PATIENT_VIEW_URL, "Patient");

    final ResolvedDependencyGraph graph =
        resolver.resolve(
            sqlQueryWithDeps("SELECT * FROM l JOIN r", Map.of("l", leftUrl, "r", rightUrl)),
            SuppliedArtefacts.empty());

    final long sharedCount =
        graph.getOrderedNodes().stream()
            .filter(node -> sharedUrl.equals(node.getCanonicalKey()))
            .count();
    assertThat(sharedCount).isEqualTo(1);
    assertThat(graph.getOrderedNodes()).hasSize(4); // shared, vd, left, right.
  }

  @Test
  void resolvesTheSameLabelInDifferentNodesWithoutCollision() {
    final String v1Url = SqlLibraryFixtures.sqlViewUrl("v1");
    final String v2Url = SqlLibraryFixtures.sqlViewUrl("v2");
    final String aUrl = SqlLibraryFixtures.viewDefinitionUrl("a");
    final String bUrl = SqlLibraryFixtures.viewDefinitionUrl("b");
    stubSqlView(v1Url, "SELECT * FROM t", "t", aUrl);
    stubSqlView(v2Url, "SELECT * FROM t", "t", bUrl);
    stubStoredViewDefinition(aUrl, aUrl, "Patient");
    stubStoredViewDefinition(bUrl, bUrl, "Observation");

    final ResolvedDependencyGraph graph =
        resolver.resolve(
            sqlQueryWithDeps("SELECT * FROM one, two", Map.of("one", v1Url, "two", v2Url)),
            SuppliedArtefacts.empty());

    final ResolvedSqlView v1 = (ResolvedSqlView) graph.getNodesByKey().get(v1Url);
    final ResolvedSqlView v2 = (ResolvedSqlView) graph.getNodesByKey().get(v2Url);
    assertThat(v1.getChildKeysByLabel()).containsEntry("t", aUrl);
    assertThat(v2.getChildKeysByLabel()).containsEntry("t", bUrl);
  }

  // A supplied artefact outranks storage, and storage is not consulted at all for that reference.
  @Test
  void prefersARequestSuppliedViewOverStorage() {
    final FhirView supplied = fhirView("Patient");

    final ResolvedDependencyGraph graph =
        resolver.resolve(
            sqlQuery("SELECT * FROM p", "p", PATIENT_VIEW_URL),
            SuppliedArtefacts.ofViews(Map.of(PATIENT_VIEW_URL, supplied)));

    final ResolvedViewDefinition node =
        (ResolvedViewDefinition) graph.getNodesByKey().get(PATIENT_VIEW_URL);
    assertThat(node.getView()).isSameAs(supplied);
    verifyNoInteractions(viewResolver);
  }

  // A supplied SQLView is traversed like a stored one, so a dependency reachable only through it
  // is still resolved - and may itself be satisfied by another supplied entry.
  @Test
  void traversesThroughASuppliedSqlView() {
    final String viewUrl = SqlLibraryFixtures.sqlViewUrl("supplied-view");
    final FhirView leafView = fhirView("Patient");
    final Library suppliedSqlView =
        SqlLibraryFixtures.sqlViewWithUrl(viewUrl, "SELECT * FROM p", "p", PATIENT_VIEW_URL);

    final ResolvedDependencyGraph graph =
        resolver.resolve(
            sqlQuery("SELECT * FROM v", "v", viewUrl),
            SuppliedArtefacts.of(
                List.of(
                    SuppliedArtefact.ofSqlView(viewUrl, null, suppliedSqlView),
                    SuppliedArtefact.ofView(PATIENT_VIEW_URL, null, leafView))));

    assertThat(graph.getNodesByKey()).containsKeys(viewUrl, PATIENT_VIEW_URL);
    assertThat(graph.getNodesByKey().get(viewUrl)).isInstanceOf(ResolvedSqlView.class);
    verifyNoInteractions(viewResolver);
  }

  // A dependency pinned to a version is satisfied only by an entry declaring that version; an
  // entry at a different version does not match and storage is consulted instead.
  @Test
  void matchesAVersionPinnedDependencyOnlyWhenVersionsAgree() {
    final FhirView supplied = fhirView("Patient");
    when(viewResolver.resolveStoredViewDefinition(any())).thenReturn(Optional.empty());
    when(libraryReferenceResolver.tryResolveSqlViewLibrary(any())).thenReturn(Optional.empty());

    final SuppliedArtefacts wrongVersion =
        SuppliedArtefacts.of(List.of(SuppliedArtefact.ofView(PATIENT_VIEW_URL, "2.0", supplied)));

    assertThatThrownBy(
            () ->
                resolver.resolve(
                    sqlQuery("SELECT * FROM p", "p", PATIENT_VIEW_URL + "|1.0"), wrongVersion))
        .isInstanceOf(ResourceNotFoundException.class);

    final SuppliedArtefacts rightVersion =
        SuppliedArtefacts.of(List.of(SuppliedArtefact.ofView(PATIENT_VIEW_URL, "1.0", supplied)));
    final ResolvedDependencyGraph graph =
        resolver.resolve(sqlQuery("SELECT * FROM p", "p", PATIENT_VIEW_URL + "|1.0"), rightVersion);

    assertThat(graph.getNodesByKey()).containsKey(PATIENT_VIEW_URL + "|1.0");
  }

  // A dependency shared by two top-level queries is resolved once when the caller shares the
  // memoisation map, which is what gives an export job one resolution per canonical URL.
  @Test
  void sharesResolvedNodesAcrossQueriesWhenTheNodeMapIsShared() {
    final FhirView supplied = fhirView("Patient");
    final SuppliedArtefacts artefacts =
        SuppliedArtefacts.ofViews(Map.of(PATIENT_VIEW_URL, supplied));
    final Map<String, ResolvedDependency> shared = new java.util.LinkedHashMap<>();

    final ResolvedDependencyGraph first =
        resolver.resolve(sqlQuery("SELECT * FROM p", "p", PATIENT_VIEW_URL), artefacts, shared);
    final ResolvedDependencyGraph second =
        resolver.resolve(sqlQuery("SELECT * FROM q", "q", PATIENT_VIEW_URL), artefacts, shared);

    assertThat(shared).hasSize(1);
    assertThat(second.getNodesByKey().get(PATIENT_VIEW_URL))
        .isSameAs(first.getNodesByKey().get(PATIENT_VIEW_URL));
  }

  // ---------------------------------------------------------------------------
  // Configured external tables (spec 060 US1).
  // ---------------------------------------------------------------------------

  @Test
  void resolvesAConfiguredExternalTableByUrl() {
    configureExternalTable(TABLE_URL, TABLE_PATH, "parquet");

    final ResolvedDependencyGraph graph =
        resolver.resolve(sqlQuery("SELECT * FROM c", "c", TABLE_URL), SuppliedArtefacts.empty());

    assertThat(graph.getTopLevelKeysByLabel()).containsEntry("c", TABLE_URL);
    final ResolvedDependency node = graph.getNodesByKey().get(TABLE_URL);
    assertThat(node).isInstanceOf(ResolvedExternalTable.class);
    final ResolvedExternalTable table = (ResolvedExternalTable) node;
    assertThat(table.getCanonicalKey()).isEqualTo(TABLE_URL);
    assertThat(table.getPath()).isEqualTo(TABLE_PATH);
    assertThat(table.getFormat()).isEqualTo("parquet");
  }

  @Test
  void resolvesTheSameExternalTableUnderTwoLabelsOnce() {
    // The table is keyed by its bare URL, so two labels over it share one leaf and the SQL can join
    // the table to itself.
    configureExternalTable(TABLE_URL, TABLE_PATH, "delta");

    final ResolvedDependencyGraph graph =
        resolver.resolve(
            sqlQueryWithDeps(
                "SELECT * FROM a JOIN b ON a.k = b.k", Map.of("a", TABLE_URL, "b", TABLE_URL)),
            SuppliedArtefacts.empty());

    assertThat(graph.getOrderedNodes()).hasSize(1);
    assertThat(graph.getTopLevelKeysByLabel())
        .containsEntry("a", TABLE_URL)
        .containsEntry("b", TABLE_URL);
  }

  @Test
  void rejectsAnExternalTableBeyondTheDepthLimit() {
    // A table leaf counts towards the depth like any other leaf: a SQLView at depth 1 referencing
    // the table places it at depth 2, over a limit of 1.
    serverConfiguration.getSqlQuery().setMaxDependencyDepth(1);
    configureExternalTable(TABLE_URL, TABLE_PATH, "delta");
    final String viewUrl = SqlLibraryFixtures.sqlViewUrl("over-table");
    stubSqlView(viewUrl, "SELECT * FROM c", "c", TABLE_URL);

    assertThatThrownBy(
            () ->
                resolver.resolve(
                    sqlQuery("SELECT * FROM v", "v", viewUrl), SuppliedArtefacts.empty()))
        .isInstanceOf(InvalidRequestException.class)
        .hasMessageContainingAll("deeper", "1", TABLE_URL);
  }

  // ---------------------------------------------------------------------------
  // External tables beneath SQLViews (spec 060 US2).
  // ---------------------------------------------------------------------------

  @Test
  void resolvesAConfiguredExternalTableBeneathASuppliedSqlView() {
    // The table is a leaf of the SQLView, so it is ordered before the view and the view's child
    // map binds the label to the table's bare URL.
    configureExternalTable(TABLE_URL, TABLE_PATH, "delta");
    final String viewUrl = SqlLibraryFixtures.sqlViewUrl("cohort-view");
    final Library suppliedSqlView =
        SqlLibraryFixtures.sqlViewWithUrl(
            viewUrl, "SELECT family_name, cohort FROM cohort", "cohort", TABLE_URL);

    final ResolvedDependencyGraph graph =
        resolver.resolve(
            sqlQuery("SELECT * FROM cv", "cv", viewUrl),
            SuppliedArtefacts.of(
                List.of(SuppliedArtefact.ofSqlView(viewUrl, null, suppliedSqlView))));

    assertThat(graph.getOrderedNodes()).hasSize(2);
    assertThat(graph.getOrderedNodes().get(0)).isInstanceOf(ResolvedExternalTable.class);
    assertThat(graph.getOrderedNodes().get(0).getCanonicalKey()).isEqualTo(TABLE_URL);
    assertThat(graph.getOrderedNodes().get(1).getCanonicalKey()).isEqualTo(viewUrl);
    final ResolvedSqlView sqlView = (ResolvedSqlView) graph.getNodesByKey().get(viewUrl);
    assertThat(sqlView.getChildKeysByLabel()).containsExactly(Map.entry("cohort", TABLE_URL));
  }

  @Test
  void resolvesADiamondOverAnExternalTableToASingleTableNode() {
    // Two SQLViews reach the same table under different labels; the table is keyed by its URL and
    // so is resolved once and shared by both arms.
    configureExternalTable(TABLE_URL, TABLE_PATH, "parquet");
    final String leftUrl = SqlLibraryFixtures.sqlViewUrl("left");
    final String rightUrl = SqlLibraryFixtures.sqlViewUrl("right");
    stubSqlView(leftUrl, "SELECT * FROM c", "c", TABLE_URL);
    stubSqlView(rightUrl, "SELECT * FROM t", "t", TABLE_URL);

    final ResolvedDependencyGraph graph =
        resolver.resolve(
            sqlQueryWithDeps("SELECT * FROM l JOIN r", Map.of("l", leftUrl, "r", rightUrl)),
            SuppliedArtefacts.empty());

    assertThat(graph.getOrderedNodes()).hasSize(3);
    assertThat(graph.getOrderedNodes().get(0)).isInstanceOf(ResolvedExternalTable.class);
    assertThat(graph.getOrderedNodes().get(0).getCanonicalKey()).isEqualTo(TABLE_URL);
    final ResolvedSqlView left = (ResolvedSqlView) graph.getNodesByKey().get(leftUrl);
    final ResolvedSqlView right = (ResolvedSqlView) graph.getNodesByKey().get(rightUrl);
    assertThat(left.getChildKeysByLabel()).containsEntry("c", TABLE_URL);
    assertThat(right.getChildKeysByLabel()).containsEntry("t", TABLE_URL);
    assertThat(graph.getNodesByKey().get(TABLE_URL))
        .isSameAs(graph.getOrderedNodes().get(0))
        .isInstanceOf(ResolvedExternalTable.class);
  }

  // ---------------------------------------------------------------------------
  // External table faults (spec 060 US3): version pins, collisions and context precedence.
  // ---------------------------------------------------------------------------

  @Test
  void rejectsAVersionPinnedReferenceToAnExternalTableAsNotFound() {
    // Tables carry no version, so a pinned reference can never mean one; with nothing stored under
    // that URL either, the reference is not found.
    configureExternalTable(TABLE_URL, TABLE_PATH, "delta");

    assertThatThrownBy(
            () ->
                resolver.resolve(
                    sqlQuery("SELECT * FROM c", "c", TABLE_URL + "|2"), SuppliedArtefacts.empty()))
        .isInstanceOf(ResourceNotFoundException.class)
        .hasMessageContainingAll("'c'", TABLE_URL + "|2")
        .hasMessageEndingWith(
            "no ViewDefinition, SQLView, external table, concept map or value set matches that"
                + " canonical URL");
  }

  @Test
  void rejectsAUrlMatchingAnExternalTableAndAStoredViewDefinitionAsAmbiguous() {
    configureExternalTable(TABLE_URL, TABLE_PATH, "delta");
    stubStoredViewDefinition(TABLE_URL, TABLE_URL, "Patient");

    assertThatThrownBy(
            () ->
                resolver.resolve(
                    sqlQuery("SELECT * FROM c", "c", TABLE_URL), SuppliedArtefacts.empty()))
        .isInstanceOf(InvalidRequestException.class)
        .hasMessageContainingAll(
            "'c'", TABLE_URL, "is ambiguous", "external table", "ViewDefinition")
        .hasMessageNotContaining("SQLView");
  }

  @Test
  void rejectsAUrlMatchingAnExternalTableAndAStoredSqlViewAsAmbiguous() {
    configureExternalTable(TABLE_URL, TABLE_PATH, "delta");
    stubSqlView(TABLE_URL, "SELECT * FROM pv", "pv", PATIENT_VIEW_URL);

    assertThatThrownBy(
            () ->
                resolver.resolve(
                    sqlQuery("SELECT * FROM c", "c", TABLE_URL), SuppliedArtefacts.empty()))
        .isInstanceOf(InvalidRequestException.class)
        .hasMessageContainingAll("'c'", TABLE_URL, "is ambiguous", "external table", "SQLView")
        .hasMessageNotContaining("ViewDefinition");
  }

  @Test
  void prefersASuppliedViewDefinitionOverAnExternalTableWithTheSameUrl() {
    // A context artefact outranks both configuration and storage, and neither is consulted.
    configureExternalTable(TABLE_URL, TABLE_PATH, "delta");
    final FhirView supplied = fhirView("Patient");

    final ResolvedDependencyGraph graph =
        resolver.resolve(
            sqlQuery("SELECT * FROM c", "c", TABLE_URL),
            SuppliedArtefacts.ofViews(Map.of(TABLE_URL, supplied)));

    assertThat(graph.getOrderedNodes()).hasSize(1);
    final ResolvedDependency node = graph.getNodesByKey().get(TABLE_URL);
    assertThat(node).isInstanceOf(ResolvedViewDefinition.class);
    assertThat(((ResolvedViewDefinition) node).getView()).isSameAs(supplied);
    verifyNoInteractions(viewResolver, libraryReferenceResolver);
  }

  // ---------------------------------------------------------------------------
  // Cycles and depth (keyed by canonical identity).
  // ---------------------------------------------------------------------------

  @Test
  void rejectsACycleNamingTheChain() {
    final String aUrl = SqlLibraryFixtures.sqlViewUrl("a");
    final String bUrl = SqlLibraryFixtures.sqlViewUrl("b");
    stubSqlView(aUrl, "SELECT * FROM b", "b", bUrl);
    stubSqlView(bUrl, "SELECT * FROM a", "a", aUrl);

    assertThatThrownBy(
            () ->
                resolver.resolve(sqlQuery("SELECT * FROM x", "x", aUrl), SuppliedArtefacts.empty()))
        .isInstanceOf(InvalidRequestException.class)
        .hasMessageContainingAll("Cyclic", aUrl, bUrl);
  }

  @Test
  void rejectsASelfReference() {
    final String selfUrl = SqlLibraryFixtures.sqlViewUrl("self");
    stubSqlView(selfUrl, "SELECT * FROM s", "s", selfUrl);

    assertThatThrownBy(
            () ->
                resolver.resolve(
                    sqlQuery("SELECT * FROM x", "x", selfUrl), SuppliedArtefacts.empty()))
        .isInstanceOf(InvalidRequestException.class)
        .hasMessageContaining("Cyclic");
  }

  @Test
  void rejectsAGraphDeeperThanTheConfiguredLimit() {
    serverConfiguration.getSqlQuery().setMaxDependencyDepth(2);
    final String v1Url = SqlLibraryFixtures.sqlViewUrl("v1");
    final String v2Url = SqlLibraryFixtures.sqlViewUrl("v2");
    final String v3Url = SqlLibraryFixtures.sqlViewUrl("v3");
    stubSqlView(v1Url, "SELECT * FROM x", "x", v2Url);
    stubSqlView(v2Url, "SELECT * FROM y", "y", v3Url);
    stubSqlView(v3Url, "SELECT * FROM pv", "pv", PATIENT_VIEW_URL);
    stubStoredViewDefinition(PATIENT_VIEW_URL, PATIENT_VIEW_URL, "Patient");

    assertThatThrownBy(
            () ->
                resolver.resolve(
                    sqlQuery("SELECT * FROM v", "v", v1Url), SuppliedArtefacts.empty()))
        .isInstanceOf(InvalidRequestException.class)
        .hasMessageContainingAll("deeper", "2");
  }

  // ---------------------------------------------------------------------------
  // Error surface (US2): not-found, ambiguity, wrong-typed dependency.
  // ---------------------------------------------------------------------------

  @Test
  void reportsNotFoundWhenNothingMatches() {
    final String missingUrl = SqlLibraryFixtures.viewDefinitionUrl("missing");

    // The outcome names the subject as the parameter at fault, as every other SQL operation 4xx
    // does, rather than leaving the client with a bare message.
    assertThatThrownBy(
            () ->
                resolver.resolve(sqlQuery("SELECT 1", "x", missingUrl), SuppliedArtefacts.empty()))
        .isInstanceOf(ResourceNotFoundException.class)
        .hasMessageContainingAll("'x'", missingUrl)
        .hasMessage(
            "Failed to resolve the dependency for label 'x' with reference '"
                + missingUrl
                + "': no ViewDefinition, SQLView, external table, concept map or value set matches"
                + " that canonical URL")
        .satisfies(
            e -> {
              final OperationOutcome outcome =
                  (OperationOutcome) ((ResourceNotFoundException) e).getOperationOutcome();
              assertThat(outcome).isNotNull();
              assertThat(outcome.getIssue()).hasSize(1);
              final OperationOutcomeIssueComponent issue = outcome.getIssueFirstRep();
              assertThat(issue.getCode()).isEqualTo(IssueType.NOTFOUND);
              assertThat(issue.getExpression())
                  .extracting(StringType::getValue)
                  .containsExactly(SubjectResolver.SUBJECT_EXPRESSION);
              assertThat(issue.getDiagnostics()).contains(missingUrl);
            });
    verify(valueSetResolver)
        .resolveCanonical(
            argThat(ref -> ref != null && missingUrl.equals(ref.getCanonicalUrl())), any());
    verify(conceptMapResolver)
        .resolveCanonical(
            argThat(ref -> ref != null && missingUrl.equals(ref.getCanonicalUrl())), any());
  }

  @Test
  void rejectsAnAmbiguousReferenceMatchingBothTypes() {
    final String clashUrl = SqlLibraryFixtures.viewDefinitionUrl("clash");
    stubStoredViewDefinition(clashUrl, clashUrl, "Patient");
    stubSqlView(clashUrl, "SELECT 1", "p", PATIENT_VIEW_URL);

    assertThatThrownBy(
            () -> resolver.resolve(sqlQuery("SELECT 1", "c", clashUrl), SuppliedArtefacts.empty()))
        .isInstanceOf(InvalidRequestException.class)
        .hasMessageContainingAll("is ambiguous", "'c'", clashUrl, "ViewDefinition", "SQLView")
        .hasMessageNotContaining("external table");
  }

  @Test
  void rejectsASqlQueryReferencedAsADependency() {
    // A Library that is itself a sql-query (not a sql-view) cannot be a dependency.
    final String queryUrl = SqlLibraryFixtures.sqlViewUrl("q");
    final Library nested = SqlLibraryFixtures.sqlQuery("SELECT 1");
    nested.setUrl(queryUrl);
    when(libraryReferenceResolver.tryResolveSqlViewLibrary(queryUrl))
        .thenReturn(Optional.of(nested));

    assertThatThrownBy(
            () -> resolver.resolve(sqlQuery("SELECT 1", "q", queryUrl), SuppliedArtefacts.empty()))
        .isInstanceOf(InvalidRequestException.class)
        .hasMessageContaining("sql-query")
        .hasMessageContaining("SQLView");
  }

  // ---------------------------------------------------------------------------
  // Value sets through the terminology layer (spec 061 US1).
  // ---------------------------------------------------------------------------

  @Test
  void resolvesAnUnmatchedUrlAsAValueSetLeaf() {
    // Nothing supplied, configured or stored matches, so the reference is passed to the membership
    // resolver, which returns a leaf that is registered under its key.
    final ResolvedValueSet valueSet = stubValueSet(VALUE_SET_URL + "|2026");

    final ResolvedDependencyGraph graph =
        resolver.resolve(
            sqlQuery("SELECT * FROM cvd", "cvd", VALUE_SET_URL + "|2026"),
            SuppliedArtefacts.empty());

    assertThat(graph.getOrderedNodes()).containsExactly(valueSet);
    assertThat(graph.getTopLevelKeysByLabel()).containsEntry("cvd", VALUE_SET_URL + "|2026");
    verify(valueSetResolver)
        .resolveCanonical(
            argThat(ref -> ref != null && "cvd".equals(ref.getLabel())),
            argThat(
                canonical ->
                    canonical != null
                        && VALUE_SET_URL.equals(canonical.getUrl())
                        && "2026".equals(canonical.getVersion())));
    verify(conceptMapResolver, never()).resolveCanonical(any(), any());
  }

  @Test
  void neverConsultsTheTerminologyLayerForAStoredViewDefinition() {
    stubStoredViewDefinition(PATIENT_VIEW_URL, PATIENT_VIEW_URL, "Patient");

    resolver.resolve(sqlQuery("SELECT * FROM p", "p", PATIENT_VIEW_URL), SuppliedArtefacts.empty());

    verifyNoInteractions(valueSetResolver, conceptMapResolver);
  }

  @Test
  void neverConsultsTheTerminologyLayerForAStoredSqlViewOrAnExternalTable() {
    final String baseUrl = SqlLibraryFixtures.sqlViewUrl("base");
    stubSqlView(baseUrl, "SELECT * FROM pv", "pv", PATIENT_VIEW_URL);
    stubStoredViewDefinition(PATIENT_VIEW_URL, PATIENT_VIEW_URL, "Patient");
    configureExternalTable(TABLE_URL, TABLE_PATH, "delta");

    resolver.resolve(
        sqlQueryWithDeps("SELECT * FROM b JOIN c", Map.of("b", baseUrl, "c", TABLE_URL)),
        SuppliedArtefacts.empty());

    verifyNoInteractions(valueSetResolver, conceptMapResolver);
  }

  @Test
  void reusesAValueSetNodeReachedTwiceUnderTheSameReferenceWithoutASecondExpansion() {
    // A diamond over a value set: two SQLViews reach it under the same reference string, and it is
    // resolved and expanded exactly once.
    stubValueSet(VALUE_SET_URL);
    final String leftUrl = SqlLibraryFixtures.sqlViewUrl("left");
    final String rightUrl = SqlLibraryFixtures.sqlViewUrl("right");
    stubSqlView(leftUrl, "SELECT * FROM v", "v", VALUE_SET_URL);
    stubSqlView(rightUrl, "SELECT * FROM w", "w", VALUE_SET_URL);

    final ResolvedDependencyGraph graph =
        resolver.resolve(
            sqlQueryWithDeps("SELECT * FROM l JOIN r", Map.of("l", leftUrl, "r", rightUrl)),
            SuppliedArtefacts.empty());

    assertThat(graph.getOrderedNodes()).hasSize(3);
    assertThat(graph.getNodesByKey().get(VALUE_SET_URL)).isInstanceOf(ResolvedValueSet.class);
    verify(valueSetResolver, times(1)).resolveCanonical(any(), any());
  }

  @Test
  void resolvesTheSameValueSetUnderTwoLabelsOnce() {
    stubValueSet(VALUE_SET_URL);

    final ResolvedDependencyGraph graph =
        resolver.resolve(
            sqlQueryWithDeps(
                "SELECT * FROM a JOIN b ON a.code = b.code",
                Map.of("a", VALUE_SET_URL, "b", VALUE_SET_URL)),
            SuppliedArtefacts.empty());

    assertThat(graph.getOrderedNodes()).hasSize(1);
    assertThat(graph.getTopLevelKeysByLabel())
        .containsEntry("a", VALUE_SET_URL)
        .containsEntry("b", VALUE_SET_URL);
    verify(valueSetResolver, times(1)).resolveCanonical(any(), any());
  }

  @Test
  void treatsAPinnedAndAnUnpinnedReferenceToOneValueSetAsTwoNodes() {
    // The matching algorithm memoises by the canonical as written, so the two strings are two
    // relations even though they may name one membership.
    stubValueSet(VALUE_SET_URL);
    stubValueSet(VALUE_SET_URL + "|2026");

    final ResolvedDependencyGraph graph =
        resolver.resolve(
            sqlQueryWithDeps(
                "SELECT * FROM a JOIN b", Map.of("a", VALUE_SET_URL, "b", VALUE_SET_URL + "|2026")),
            SuppliedArtefacts.empty());

    assertThat(graph.getOrderedNodes()).hasSize(2);
    assertThat(graph.getNodesByKey()).containsKeys(VALUE_SET_URL, VALUE_SET_URL + "|2026");
    verify(valueSetResolver, times(2)).resolveCanonical(any(), any());
  }

  @Test
  void rejectsAValueSetBeyondTheDepthLimit() {
    serverConfiguration.getSqlQuery().setMaxDependencyDepth(1);
    stubValueSet(VALUE_SET_URL);
    final String viewUrl = SqlLibraryFixtures.sqlViewUrl("over-value-set");
    stubSqlView(viewUrl, "SELECT * FROM v", "v", VALUE_SET_URL);

    assertThatThrownBy(
            () ->
                resolver.resolve(
                    sqlQuery("SELECT * FROM x", "x", viewUrl), SuppliedArtefacts.empty()))
        .isInstanceOf(InvalidRequestException.class)
        .hasMessageContainingAll("deeper", "1", VALUE_SET_URL);
    verify(valueSetResolver, never()).resolveCanonical(any(), any());
  }

  // ---------------------------------------------------------------------------
  // Concept maps through the terminology layer (spec 062 US1).
  // ---------------------------------------------------------------------------

  @Test
  void resolvesAnUnmatchedUrlThatIsNoValueSetAsAConceptMapLeaf() {
    // The value set lookup runs first and finds nothing, so the reference falls through to the
    // concept map lookup, whose leaf is registered under the reference as written.
    final ResolvedConceptMap conceptMap = stubConceptMap(CONCEPT_MAP_URL + "|2026");

    final ResolvedDependencyGraph graph =
        resolver.resolve(
            sqlQuery("SELECT * FROM m", "m", CONCEPT_MAP_URL + "|2026"), SuppliedArtefacts.empty());

    assertThat(graph.getOrderedNodes()).containsExactly(conceptMap);
    assertThat(graph.getTopLevelKeysByLabel()).containsEntry("m", CONCEPT_MAP_URL + "|2026");
    final InOrder order = inOrder(valueSetResolver, conceptMapResolver);
    order
        .verify(valueSetResolver)
        .resolveCanonical(
            argThat(ref -> ref != null && "m".equals(ref.getLabel())),
            argThat(
                canonical ->
                    canonical != null
                        && CONCEPT_MAP_URL.equals(canonical.getUrl())
                        && "2026".equals(canonical.getVersion())));
    order
        .verify(conceptMapResolver)
        .resolveCanonical(
            argThat(ref -> ref != null && "m".equals(ref.getLabel())),
            argThat(
                canonical ->
                    canonical != null
                        && CONCEPT_MAP_URL.equals(canonical.getUrl())
                        && "2026".equals(canonical.getVersion())));
  }

  @Test
  void neverLooksUpAnImplicitSnomedValueSetUrlAsAConceptMap() {
    final String url = "http://snomed.info/sct?fhir_vs=isa/73211009";

    assertThatThrownBy(
            () -> resolver.resolve(sqlQuery("SELECT 1", "t2", url), SuppliedArtefacts.empty()))
        .isInstanceOf(ResourceNotFoundException.class)
        .hasMessageContainingAll("'t2'", url);
    verify(valueSetResolver).resolveCanonical(any(), any());
    verify(conceptMapResolver, never()).resolveCanonical(any(), any());
  }

  @Test
  void neverLooksUpAVclValueSetUrlAsAConceptMap() {
    final String url =
        "http://fhir.org/VCL?v1="
            + URLEncoder.encode("(http://snomed.info/sct)inactive = true", StandardCharsets.UTF_8);

    assertThatThrownBy(
            () -> resolver.resolve(sqlQuery("SELECT 1", "vcl", url), SuppliedArtefacts.empty()))
        .isInstanceOf(ResourceNotFoundException.class)
        .hasMessageContainingAll("'vcl'", url);
    verify(valueSetResolver).resolveCanonical(any(), any());
    verify(conceptMapResolver, never()).resolveCanonical(any(), any());
  }

  @Test
  void resolvesASnomedImplicitConceptMapUrlWithoutAValueSetLookup() {
    // A fhir_cm URL names a concept map by its grammar, so it goes straight to the concept map
    // lookup.
    final String url = "http://snomed.info/sct?fhir_cm=900000000000526001";
    final ResolvedConceptMap conceptMap = stubConceptMap(url);

    final ResolvedDependencyGraph graph =
        resolver.resolve(sqlQuery("SELECT * FROM r", "r", url), SuppliedArtefacts.empty());

    assertThat(graph.getOrderedNodes()).containsExactly(conceptMap);
    verify(valueSetResolver, never()).resolveCanonical(any(), any());
  }

  @Test
  void propagatesTheNotFoundTheConceptMapResolverRaisesForASnomedImplicitConceptMapUrl() {
    final String url = "http://snomed.info/sct?fhir_cm=900000000000497000";
    final ResourceNotFoundException notFound =
        new ResourceNotFoundException("raised by the concept map resolver");
    when(conceptMapResolver.resolveCanonical(any(), any())).thenThrow(notFound);

    assertThatThrownBy(
            () -> resolver.resolve(sqlQuery("SELECT 1", "moved", url), SuppliedArtefacts.empty()))
        .isSameAs(notFound);
    verify(valueSetResolver, never()).resolveCanonical(any(), any());
    verify(conceptMapResolver)
        .resolveCanonical(
            argThat(ref -> ref != null && "moved".equals(ref.getLabel())),
            argThat(canonical -> canonical != null && url.equals(canonical.getUrl())));
  }

  @Test
  void reusesAConceptMapNodeReachedTwiceUnderTheSameReferenceWithoutASecondLookup() {
    // A diamond over a concept map: two SQLViews reach it under the same reference string, and
    // neither terminology lookup runs a second time.
    stubConceptMap(CONCEPT_MAP_URL);
    final String leftUrl = SqlLibraryFixtures.sqlViewUrl("left");
    final String rightUrl = SqlLibraryFixtures.sqlViewUrl("right");
    stubSqlView(leftUrl, "SELECT * FROM v", "v", CONCEPT_MAP_URL);
    stubSqlView(rightUrl, "SELECT * FROM w", "w", CONCEPT_MAP_URL);

    final ResolvedDependencyGraph graph =
        resolver.resolve(
            sqlQueryWithDeps("SELECT * FROM l JOIN r", Map.of("l", leftUrl, "r", rightUrl)),
            SuppliedArtefacts.empty());

    assertThat(graph.getOrderedNodes()).hasSize(3);
    assertThat(graph.getNodesByKey().get(CONCEPT_MAP_URL)).isInstanceOf(ResolvedConceptMap.class);
    verify(valueSetResolver, times(1)).resolveCanonical(any(), any());
    verify(conceptMapResolver, times(1)).resolveCanonical(any(), any());
  }

  @Test
  void resolvesTheSameConceptMapUnderTwoLabelsOnce() {
    stubConceptMap(CONCEPT_MAP_URL);

    final ResolvedDependencyGraph graph =
        resolver.resolve(
            sqlQueryWithDeps(
                "SELECT * FROM a JOIN b ON a.source_code = b.target_code",
                Map.of("a", CONCEPT_MAP_URL, "b", CONCEPT_MAP_URL)),
            SuppliedArtefacts.empty());

    assertThat(graph.getOrderedNodes()).hasSize(1);
    assertThat(graph.getTopLevelKeysByLabel())
        .containsEntry("a", CONCEPT_MAP_URL)
        .containsEntry("b", CONCEPT_MAP_URL);
    verify(valueSetResolver, times(1)).resolveCanonical(any(), any());
    verify(conceptMapResolver, times(1)).resolveCanonical(any(), any());
  }

  @Test
  void rejectsAConceptMapBeyondTheDepthLimit() {
    serverConfiguration.getSqlQuery().setMaxDependencyDepth(1);
    stubConceptMap(CONCEPT_MAP_URL);
    final String viewUrl = SqlLibraryFixtures.sqlViewUrl("over-concept-map");
    stubSqlView(viewUrl, "SELECT * FROM m", "m", CONCEPT_MAP_URL);

    assertThatThrownBy(
            () ->
                resolver.resolve(
                    sqlQuery("SELECT * FROM x", "x", viewUrl), SuppliedArtefacts.empty()))
        .isInstanceOf(InvalidRequestException.class)
        .hasMessageContainingAll("deeper", "1", CONCEPT_MAP_URL);
    verify(valueSetResolver, never()).resolveCanonical(any(), any());
    verify(conceptMapResolver, never()).resolveCanonical(any(), any());
  }

  // ---------------------------------------------------------------------------
  // Outcomes of the terminology lookups (spec 062 US5): found, absent and unknown.
  // ---------------------------------------------------------------------------

  @Test
  void discardsAnIndeterminateValueSetLookupWhenAConceptMapIsFound() {
    stubIndeterminateValueSet(VALUE_SET_ISSUE);
    final ResolvedConceptMap conceptMap = stubConceptMap(CONCEPT_MAP_URL);

    final ResolvedDependencyGraph graph =
        resolver.resolve(
            sqlQuery("SELECT * FROM m", "m", CONCEPT_MAP_URL), SuppliedArtefacts.empty());

    assertThat(graph.getOrderedNodes()).containsExactly(conceptMap);
  }

  @Test
  void reportsAnIndeterminateValueSetLookupWhenNoConceptMapIsFound() {
    stubIndeterminateValueSet(VALUE_SET_ISSUE);

    assertThatThrownBy(
            () ->
                resolver.resolve(
                    sqlQuery("SELECT * FROM m", "m", CONCEPT_MAP_URL), SuppliedArtefacts.empty()))
        .isInstanceOf(UnprocessableEntityException.class)
        .satisfies(thrown -> assertKeptIssues(thrown, VALUE_SET_ISSUE));
  }

  @Test
  void reportsBothIndeterminateLookupsInOne422ValueSetFirst() {
    stubIndeterminateValueSet(VALUE_SET_ISSUE);
    stubIndeterminateConceptMap(CONCEPT_MAP_ISSUE);

    assertThatThrownBy(
            () ->
                resolver.resolve(
                    sqlQuery("SELECT * FROM m", "m", CONCEPT_MAP_URL), SuppliedArtefacts.empty()))
        .isInstanceOf(UnprocessableEntityException.class)
        .satisfies(thrown -> assertKeptIssues(thrown, VALUE_SET_ISSUE, CONCEPT_MAP_ISSUE));
  }

  @Test
  void reportsAnIndeterminateConceptMapLookupAfterAnAbsentValueSet() {
    stubIndeterminateConceptMap(CONCEPT_MAP_ISSUE);

    assertThatThrownBy(
            () ->
                resolver.resolve(
                    sqlQuery("SELECT * FROM m", "m", CONCEPT_MAP_URL), SuppliedArtefacts.empty()))
        .isInstanceOf(UnprocessableEntityException.class)
        .satisfies(thrown -> assertKeptIssues(thrown, CONCEPT_MAP_ISSUE));
  }

  @Test
  void reportsAFaultInAFoundConceptMapAloneDiscardingTheKeptValueSetIssue() {
    stubIndeterminateValueSet(VALUE_SET_ISSUE);
    final UnprocessableEntityException fault =
        new UnprocessableEntityException("raised by the concept map resolver");
    when(conceptMapResolver.resolveCanonical(any(), any())).thenThrow(fault);

    assertThatThrownBy(
            () ->
                resolver.resolve(
                    sqlQuery("SELECT * FROM m", "m", CONCEPT_MAP_URL), SuppliedArtefacts.empty()))
        .isSameAs(fault);
  }

  @Test
  void reportsAnUndeterminableConceptMapVersionAloneDiscardingTheKeptValueSetIssue() {
    stubIndeterminateValueSet(VALUE_SET_ISSUE);
    final ResourceNotFoundException fault =
        new ResourceNotFoundException("raised by the concept map resolver");
    when(conceptMapResolver.resolveCanonical(any(), any())).thenThrow(fault);

    assertThatThrownBy(
            () ->
                resolver.resolve(
                    sqlQuery("SELECT * FROM m", "m", CONCEPT_MAP_URL), SuppliedArtefacts.empty()))
        .isSameAs(fault);
  }

  @Test
  void propagatesAPlain422FromTheValueSetLookupWithoutAConceptMapLookup() {
    // An unreachable server, or a fault in a value set that was found, ends resolution at once.
    final UnprocessableEntityException fault =
        new UnprocessableEntityException("raised by the membership resolver");
    when(valueSetResolver.resolveCanonical(any(), any())).thenThrow(fault);

    assertThatThrownBy(
            () ->
                resolver.resolve(
                    sqlQuery("SELECT * FROM m", "m", CONCEPT_MAP_URL), SuppliedArtefacts.empty()))
        .isSameAs(fault);
    verify(conceptMapResolver, never()).resolveCanonical(any(), any());
  }

  @Test
  void reportsAnIndeterminateImplicitValueSetLookupWithoutAConceptMapLookup() {
    final String url = "http://snomed.info/sct?fhir_vs=isa/73211009";
    stubIndeterminateValueSet(VALUE_SET_ISSUE);

    assertThatThrownBy(
            () -> resolver.resolve(sqlQuery("SELECT 1", "t2", url), SuppliedArtefacts.empty()))
        .isInstanceOf(UnprocessableEntityException.class)
        .satisfies(thrown -> assertKeptIssues(thrown, VALUE_SET_ISSUE));
    verify(conceptMapResolver, never()).resolveCanonical(any(), any());
  }

  // ---------------------------------------------------------------------------
  // Supplied ValueSets (spec 061 US2).
  // ---------------------------------------------------------------------------

  @Test
  void resolvesASuppliedValueSetAsALeafThroughTheMembershipResolver() {
    final SuppliedArtefact supplied = suppliedValueSet("2026");
    final ResolvedValueSet leaf = stubSuppliedValueSet(supplied, VALUE_SET_URL + "|2026");

    final ResolvedDependencyGraph graph =
        resolver.resolve(
            sqlQuery("SELECT * FROM cvd", "cvd", VALUE_SET_URL + "|2026"),
            SuppliedArtefacts.of(List.of(supplied)));

    assertThat(graph.getOrderedNodes()).containsExactly(leaf);
    assertThat(graph.getTopLevelKeysByLabel()).containsEntry("cvd", VALUE_SET_URL + "|2026");
    verify(valueSetResolver)
        .resolveSupplied(argThat(ref -> ref != null && "cvd".equals(ref.getLabel())), eq(supplied));
    verify(valueSetResolver, never()).resolveCanonical(any(), any());
  }

  @Test
  void prefersASuppliedValueSetOverAStoredViewDefinitionWithTheSameUrl() {
    // A context artefact outranks storage, and neither stored lookup is consulted for that URL.
    stubStoredViewDefinition(VALUE_SET_URL, VALUE_SET_URL, "Condition");
    final SuppliedArtefact supplied = suppliedValueSet(null);
    stubSuppliedValueSet(supplied, VALUE_SET_URL);

    final ResolvedDependencyGraph graph =
        resolver.resolve(
            sqlQuery("SELECT * FROM cvd", "cvd", VALUE_SET_URL),
            SuppliedArtefacts.of(List.of(supplied)));

    assertThat(graph.getOrderedNodes()).hasSize(1);
    assertThat(graph.getNodesByKey().get(VALUE_SET_URL)).isInstanceOf(ResolvedValueSet.class);
    verifyNoInteractions(viewResolver, libraryReferenceResolver);
  }

  @Test
  void matchesAPinnedDependencyToASuppliedValueSetOnlyWhenTheVersionsAgree() {
    final SuppliedArtefact wrongVersion = suppliedValueSet("2025");
    stubSuppliedValueSet(wrongVersion, VALUE_SET_URL + "|2025");
    final ResolvedValueSet canonical = stubValueSet(VALUE_SET_URL + "|2026");

    final ResolvedDependencyGraph fellThrough =
        resolver.resolve(
            sqlQuery("SELECT * FROM cvd", "cvd", VALUE_SET_URL + "|2026"),
            SuppliedArtefacts.of(List.of(wrongVersion)));

    assertThat(fellThrough.getOrderedNodes()).containsExactly(canonical);
    verify(valueSetResolver, never()).resolveSupplied(any(), any());
    verify(valueSetResolver).resolveCanonical(any(), any());

    final SuppliedArtefact rightVersion = suppliedValueSet("2026");
    final ResolvedValueSet suppliedLeaf =
        stubSuppliedValueSet(rightVersion, VALUE_SET_URL + "|2026");

    final ResolvedDependencyGraph matched =
        resolver.resolve(
            sqlQuery("SELECT * FROM cvd", "cvd", VALUE_SET_URL + "|2026"),
            SuppliedArtefacts.of(List.of(rightVersion)));

    assertThat(matched.getOrderedNodes()).containsExactly(suppliedLeaf);
    verify(valueSetResolver).resolveSupplied(any(), eq(rightVersion));
  }

  @Test
  void resolvesASuppliedValueSetReachedUnderTwoLabelsOnce() {
    // A compose-only supplied ValueSet is expanded by the terminology layer, so the node is reused
    // rather than the artefact being resolved again for the second label.
    final SuppliedArtefact supplied = suppliedValueSet(null);
    stubSuppliedValueSet(supplied, VALUE_SET_URL);

    final ResolvedDependencyGraph graph =
        resolver.resolve(
            sqlQueryWithDeps(
                "SELECT * FROM a JOIN b ON a.code = b.code",
                Map.of("a", VALUE_SET_URL, "b", VALUE_SET_URL)),
            SuppliedArtefacts.of(List.of(supplied)));

    assertThat(graph.getOrderedNodes()).hasSize(1);
    assertThat(graph.getTopLevelKeysByLabel())
        .containsEntry("a", VALUE_SET_URL)
        .containsEntry("b", VALUE_SET_URL);
    verify(valueSetResolver, times(1)).resolveSupplied(any(), any());
  }

  // ---------------------------------------------------------------------------
  // Supplied ConceptMaps (spec 062 US2).
  // ---------------------------------------------------------------------------

  @Test
  void resolvesASuppliedConceptMapAsALeafThroughTheConceptMapResolver() {
    final SuppliedArtefact supplied = suppliedConceptMap("2026");
    final ResolvedConceptMap leaf = stubSuppliedConceptMap(supplied, CONCEPT_MAP_URL + "|2026");

    final ResolvedDependencyGraph graph =
        resolver.resolve(
            sqlQuery("SELECT * FROM cm", "cm", CONCEPT_MAP_URL + "|2026"),
            SuppliedArtefacts.of(List.of(supplied)));

    assertThat(graph.getOrderedNodes()).containsExactly(leaf);
    assertThat(graph.getTopLevelKeysByLabel()).containsEntry("cm", CONCEPT_MAP_URL + "|2026");
    verify(conceptMapResolver)
        .resolveSupplied(argThat(ref -> ref != null && "cm".equals(ref.getLabel())), eq(supplied));
    verify(conceptMapResolver, never()).resolveCanonical(any(), any());
    verifyNoInteractions(valueSetResolver);
  }

  @Test
  void prefersASuppliedConceptMapOverAStoredViewDefinitionWithTheSameUrl() {
    stubStoredViewDefinition(CONCEPT_MAP_URL, CONCEPT_MAP_URL, "Condition");
    final SuppliedArtefact supplied = suppliedConceptMap(null);
    stubSuppliedConceptMap(supplied, CONCEPT_MAP_URL);

    final ResolvedDependencyGraph graph =
        resolver.resolve(
            sqlQuery("SELECT * FROM cm", "cm", CONCEPT_MAP_URL),
            SuppliedArtefacts.of(List.of(supplied)));

    assertThat(graph.getOrderedNodes()).hasSize(1);
    assertThat(graph.getNodesByKey().get(CONCEPT_MAP_URL)).isInstanceOf(ResolvedConceptMap.class);
    verifyNoInteractions(viewResolver, libraryReferenceResolver);
  }

  @Test
  void matchesAPinnedDependencyToASuppliedConceptMapOnlyWhenTheVersionsAgree() {
    final SuppliedArtefact wrongVersion = suppliedConceptMap("2025");
    stubSuppliedConceptMap(wrongVersion, CONCEPT_MAP_URL + "|2025");
    final ResolvedConceptMap canonical = stubConceptMap(CONCEPT_MAP_URL + "|2026");

    final ResolvedDependencyGraph fellThrough =
        resolver.resolve(
            sqlQuery("SELECT * FROM cm", "cm", CONCEPT_MAP_URL + "|2026"),
            SuppliedArtefacts.of(List.of(wrongVersion)));

    assertThat(fellThrough.getOrderedNodes()).containsExactly(canonical);
    verify(conceptMapResolver, never()).resolveSupplied(any(), any());
    verify(valueSetResolver).resolveCanonical(any(), any());
    verify(conceptMapResolver).resolveCanonical(any(), any());

    final SuppliedArtefact rightVersion = suppliedConceptMap("2026");
    final ResolvedConceptMap suppliedLeaf =
        stubSuppliedConceptMap(rightVersion, CONCEPT_MAP_URL + "|2026");

    final ResolvedDependencyGraph matched =
        resolver.resolve(
            sqlQuery("SELECT * FROM cm", "cm", CONCEPT_MAP_URL + "|2026"),
            SuppliedArtefacts.of(List.of(rightVersion)));

    assertThat(matched.getOrderedNodes()).containsExactly(suppliedLeaf);
    verify(conceptMapResolver).resolveSupplied(any(), eq(rightVersion));
  }

  @Test
  void resolvesASuppliedConceptMapReachedUnderTwoLabelsOnce() {
    final SuppliedArtefact supplied = suppliedConceptMap(null);
    stubSuppliedConceptMap(supplied, CONCEPT_MAP_URL);

    final ResolvedDependencyGraph graph =
        resolver.resolve(
            sqlQueryWithDeps(
                "SELECT * FROM a JOIN b ON a.source_code = b.source_code",
                Map.of("a", CONCEPT_MAP_URL, "b", CONCEPT_MAP_URL)),
            SuppliedArtefacts.of(List.of(supplied)));

    assertThat(graph.getOrderedNodes()).hasSize(1);
    assertThat(graph.getTopLevelKeysByLabel())
        .containsEntry("a", CONCEPT_MAP_URL)
        .containsEntry("b", CONCEPT_MAP_URL);
    verify(conceptMapResolver, times(1)).resolveSupplied(any(), any());
  }

  // ---------------------------------------------------------------------------
  // Helpers.
  // ---------------------------------------------------------------------------

  /** Stubs the value set lookup as indeterminate, carrying one issue with the given text. */
  private void stubIndeterminateValueSet(@Nonnull final String diagnostics) {
    when(valueSetResolver.resolveCanonical(any(), any()))
        .thenThrow(
            new IndeterminateLookupException(SubjectResolver.SUBJECT_EXPRESSION, diagnostics));
  }

  /** Stubs the concept map lookup as indeterminate, carrying one issue with the given text. */
  private void stubIndeterminateConceptMap(@Nonnull final String diagnostics) {
    when(conceptMapResolver.resolveCanonical(any(), any()))
        .thenThrow(
            new IndeterminateLookupException(SubjectResolver.SUBJECT_EXPRESSION, diagnostics));
  }

  /**
   * Asserts that the exception is a 422 whose outcome carries exactly the given kept issues, in
   * order, each an invalid issue at the subject expression, and that it is not itself a single
   * indeterminate lookup.
   */
  private static void assertKeptIssues(
      @Nonnull final Throwable thrown, @Nonnull final String... diagnostics) {
    final BaseServerResponseException exception = (BaseServerResponseException) thrown;
    assertThat(exception.getStatusCode()).isEqualTo(422);
    assertThat(exception).isNotInstanceOf(IndeterminateLookupException.class);
    final OperationOutcome outcome = (OperationOutcome) exception.getOperationOutcome();
    assertThat(outcome.getIssue())
        .extracting(OperationOutcomeIssueComponent::getDiagnostics)
        .containsExactly(diagnostics);
    assertThat(outcome.getIssue())
        .allSatisfy(
            issue -> {
              assertThat(issue.getCode()).isEqualTo(IssueType.INVALID);
              assertThat(issue.getExpression())
                  .extracting(value -> value.getValue())
                  .containsExactly(SubjectResolver.SUBJECT_EXPRESSION);
            });
  }

  /** Builds a top-level SQLQuery ParsedSqlQuery with one dependency. */
  @Nonnull
  private static ParsedSqlQuery sqlQuery(
      @Nonnull final String sql, @Nonnull final String label, @Nonnull final String resource) {
    return sqlQueryWithDeps(sql, Map.of(label, resource));
  }

  /** Builds a top-level SQLQuery ParsedSqlQuery with several dependencies. */
  @Nonnull
  private static ParsedSqlQuery sqlQueryWithDeps(
      @Nonnull final String sql, @Nonnull final Map<String, String> dependenciesByLabel) {
    final List<ViewArtifactReference> references = new ArrayList<>();
    dependenciesByLabel.forEach(
        (label, resource) -> references.add(new ViewArtifactReference(label, resource)));
    return new ParsedSqlQuery(sql, references, List.of(), SqlLibraryParser.SQL_QUERY_TYPE_CODE);
  }

  /**
   * Adds one external table to the configuration and rebuilds the resolver, since the resolver
   * indexes the configured tables when it is constructed.
   */
  private void configureExternalTable(
      @Nonnull final String url, @Nonnull final String path, @Nonnull final String format) {
    final ExternalTableConfiguration table = new ExternalTableConfiguration();
    table.setUrl(url);
    table.setPath(path);
    table.setFormat(format);
    serverConfiguration.getSqlQuery().getExternalTables().add(table);
    resolver = newResolver();
  }

  /** Builds a resolver over the current mocks and configuration. */
  @Nonnull
  private SqlDependencyResolver newResolver() {
    return new SqlDependencyResolver(
        viewResolver,
        libraryReferenceResolver,
        valueSetResolver,
        conceptMapResolver,
        new SqlLibraryParser(),
        serverConfiguration);
  }

  /**
   * Stubs the membership resolver to resolve the given canonical (as written) to a value set leaf
   * with one member, keyed by that canonical, and returns the leaf.
   */
  @Nonnull
  private ResolvedValueSet stubValueSet(@Nonnull final String canonical) {
    final CanonicalReference parsed = CanonicalReference.parse(canonical);
    final ResolvedValueSet leaf =
        new ResolvedValueSet(
            canonical,
            new ValueSetExpansion(
                parsed.getUrl(),
                parsed.getVersion(),
                null,
                null,
                List.of(),
                List.of(
                    new ValueSetMember("http://snomed.info/sct", null, "22298006", null, null))));
    when(valueSetResolver.resolveCanonical(
            argThat(ref -> ref != null && canonical.equals(ref.getCanonicalUrl())), any()))
        .thenReturn(Optional.of(leaf));
    return leaf;
  }

  /**
   * Stubs the concept map resolver to resolve the given canonical (as written) to a concept map
   * leaf with one mapping, keyed by that canonical, and returns the leaf.
   */
  @Nonnull
  private ResolvedConceptMap stubConceptMap(@Nonnull final String canonical) {
    final CanonicalReference parsed = CanonicalReference.parse(canonical);
    final ResolvedConceptMap leaf =
        new ResolvedConceptMap(
            canonical,
            new ConceptMapContent(
                parsed.getUrl(),
                parsed.getVersion(),
                List.of(
                    new ConceptMapping(
                        "http://snomed.info/sct",
                        null,
                        "22298006",
                        null,
                        "http://hl7.org/fhir/sid/icd-10",
                        null,
                        "I21",
                        null,
                        ConceptMapRelationship.EQUIVALENT))));
    when(conceptMapResolver.resolveCanonical(
            argThat(ref -> ref != null && canonical.equals(ref.getCanonicalUrl())), any()))
        .thenReturn(Optional.of(leaf));
    return leaf;
  }

  /** Builds a context entry for a ValueSet at {@link #VALUE_SET_URL} with the given version. */
  @Nonnull
  private static SuppliedArtefact suppliedValueSet(final String version) {
    final ValueSet valueSet = new ValueSet();
    valueSet.setUrl(VALUE_SET_URL);
    valueSet.setVersion(version);
    valueSet.getExpansion().addContains().setSystem("http://snomed.info/sct").setCode("22298006");
    return SuppliedArtefact.ofValueSet(VALUE_SET_URL, version, valueSet);
  }

  /**
   * Stubs the membership resolver to resolve the given supplied artefact to a value set leaf with
   * one member under the given key, and returns the leaf.
   */
  @Nonnull
  private ResolvedValueSet stubSuppliedValueSet(
      @Nonnull final SuppliedArtefact supplied, @Nonnull final String key) {
    final ResolvedValueSet leaf =
        new ResolvedValueSet(
            key,
            new ValueSetExpansion(
                supplied.getUrl(),
                supplied.getVersion(),
                null,
                null,
                List.of(),
                List.of(
                    new ValueSetMember("http://snomed.info/sct", null, "22298006", null, null))));
    when(valueSetResolver.resolveSupplied(any(), eq(supplied))).thenReturn(leaf);
    return leaf;
  }

  /** Builds a context entry for a ConceptMap at {@link #CONCEPT_MAP_URL} with the given version. */
  @Nonnull
  private static SuppliedArtefact suppliedConceptMap(final String version) {
    final ConceptMap conceptMap = new ConceptMap();
    conceptMap.setUrl(CONCEPT_MAP_URL);
    conceptMap.setVersion(version);
    return SuppliedArtefact.ofConceptMap(CONCEPT_MAP_URL, version, conceptMap);
  }

  /**
   * Stubs the concept map resolver to resolve the given supplied artefact to a concept map leaf
   * with no mappings under the given key, and returns the leaf.
   */
  @Nonnull
  private ResolvedConceptMap stubSuppliedConceptMap(
      @Nonnull final SuppliedArtefact supplied, @Nonnull final String key) {
    final ResolvedConceptMap leaf =
        new ResolvedConceptMap(
            key, new ConceptMapContent(supplied.getUrl(), supplied.getVersion(), List.of()));
    when(conceptMapResolver.resolveSupplied(any(), eq(supplied))).thenReturn(leaf);
    return leaf;
  }

  /**
   * Stubs the view resolver to resolve a reference whose canonical url matches {@code referenceUrl}
   * to a stored ViewDefinition over the given resource type, with the given resolved canonical key.
   */
  private void stubStoredViewDefinition(
      @Nonnull final String referenceUrl,
      @Nonnull final String resolvedKey,
      @Nonnull final String resourceType) {
    when(viewResolver.resolveStoredViewDefinition(
            argThat(ref -> ref != null && referenceUrl.equals(ref.getCanonicalUrl()))))
        .thenReturn(Optional.of(new ResolvedViewDefinition(resolvedKey, fhirView(resourceType))));
  }

  /** Stubs the library resolver to return a stored SQLView (carrying {@code url}) with one dep. */
  private void stubSqlView(
      @Nonnull final String url,
      @Nonnull final String sql,
      @Nonnull final String depLabel,
      @Nonnull final String depResource) {
    final Library sqlView = SqlLibraryFixtures.sqlViewWithUrl(url, sql, depLabel, depResource);
    when(libraryReferenceResolver.tryResolveSqlViewLibrary(url)).thenReturn(Optional.of(sqlView));
  }

  @Nonnull
  private static FhirView fhirView(@Nonnull final String resourceType) {
    return FhirView.ofResource(resourceType)
        .select(FhirView.columns(FhirView.column("id", "id")))
        .build();
  }
}
