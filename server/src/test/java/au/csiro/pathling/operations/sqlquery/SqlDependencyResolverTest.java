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
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import au.csiro.pathling.config.AuthorizationConfiguration;
import au.csiro.pathling.config.ExternalTableConfiguration;
import au.csiro.pathling.config.ServerConfiguration;
import au.csiro.pathling.config.SqlQueryConfiguration;
import au.csiro.pathling.operations.sql.SuppliedArtefact;
import au.csiro.pathling.operations.sql.SuppliedArtefacts;
import au.csiro.pathling.views.FhirView;
import ca.uhn.fhir.rest.server.exceptions.InvalidRequestException;
import ca.uhn.fhir.rest.server.exceptions.ResourceNotFoundException;
import jakarta.annotation.Nonnull;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.hl7.fhir.r4.model.Library;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for {@link SqlDependencyResolver} covering canonical-URL resolution, the resolved
 * graph shape for a {@code SQLQuery -> SQLView -> ViewDefinition} chain, supplied-artefact
 * precedence and traversal, diamond de-duplication (including bare-url vs {@code url|version}),
 * configured external tables, and the structural rejections (cycles, depth, ambiguity, not-found,
 * and wrong-typed dependencies).
 *
 * @author John Grimes
 */
class SqlDependencyResolverTest {

  private static final String PATIENT_VIEW_URL =
      SqlLibraryFixtures.viewDefinitionUrl("patient-view");

  private static final String TABLE_URL = "https://example.org/data/cohorts";

  private static final String TABLE_PATH = "file:///data/reference/cohorts";

  private ViewResolver viewResolver;
  private LibraryReferenceResolver libraryReferenceResolver;
  private ServerConfiguration serverConfiguration;
  private SqlDependencyResolver resolver;

  @BeforeEach
  void setUp() {
    viewResolver = mock(ViewResolver.class);
    libraryReferenceResolver = mock(LibraryReferenceResolver.class);
    serverConfiguration = new ServerConfiguration();
    final AuthorizationConfiguration auth = new AuthorizationConfiguration();
    auth.setEnabled(false);
    serverConfiguration.setAuth(auth);
    serverConfiguration.setSqlQuery(new SqlQueryConfiguration());
    resolver =
        new SqlDependencyResolver(
            viewResolver, libraryReferenceResolver, new SqlLibraryParser(), serverConfiguration);
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
            "no ViewDefinition, SQLView or external table matches that canonical URL");
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

    assertThatThrownBy(
            () ->
                resolver.resolve(sqlQuery("SELECT 1", "x", missingUrl), SuppliedArtefacts.empty()))
        .isInstanceOf(ResourceNotFoundException.class)
        .hasMessageContainingAll("'x'", missingUrl)
        .hasMessageEndingWith(
            "no ViewDefinition, SQLView or external table matches that canonical URL");
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
  // Helpers.
  // ---------------------------------------------------------------------------

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
    resolver =
        new SqlDependencyResolver(
            viewResolver, libraryReferenceResolver, new SqlLibraryParser(), serverConfiguration);
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
