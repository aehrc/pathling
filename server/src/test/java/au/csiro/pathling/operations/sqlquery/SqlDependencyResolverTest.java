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
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import au.csiro.pathling.config.AuthorizationConfiguration;
import au.csiro.pathling.config.ServerConfiguration;
import au.csiro.pathling.config.SqlQueryConfiguration;
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
 * graph shape for a {@code SQLQuery -> SQLView -> ViewDefinition} chain, request-supplied view
 * preference, diamond de-duplication (including bare-url vs {@code url|version}), and the
 * structural rejections (cycles, depth, ambiguity, not-found, and wrong-typed dependencies).
 *
 * @author John Grimes
 */
class SqlDependencyResolverTest {

  private static final String PATIENT_VIEW_URL =
      SqlLibraryFixtures.viewDefinitionUrl("patient-view");

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
        resolver.resolve(sqlQuery("SELECT * FROM p", "p", PATIENT_VIEW_URL), Map.of());

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
        resolver.resolve(sqlQuery("SELECT * FROM b", "b", baseUrl), Map.of());

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
        resolver.resolve(sqlQuery("SELECT * FROM v", "v", v1Url), Map.of());

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
        resolver.resolve(sqlQuery("SELECT * FROM b", "b", baseUrl), Map.of());

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
            Map.of());

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
            Map.of());

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
            Map.of());

    final ResolvedSqlView v1 = (ResolvedSqlView) graph.getNodesByKey().get(v1Url);
    final ResolvedSqlView v2 = (ResolvedSqlView) graph.getNodesByKey().get(v2Url);
    assertThat(v1.getChildKeysByLabel()).containsEntry("t", aUrl);
    assertThat(v2.getChildKeysByLabel()).containsEntry("t", bUrl);
  }

  @Test
  void prefersARequestSuppliedViewOverStorage() {
    final FhirView supplied = fhirView("Patient");
    when(viewResolver.resolveSuppliedView(
            argThat(ref -> ref != null && PATIENT_VIEW_URL.equals(ref.getCanonicalUrl())),
            argThat(map -> map.containsKey(PATIENT_VIEW_URL))))
        .thenReturn(Optional.of(new ResolvedViewDefinition(PATIENT_VIEW_URL, supplied)));

    final ResolvedDependencyGraph graph =
        resolver.resolve(
            sqlQuery("SELECT * FROM p", "p", PATIENT_VIEW_URL), Map.of(PATIENT_VIEW_URL, supplied));

    final ResolvedViewDefinition node =
        (ResolvedViewDefinition) graph.getNodesByKey().get(PATIENT_VIEW_URL);
    assertThat(node.getView()).isSameAs(supplied);
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

    assertThatThrownBy(() -> resolver.resolve(sqlQuery("SELECT * FROM x", "x", aUrl), Map.of()))
        .isInstanceOf(InvalidRequestException.class)
        .hasMessageContainingAll("Cyclic", aUrl, bUrl);
  }

  @Test
  void rejectsASelfReference() {
    final String selfUrl = SqlLibraryFixtures.sqlViewUrl("self");
    stubSqlView(selfUrl, "SELECT * FROM s", "s", selfUrl);

    assertThatThrownBy(() -> resolver.resolve(sqlQuery("SELECT * FROM x", "x", selfUrl), Map.of()))
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

    assertThatThrownBy(() -> resolver.resolve(sqlQuery("SELECT * FROM v", "v", v1Url), Map.of()))
        .isInstanceOf(InvalidRequestException.class)
        .hasMessageContainingAll("deeper", "2");
  }

  // ---------------------------------------------------------------------------
  // Error surface (US2): not-found, ambiguity, wrong-typed dependency.
  // ---------------------------------------------------------------------------

  @Test
  void reportsNotFoundWhenNeitherAViewDefinitionNorASqlViewMatches() {
    final String missingUrl = SqlLibraryFixtures.viewDefinitionUrl("missing");

    assertThatThrownBy(() -> resolver.resolve(sqlQuery("SELECT 1", "x", missingUrl), Map.of()))
        .isInstanceOf(ResourceNotFoundException.class)
        .hasMessageContaining("x")
        .hasMessageContaining(missingUrl);
  }

  @Test
  void rejectsAnAmbiguousReferenceMatchingBothTypes() {
    final String clashUrl = SqlLibraryFixtures.viewDefinitionUrl("clash");
    stubStoredViewDefinition(clashUrl, clashUrl, "Patient");
    stubSqlView(clashUrl, "SELECT 1", "p", PATIENT_VIEW_URL);

    assertThatThrownBy(() -> resolver.resolve(sqlQuery("SELECT 1", "c", clashUrl), Map.of()))
        .isInstanceOf(InvalidRequestException.class)
        .hasMessageContaining("ambiguous")
        .hasMessageContaining("c")
        .hasMessageContaining(clashUrl);
  }

  @Test
  void rejectsASqlQueryReferencedAsADependency() {
    // A Library that is itself a sql-query (not a sql-view) cannot be a dependency.
    final String queryUrl = SqlLibraryFixtures.sqlViewUrl("q");
    final Library nested = SqlLibraryFixtures.sqlQuery("SELECT 1");
    nested.setUrl(queryUrl);
    when(libraryReferenceResolver.tryResolveSqlViewLibrary(queryUrl))
        .thenReturn(Optional.of(nested));

    assertThatThrownBy(() -> resolver.resolve(sqlQuery("SELECT 1", "q", queryUrl), Map.of()))
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
