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
import static org.mockito.Mockito.when;

import au.csiro.pathling.config.AuthorizationConfiguration;
import au.csiro.pathling.config.ServerConfiguration;
import au.csiro.pathling.config.SqlQueryConfiguration;
import au.csiro.pathling.views.FhirView;
import ca.uhn.fhir.rest.server.exceptions.InvalidRequestException;
import ca.uhn.fhir.rest.server.exceptions.ResourceNotFoundException;
import jakarta.annotation.Nonnull;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.hl7.fhir.r4.model.Library;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for {@link SqlDependencyResolver} covering reference disambiguation, the resolved
 * graph shape for a {@code SQLQuery -> SQLView -> ViewDefinition} chain, request-supplied view
 * preference, and the structural rejections (cycles, depth, malformed and wrong-typed
 * dependencies).
 *
 * @author John Grimes
 */
class SqlDependencyResolverTest {

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
  // Disambiguation.
  // ---------------------------------------------------------------------------

  @Test
  void viewDefinitionPrefixResolvesAViewDefinition() {
    stubViewDefinition("ViewDefinition/patient-view", "Patient");

    final ResolvedDependencyGraph graph =
        resolver.resolve(sqlQuery("SELECT * FROM p", "p", "ViewDefinition/patient-view"), Map.of());

    assertThat(graph.getTopLevelKeysByLabel()).containsEntry("p", "ViewDefinition/patient-view");
    assertThat(graph.getNodesByKey().get("ViewDefinition/patient-view"))
        .isInstanceOf(ResolvedViewDefinition.class);
  }

  @Test
  void libraryPrefixResolvesASqlView() {
    // Library/base is a SQLView over a ViewDefinition.
    stubSqlView("base", "SELECT * FROM pv", "pv", "ViewDefinition/patient-view");
    stubViewDefinition("ViewDefinition/patient-view", "Patient");

    final ResolvedDependencyGraph graph =
        resolver.resolve(sqlQuery("SELECT * FROM b", "b", "Library/base"), Map.of());

    assertThat(graph.getTopLevelKeysByLabel()).containsEntry("b", "Library/base");
    assertThat(graph.getNodesByKey().get("Library/base")).isInstanceOf(ResolvedSqlView.class);
    assertThat(graph.getNodesByKey().get("ViewDefinition/patient-view"))
        .isInstanceOf(ResolvedViewDefinition.class);
  }

  @Test
  void bareCanonicalResolvesViewDefinitionFirst() {
    when(viewResolver.tryResolveViewDefinition(
            argThat(ref -> "https://example.org/views/p".equals(ref.getCanonicalUrl())), any()))
        .thenReturn(
            Optional.of(new ResolvedViewDefinition("ViewDefinition/p", fhirView("Patient"))));

    final ResolvedDependencyGraph graph =
        resolver.resolve(sqlQuery("SELECT * FROM p", "p", "https://example.org/views/p"), Map.of());

    assertThat(graph.getTopLevelKeysByLabel()).containsEntry("p", "ViewDefinition/p");
    assertThat(graph.getNodesByKey().get("ViewDefinition/p"))
        .isInstanceOf(ResolvedViewDefinition.class);
  }

  @Test
  void bareCanonicalFallsBackToSqlViewWhenNoViewDefinition() {
    final String canonical = "https://example.org/SQLView/active";
    when(viewResolver.tryResolveViewDefinition(
            argThat(ref -> canonical.equals(ref.getCanonicalUrl())), any()))
        .thenReturn(Optional.empty());
    final Library sqlView = SqlLibraryFixtures.sqlView("SELECT 1");
    sqlView.setId("active");
    sqlView.setUrl(canonical);
    when(libraryReferenceResolver.resolve(argThat(ref -> canonical.equals(ref.getReference()))))
        .thenReturn(sqlView);

    final ResolvedDependencyGraph graph =
        resolver.resolve(sqlQuery("SELECT * FROM a", "a", canonical), Map.of());

    assertThat(graph.getTopLevelKeysByLabel()).containsEntry("a", "Library/active");
    assertThat(graph.getNodesByKey().get("Library/active")).isInstanceOf(ResolvedSqlView.class);
  }

  @Test
  void unresolvableReferenceErrorsNamingLabelAndResource() {
    when(libraryReferenceResolver.resolve(any()))
        .thenThrow(new ResourceNotFoundException("not found"));

    assertThatThrownBy(
            () -> resolver.resolve(sqlQuery("SELECT 1", "x", "Library/missing"), Map.of()))
        .isInstanceOf(InvalidRequestException.class)
        .hasMessageContaining("x")
        .hasMessageContaining("Library/missing");
  }

  @Test
  void sqlQueryReferencedAsDependencyIsRejected() {
    // A Library/q that is itself a sql-query (not a sql-view) cannot be a dependency.
    final Library nested = SqlLibraryFixtures.sqlQuery("SELECT 1");
    nested.setId("q");
    when(libraryReferenceResolver.resolve(argThat(ref -> "Library/q".equals(ref.getReference()))))
        .thenReturn(nested);

    assertThatThrownBy(() -> resolver.resolve(sqlQuery("SELECT 1", "q", "Library/q"), Map.of()))
        .isInstanceOf(InvalidRequestException.class)
        .hasMessageContaining("sql-query")
        .hasMessageContaining("SQLView");
  }

  // ---------------------------------------------------------------------------
  // Graph shape.
  // ---------------------------------------------------------------------------

  @Test
  void buildsTopologicallyOrderedTwoNodeGraph() {
    stubSqlView("base", "SELECT * FROM pv", "pv", "ViewDefinition/patient-view");
    stubViewDefinition("ViewDefinition/patient-view", "Patient");

    final ResolvedDependencyGraph graph =
        resolver.resolve(sqlQuery("SELECT * FROM b", "b", "Library/base"), Map.of());

    // The ViewDefinition leaf must appear before the SQLView that depends on it.
    assertThat(graph.getOrderedNodes()).hasSize(2);
    assertThat(graph.getOrderedNodes().get(0).getCanonicalKey())
        .isEqualTo("ViewDefinition/patient-view");
    assertThat(graph.getOrderedNodes().get(1).getCanonicalKey()).isEqualTo("Library/base");

    final ResolvedSqlView sqlView = (ResolvedSqlView) graph.getNodesByKey().get("Library/base");
    assertThat(sqlView.getChildKeysByLabel()).containsEntry("pv", "ViewDefinition/patient-view");
    assertThat(graph.getTopLevelKeysByLabel()).containsEntry("b", "Library/base");
  }

  @Test
  void prefersRequestSuppliedViewDefinitionOverStorage() {
    final FhirView supplied = fhirView("Patient");
    when(viewResolver.resolveViewDefinition(
            argThat(ref -> "ViewDefinition/patient-view".equals(ref.getCanonicalUrl())),
            argThat(map -> map.containsKey("patient-view"))))
        .thenReturn(new ResolvedViewDefinition("ViewDefinition/patient-view", supplied));

    final ResolvedDependencyGraph graph =
        resolver.resolve(
            sqlQuery("SELECT * FROM p", "p", "ViewDefinition/patient-view"),
            Map.of("patient-view", supplied));

    final ResolvedViewDefinition node =
        (ResolvedViewDefinition) graph.getNodesByKey().get("ViewDefinition/patient-view");
    assertThat(node.getView()).isSameAs(supplied);
  }

  // ---------------------------------------------------------------------------
  // Nested graphs, diamonds, cycles, depth, and label scoping (US2).
  // ---------------------------------------------------------------------------

  @Test
  void resolvesAThreeLevelNestedChain() {
    // SQLQuery -> v1 (SQLView) -> v2 (SQLView) -> ViewDefinition.
    stubSqlView("v1", "SELECT * FROM x", "x", "Library/v2");
    stubSqlView("v2", "SELECT * FROM pv", "pv", "ViewDefinition/patient-view");
    stubViewDefinition("ViewDefinition/patient-view", "Patient");

    final ResolvedDependencyGraph graph =
        resolver.resolve(sqlQuery("SELECT * FROM v", "v", "Library/v1"), Map.of());

    assertThat(graph.getOrderedNodes()).hasSize(3);
    // Dependencies precede dependents: VD, then v2, then v1.
    assertThat(graph.getOrderedNodes().get(0).getCanonicalKey())
        .isEqualTo("ViewDefinition/patient-view");
    assertThat(graph.getOrderedNodes().get(1).getCanonicalKey()).isEqualTo("Library/v2");
    assertThat(graph.getOrderedNodes().get(2).getCanonicalKey()).isEqualTo("Library/v1");
  }

  @Test
  void resolvesADiamondSharedNodeOnce() {
    // SQLQuery references both left and right, each of which references the same shared SQLView.
    stubSqlView("left", "SELECT * FROM s", "s", "Library/shared");
    stubSqlView("right", "SELECT * FROM s", "s", "Library/shared");
    stubSqlView("shared", "SELECT * FROM pv", "pv", "ViewDefinition/patient-view");
    stubViewDefinition("ViewDefinition/patient-view", "Patient");

    final ResolvedDependencyGraph graph =
        resolver.resolve(
            sqlQueryWithDeps(
                "SELECT * FROM l JOIN r", Map.of("l", "Library/left", "r", "Library/right")),
            Map.of());

    // The shared node and the ViewDefinition each appear exactly once.
    assertThat(graph.getNodesByKey()).containsKey("Library/shared");
    final long sharedCount =
        graph.getOrderedNodes().stream()
            .filter(node -> "Library/shared".equals(node.getCanonicalKey()))
            .count();
    assertThat(sharedCount).isEqualTo(1);
    assertThat(graph.getOrderedNodes()).hasSize(4); // shared, vd, left, right.
  }

  @Test
  void resolvesTheSameLabelInDifferentNodesWithoutCollision() {
    // v1 and v2 both use label "t", but for different ViewDefinitions.
    stubSqlView("v1", "SELECT * FROM t", "t", "ViewDefinition/a");
    stubSqlView("v2", "SELECT * FROM t", "t", "ViewDefinition/b");
    stubViewDefinition("ViewDefinition/a", "Patient");
    stubViewDefinition("ViewDefinition/b", "Observation");

    final ResolvedDependencyGraph graph =
        resolver.resolve(
            sqlQueryWithDeps(
                "SELECT * FROM one, two", Map.of("one", "Library/v1", "two", "Library/v2")),
            Map.of());

    final ResolvedSqlView v1 = (ResolvedSqlView) graph.getNodesByKey().get("Library/v1");
    final ResolvedSqlView v2 = (ResolvedSqlView) graph.getNodesByKey().get("Library/v2");
    assertThat(v1.getChildKeysByLabel()).containsEntry("t", "ViewDefinition/a");
    assertThat(v2.getChildKeysByLabel()).containsEntry("t", "ViewDefinition/b");
  }

  @Test
  void rejectsACycleNamingTheChain() {
    stubSqlView("a", "SELECT * FROM b", "b", "Library/b");
    stubSqlView("b", "SELECT * FROM a", "a", "Library/a");

    assertThatThrownBy(
            () -> resolver.resolve(sqlQuery("SELECT * FROM x", "x", "Library/a"), Map.of()))
        .isInstanceOf(InvalidRequestException.class)
        .hasMessageContainingAll("Cyclic", "Library/a", "Library/b");
  }

  @Test
  void rejectsASelfReference() {
    stubSqlView("self", "SELECT * FROM s", "s", "Library/self");

    assertThatThrownBy(
            () -> resolver.resolve(sqlQuery("SELECT * FROM x", "x", "Library/self"), Map.of()))
        .isInstanceOf(InvalidRequestException.class)
        .hasMessageContaining("Cyclic");
  }

  @Test
  void rejectsAGraphDeeperThanTheConfiguredLimit() {
    serverConfiguration.getSqlQuery().setMaxDependencyDepth(2);
    // top -> v1 (depth 1) -> v2 (depth 2) -> v3 (depth 3, exceeds the limit of 2).
    stubSqlView("v1", "SELECT * FROM x", "x", "Library/v2");
    stubSqlView("v2", "SELECT * FROM y", "y", "Library/v3");
    stubSqlView("v3", "SELECT * FROM pv", "pv", "ViewDefinition/patient-view");
    stubViewDefinition("ViewDefinition/patient-view", "Patient");

    assertThatThrownBy(
            () -> resolver.resolve(sqlQuery("SELECT * FROM v", "v", "Library/v1"), Map.of()))
        .isInstanceOf(InvalidRequestException.class)
        .hasMessageContainingAll("deeper", "2");
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
    final List<ViewArtifactReference> references = new java.util.ArrayList<>();
    dependenciesByLabel.forEach(
        (label, resource) -> references.add(new ViewArtifactReference(label, resource)));
    return new ParsedSqlQuery(sql, references, List.of(), SqlLibraryParser.SQL_QUERY_TYPE_CODE);
  }

  /** Stubs the view resolver to resolve the given reference to a ViewDefinition over a resource. */
  private void stubViewDefinition(
      @Nonnull final String reference, @Nonnull final String resourceType) {
    final String key =
        reference.startsWith("ViewDefinition/") ? reference : "ViewDefinition/" + reference;
    when(viewResolver.resolveViewDefinition(
            argThat(ref -> ref != null && reference.equals(ref.getCanonicalUrl())), any()))
        .thenReturn(new ResolvedViewDefinition(key, fhirView(resourceType)));
  }

  /** Stubs the library resolver to return a stored SQLView with one dependency. */
  private void stubSqlView(
      @Nonnull final String id,
      @Nonnull final String sql,
      @Nonnull final String depLabel,
      @Nonnull final String depResource) {
    final Library sqlView = SqlLibraryFixtures.sqlView(sql, depLabel, depResource);
    sqlView.setId(id);
    when(libraryReferenceResolver.resolve(
            argThat(ref -> ref != null && ("Library/" + id).equals(ref.getReference()))))
        .thenReturn(sqlView);
  }

  @Nonnull
  private static FhirView fhirView(@Nonnull final String resourceType) {
    return FhirView.ofResource(resourceType)
        .select(FhirView.columns(FhirView.column("id", "id")))
        .build();
  }
}
