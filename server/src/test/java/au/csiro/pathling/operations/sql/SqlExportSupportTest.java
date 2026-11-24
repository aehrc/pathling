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

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

import au.csiro.pathling.async.JobRegistry;
import au.csiro.pathling.async.RequestTagFactory;
import au.csiro.pathling.config.ServerConfiguration;
import au.csiro.pathling.operations.bulkexport.ExportResultRegistry;
import au.csiro.pathling.operations.export.ExportFileWriter;
import au.csiro.pathling.operations.sqlquery.ParsedSqlQuery;
import au.csiro.pathling.operations.sqlquery.PreparedSqlQuery;
import au.csiro.pathling.operations.sqlquery.ResolvedConceptMap;
import au.csiro.pathling.operations.sqlquery.ResolvedDependency;
import au.csiro.pathling.operations.sqlquery.ResolvedDependencyGraph;
import au.csiro.pathling.operations.sqlquery.ResolvedExternalTable;
import au.csiro.pathling.operations.sqlquery.ResolvedSqlView;
import au.csiro.pathling.operations.sqlquery.ResolvedValueSet;
import au.csiro.pathling.operations.sqlquery.ResolvedViewDefinition;
import au.csiro.pathling.operations.sqlquery.SqlLibraryParser;
import au.csiro.pathling.operations.sqlquery.SqlQueryRequest;
import au.csiro.pathling.terminology.conceptmap.ConceptMapContent;
import au.csiro.pathling.terminology.conceptmap.ConceptMapping;
import au.csiro.pathling.terminology.expand.ValueSetExpansion;
import au.csiro.pathling.terminology.expand.ValueSetMember;
import au.csiro.pathling.views.FhirView;
import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for the {@code $sql-export} cache key, which decides whether a kick-off deduplicates
 * onto an existing job.
 *
 * <p>The key has to separate two kick-offs whose subjects are identical but whose dependencies
 * resolved to different content, which is what happens when a client inlines a different {@code
 * context} body at a canonical URL it has used before (aehrc/pathling#2757). Equally it has to keep
 * matching kick-offs that really are the same, since that is what the job cache is for.
 *
 * <p>The graphs here are built directly rather than resolved, so the tests state exactly which
 * difference is under examination without needing Spark or a repository.
 *
 * @author John Grimes
 */
class SqlExportSupportTest {

  private static final String VIEW_URL = "https://pathling.csiro.au/test/crit_a";

  private static final String CHILD_URL = "https://pathling.csiro.au/test/crit_b";

  private static final String SUBJECT_SQL = "SELECT count(*) AS n FROM crit_a";

  private SqlExportSupport support;

  @BeforeEach
  void setUp() {
    // The key is computed from the parsed request alone, so none of these collaborators
    // participate.
    support =
        new SqlExportSupport(
            mock(SqlExportExecutor.class),
            mock(JobRegistry.class),
            mock(RequestTagFactory.class),
            mock(ExportResultRegistry.class),
            mock(ServerConfiguration.class),
            mock(ExportFileWriter.class));
  }

  // -------------------------------------------------------------------------
  // Content of the resolved closure.
  // -------------------------------------------------------------------------

  // The defect reported in aehrc/pathling#2757: the subject is byte-identical and the dependency
  // still resolves under the same canonical URL, so the only thing separating the two kick-offs
  // is the body that URL resolved to. Keying on the label-to-URL mapping alone made these two
  // requests the same job, and the second was answered with the first one's rows.
  @Test
  void aDifferentSqlViewBodyAtTheSameUrlProducesADifferentKey() {
    final String first =
        support.computeCacheKeyComponent(
            requestOver(sqlViewGraph("SELECT patient_key FROM table_one")));
    final String second =
        support.computeCacheKeyComponent(
            requestOver(sqlViewGraph("SELECT patient_key FROM table_two WHERE 1 = 0")));

    assertThat(second).isNotEqualTo(first);
  }

  // The same separation is needed for a ViewDefinition leaf, whose content is its projection
  // rather than SQL text.
  @Test
  void aDifferentViewDefinitionBodyAtTheSameUrlProducesADifferentKey() {
    final String first =
        support.computeCacheKeyComponent(requestOver(viewDefinitionGraph("name.first().family")));
    final String second =
        support.computeCacheKeyComponent(requestOver(viewDefinitionGraph("name.first().given")));

    assertThat(second).isNotEqualTo(first);
  }

  // An external table's identity is the location it reads, which the operator can repoint at a
  // different dataset while the canonical URL stays as it was.
  @Test
  void aDifferentExternalTableTargetAtTheSameUrlProducesADifferentKey() {
    final String first =
        support.computeCacheKeyComponent(
            requestOver(externalTableGraph("s3://bucket/one", "delta")));
    final String second =
        support.computeCacheKeyComponent(
            requestOver(externalTableGraph("s3://bucket/two", "delta")));
    final String reformatted =
        support.computeCacheKeyComponent(
            requestOver(externalTableGraph("s3://bucket/one", "parquet")));

    assertThat(second).isNotEqualTo(first);
    assertThat(reformatted).isNotEqualTo(first);
  }

  // A change anywhere in the closure counts, not just at the top level. The subject reaches this
  // node only through the SQLView it does reference, so a key covering just the subject's own
  // dependencies would miss it.
  @Test
  void aDifferentBodyDeeperInTheClosureProducesADifferentKey() {
    final String first =
        support.computeCacheKeyComponent(requestOver(nestedGraph("SELECT * FROM base")));
    final String second =
        support.computeCacheKeyComponent(
            requestOver(nestedGraph("SELECT * FROM base WHERE 1 = 0")));

    assertThat(second).isNotEqualTo(first);
  }

  // A value set leaf's rows are its members, so two kick-offs that inline different memberships at
  // the one canonical URL would export different rows and must not share a job (spec 061, US3
  // scenario 5).
  @Test
  void aDifferentValueSetMembershipAtTheSameUrlProducesADifferentKey() {
    final String first =
        support.computeCacheKeyComponent(
            requestOver(valueSetGraph(member("22298006"), member("I21"))));
    final String second =
        support.computeCacheKeyComponent(
            requestOver(valueSetGraph(member("22298006"), member("73211009"))));

    assertThat(second).isNotEqualTo(first);
  }

  // A concept map leaf's rows are its mappings, so two kick-offs that inline different maps at the
  // one canonical URL would export different translations and must not share a job (spec 062, US3
  // scenario 5).
  @Test
  void aDifferentConceptMapAtTheSameUrlProducesADifferentKey() {
    final String first =
        support.computeCacheKeyComponent(
            requestOver(conceptMapGraph(mapping("22298006", "I21", "equivalent"))));
    final String second =
        support.computeCacheKeyComponent(
            requestOver(conceptMapGraph(mapping("22298006", "I22", "equivalent"))));
    final String relabelled =
        support.computeCacheKeyComponent(
            requestOver(conceptMapGraph(mapping("22298006", "I21", "related-to"))));

    assertThat(second).isNotEqualTo(first);
    assertThat(relabelled).isNotEqualTo(first);
  }

  // -------------------------------------------------------------------------
  // Kick-offs that really are the same.
  // -------------------------------------------------------------------------

  // Deduplication is the point of the key, so an identical kick-off must still match.
  @Test
  void anIdenticalRequestProducesTheSameKey() {
    final String first =
        support.computeCacheKeyComponent(
            requestOver(sqlViewGraph("SELECT patient_key FROM table_one")));
    final String second =
        support.computeCacheKeyComponent(
            requestOver(sqlViewGraph("SELECT patient_key FROM table_one")));

    assertThat(second).isEqualTo(first);
  }

  // A ViewDefinition leaf describes itself through its parsed projection, whose rendering has to
  // be value-based for two separately parsed copies of one body to match. An identity-based
  // rendering would leave every inequality test above green while silently stopping every
  // ViewDefinition-backed kick-off from ever deduplicating.
  @Test
  void anIdenticalViewDefinitionDependencyProducesTheSameKey() {
    final String first =
        support.computeCacheKeyComponent(requestOver(viewDefinitionGraph("name.first().family")));
    final String second =
        support.computeCacheKeyComponent(requestOver(viewDefinitionGraph("name.first().family")));

    assertThat(second).isEqualTo(first);
  }

  // The resolver memoises nodes into a map shared across the subjects of a job, so the iteration
  // order of a closure depends on the order the job's subjects were resolved in. That is not a
  // difference in what will be executed, and must not fork a new job.
  @Test
  void theSameClosureReachedInADifferentOrderProducesTheSameKey() {
    final String ascending = support.computeCacheKeyComponent(requestOver(twoLabelGraph(false)));
    final String descending = support.computeCacheKeyComponent(requestOver(twoLabelGraph(true)));

    assertThat(descending).isEqualTo(ascending);
  }

  // Two separately resolved copies of one membership describe themselves identically, so a
  // repeated kick-off still deduplicates onto the job that is already running.
  @Test
  void anIdenticalValueSetMembershipProducesTheSameKey() {
    final String first =
        support.computeCacheKeyComponent(
            requestOver(valueSetGraph(member("22298006"), member("I21"))));
    final String second =
        support.computeCacheKeyComponent(
            requestOver(valueSetGraph(member("22298006"), member("I21"))));

    assertThat(second).isEqualTo(first);
  }

  // Two separately resolved copies of one concept map describe themselves identically, so a
  // repeated kick-off still deduplicates onto the job that is already running.
  @Test
  void anIdenticalConceptMapProducesTheSameKey() {
    final String first =
        support.computeCacheKeyComponent(
            requestOver(
                conceptMapGraph(
                    mapping("22298006", "I21", "equivalent"), mapping("73211009", "E14", null))));
    final String second =
        support.computeCacheKeyComponent(
            requestOver(
                conceptMapGraph(
                    mapping("22298006", "I21", "equivalent"), mapping("73211009", "E14", null))));

    assertThat(second).isEqualTo(first);
  }

  // A dependency swapped for a different resource is a different request even when both bodies
  // are identical, because the label now points somewhere else.
  @Test
  void theSameBodyUnderADifferentUrlProducesADifferentKey() {
    final String first =
        support.computeCacheKeyComponent(
            requestOver(sqlViewGraph(VIEW_URL, "SELECT patient_key FROM table_one")));
    final String second =
        support.computeCacheKeyComponent(
            requestOver(sqlViewGraph(CHILD_URL, "SELECT patient_key FROM table_one")));

    assertThat(second).isNotEqualTo(first);
  }

  // -------------------------------------------------------------------------
  // Values that reproduce the key's own delimiters (aehrc/pathling#2768).
  // -------------------------------------------------------------------------

  // The worked example from aehrc/pathling#2768. The first graph is three SQLViews deep, and the
  // second is two deep, with the top view's SQL carrying, after a line comment, a rendering of the
  // middle view of the first graph. Rendered without escaping the two graphs produced the same
  // dependency description, so the second kick-off was answered with the first one's rows.
  @Test
  void aSqlViewBodyThatForgesAnotherGraphProducesADifferentKey() {
    final String v1 = "https://example.org/Library/v1";
    final String v2 = "https://example.org/Library/v2";
    final String v3 = "https://example.org/Library/v3";
    final ResolvedSqlView leaf = new ResolvedSqlView(v3, "SELECT 1 AS n", Map.of());
    final String honest =
        support.computeCacheKeyComponent(
            requestOver(
                graph(
                    Map.of("crit_a", v1),
                    new ResolvedSqlView(v1, "SELECT * FROM lbl --", Map.of("lbl", v2)),
                    new ResolvedSqlView(v2, "SELECT n * 10 AS n FROM lbl", Map.of("lbl", v3)),
                    leaf)));
    final String forged =
        support.computeCacheKeyComponent(
            requestOver(
                graph(
                    Map.of("crit_a", v1),
                    new ResolvedSqlView(
                        v1,
                        "SELECT * FROM lbl --:{lbl="
                            + v2
                            + "},"
                            + v2
                            + "=sql-view:SELECT n * 10 AS n FROM lbl",
                        Map.of("lbl", v3)),
                    leaf)));

    assertThat(forged).isNotEqualTo(honest);
  }

  // The subjects section has the same weakness: one subject's SQL can carry, after a line comment,
  // the rendering of a second subject, so that one subject renders as two.
  @Test
  void aSubjectSqlThatForgesASecondSubjectProducesADifferentKey() {
    final ResolvedDependencyGraph empty = graph(Map.of());
    final String honest =
        support.computeCacheKeyComponent(
            requestOf(
                sqlSubject("n1", "SELECT 1 AS n --", Map.of(), empty),
                sqlSubject("n2", "SELECT 2 AS n", Map.of(), empty)));
    final String forged =
        support.computeCacheKeyComponent(
            requestOf(
                sqlSubject(
                    "n1",
                    "SELECT 1 AS n --:{}:{},n2:" + SubjectKind.SQL_QUERY + ":SELECT 2 AS n",
                    Map.of(),
                    empty)));

    assertThat(forged).isNotEqualTo(honest);
  }

  // A string parameter value can reproduce the rendering of a second binding, so that one binding
  // renders as two.
  @Test
  void aParameterValueThatForgesASecondBindingProducesADifferentKey() {
    final Map<String, Object> two = new LinkedHashMap<>();
    two.put("p", "1");
    two.put("q", "2");
    final String honest =
        support.computeCacheKeyComponent(
            requestOf(sqlSubject("subject", SUBJECT_SQL, two, sqlViewGraph("SELECT 1"))));
    final String forged =
        support.computeCacheKeyComponent(
            requestOf(
                sqlSubject(
                    "subject", SUBJECT_SQL, Map.of("p", "1, q=2"), sqlViewGraph("SELECT 1"))));

    assertThat(forged).isNotEqualTo(honest);
  }

  // A ViewDefinition dependency is described by its projection. A column path that reproduces the
  // rendering of a second column must not make one column indistinguishable from two.
  @Test
  void aViewDefinitionDependencyThatForgesASecondColumnProducesADifferentKey() {
    final String honest =
        support.computeCacheKeyComponent(requestOver(viewDefinitionGraph(twoColumnView())));
    final String forged =
        support.computeCacheKeyComponent(requestOver(viewDefinitionGraph(forgedColumnView())));

    assertThat(forged).isNotEqualTo(honest);
  }

  // A ViewDefinition subject is described by its projection in the same way as a ViewDefinition
  // dependency, and needs the same protection.
  @Test
  void aViewDefinitionSubjectThatForgesASecondColumnProducesADifferentKey() {
    final String honest =
        support.computeCacheKeyComponent(
            requestOf(SubjectInput.ofView("subject", twoColumnView())));
    final String forged =
        support.computeCacheKeyComponent(
            requestOf(SubjectInput.ofView("subject", forgedColumnView())));

    assertThat(forged).isNotEqualTo(honest);
  }

  // Escaping must not get in the way of deduplication: the same delimiter-laden values, parsed
  // twice, still produce the same key.
  @Test
  void anIdenticalRequestCarryingDelimitersProducesTheSameKey() {
    final String sql = "SELECT * FROM lbl --:{lbl=x},y=[z]|%3A";
    final String first =
        support.computeCacheKeyComponent(
            requestOf(
                sqlSubject("a:b", sql, Map.of("p", "1, q=2"), sqlViewGraph(VIEW_URL, sql)),
                SubjectInput.ofView("c,d", forgedColumnView())));
    final String second =
        support.computeCacheKeyComponent(
            requestOf(
                sqlSubject("a:b", sql, Map.of("p", "1, q=2"), sqlViewGraph(VIEW_URL, sql)),
                SubjectInput.ofView("c,d", forgedColumnView())));

    assertThat(second).isEqualTo(first);
  }

  // ---- view builders ----

  /** A Patient view with two columns, {@code v} and {@code w}. */
  @Nonnull
  private static FhirView twoColumnView() {
    return FhirView.ofResource("Patient")
        .select(FhirView.columns(FhirView.column("v", "x"), FhirView.column("w", "y")))
        .build();
  }

  /**
   * A Patient view with a single column whose path reproduces the rendering of the second column of
   * {@link #twoColumnView()}.
   */
  @Nonnull
  private static FhirView forgedColumnView() {
    final String rest = ", description=null, collection=false, type=null, tag=[])";
    return FhirView.ofResource("Patient")
        .select(FhirView.columns(FhirView.column("v", "x" + rest + ", Column(name=w, path=y")))
        .build();
  }

  // ---- graph builders ----

  /** A graph whose single dependency is a SQLView carrying the given SQL, keyed by VIEW_URL. */
  @Nonnull
  private static ResolvedDependencyGraph sqlViewGraph(@Nonnull final String sql) {
    return sqlViewGraph(VIEW_URL, sql);
  }

  /** A graph whose single dependency is a SQLView carrying the given SQL, under the given key. */
  @Nonnull
  private static ResolvedDependencyGraph sqlViewGraph(
      @Nonnull final String key, @Nonnull final String sql) {
    return graph(Map.of("crit_a", key), new ResolvedSqlView(key, sql, Map.of()));
  }

  /** A graph whose single dependency is a ViewDefinition projecting the given path. */
  @Nonnull
  private static ResolvedDependencyGraph viewDefinitionGraph(@Nonnull final String path) {
    return viewDefinitionGraph(
        FhirView.ofResource("Patient")
            .select(FhirView.columns(FhirView.column("v", path)))
            .build());
  }

  /** A graph whose single dependency is the given ViewDefinition. */
  @Nonnull
  private static ResolvedDependencyGraph viewDefinitionGraph(@Nonnull final FhirView view) {
    return graph(Map.of("crit_a", VIEW_URL), new ResolvedViewDefinition(VIEW_URL, view));
  }

  /** A graph whose single dependency is an external table at the given path and format. */
  @Nonnull
  private static ResolvedDependencyGraph externalTableGraph(
      @Nonnull final String path, @Nonnull final String format) {
    return graph(Map.of("crit_a", VIEW_URL), new ResolvedExternalTable(VIEW_URL, path, format));
  }

  /** A graph whose single dependency is a value set holding the given members. */
  @Nonnull
  private static ResolvedDependencyGraph valueSetGraph(@Nonnull final ValueSetMember... members) {
    final ValueSetExpansion expansion =
        new ValueSetExpansion(VIEW_URL, null, null, null, List.of(), List.of(members));
    return graph(Map.of("crit_a", VIEW_URL), new ResolvedValueSet(VIEW_URL, expansion));
  }

  /** A SNOMED CT member with the given code and no display or inactive flag. */
  @Nonnull
  private static ValueSetMember member(@Nonnull final String code) {
    return new ValueSetMember("http://snomed.info/sct", null, code, null, null);
  }

  /** A graph whose single dependency is a concept map holding the given mappings. */
  @Nonnull
  private static ResolvedDependencyGraph conceptMapGraph(
      @Nonnull final ConceptMapping... mappings) {
    final ConceptMapContent content =
        ConceptMapContent.fromMappings(VIEW_URL, "2026", List.of(mappings), Integer.MAX_VALUE);
    return graph(Map.of("crit_a", VIEW_URL), new ResolvedConceptMap(VIEW_URL, content));
  }

  /** A SNOMED CT to ICD-10 mapping with the given codes and relationship, and no displays. */
  @Nonnull
  private static ConceptMapping mapping(
      @Nonnull final String sourceCode,
      @Nonnull final String targetCode,
      @Nullable final String relationship) {
    return new ConceptMapping(
        "http://snomed.info/sct",
        null,
        sourceCode,
        null,
        "http://hl7.org/fhir/sid/icd-10",
        "2019",
        targetCode,
        null,
        relationship);
  }

  /**
   * A graph whose single top-level dependency is a SQLView that itself depends on a second SQLView
   * carrying the given SQL, so the varying node is reachable only transitively.
   */
  @Nonnull
  private static ResolvedDependencyGraph nestedGraph(@Nonnull final String childSql) {
    return graph(
        Map.of("crit_a", VIEW_URL),
        new ResolvedSqlView(VIEW_URL, "SELECT * FROM crit_b", Map.of("crit_b", CHILD_URL)),
        new ResolvedSqlView(CHILD_URL, childSql, Map.of()));
  }

  /**
   * A graph with two independent top-level dependencies, registered in either order. The two
   * orderings describe the same closure.
   */
  @Nonnull
  private static ResolvedDependencyGraph twoLabelGraph(final boolean reversed) {
    final ResolvedSqlView a = new ResolvedSqlView(VIEW_URL, "SELECT 1 AS a", Map.of());
    final ResolvedSqlView b = new ResolvedSqlView(CHILD_URL, "SELECT 2 AS b", Map.of());
    final Map<String, String> labels = new LinkedHashMap<>();
    if (reversed) {
      labels.put("crit_b", CHILD_URL);
      labels.put("crit_a", VIEW_URL);
      return graph(labels, b, a);
    }
    labels.put("crit_a", VIEW_URL);
    labels.put("crit_b", CHILD_URL);
    return graph(labels, a, b);
  }

  /** Assembles a graph from its top-level label mapping and its nodes, in registration order. */
  @Nonnull
  private static ResolvedDependencyGraph graph(
      @Nonnull final Map<String, String> topLevelKeysByLabel,
      @Nonnull final ResolvedDependency... nodes) {
    final Map<String, ResolvedDependency> nodesByKey = new LinkedHashMap<>();
    for (final ResolvedDependency node : nodes) {
      nodesByKey.put(node.getCanonicalKey(), node);
    }
    return new ResolvedDependencyGraph(List.of(nodes), Map.copyOf(topLevelKeysByLabel), nodesByKey);
  }

  /** A single-subject request whose SQL is fixed, over the given dependency graph. */
  @Nonnull
  private static SqlExportRequest requestOver(@Nonnull final ResolvedDependencyGraph graph) {
    return requestOf(sqlSubject("subject", SUBJECT_SQL, Map.of(), graph));
  }

  /** A SQLQuery subject with the given name, SQL and bindings, over the given dependency graph. */
  @Nonnull
  private static SubjectInput sqlSubject(
      @Nonnull final String name,
      @Nonnull final String sql,
      @Nonnull final Map<String, Object> bindings,
      @Nonnull final ResolvedDependencyGraph graph) {
    final ParsedSqlQuery parsed =
        new ParsedSqlQuery(sql, List.of(), List.of(), SqlLibraryParser.SQL_QUERY_TYPE_CODE);
    final PreparedSqlQuery prepared =
        new PreparedSqlQuery(new SqlQueryRequest(parsed, null, bindings), graph);
    return SubjectInput.ofSql(SubjectKind.SQL_QUERY, name, prepared);
  }

  /** A request made up of the given subjects, with every other parameter fixed. */
  @Nonnull
  private static SqlExportRequest requestOf(@Nonnull final SubjectInput... subjects) {
    return new SqlExportRequest(
        "http://localhost/fhir/$sql-export",
        "http://localhost/fhir",
        List.of(subjects),
        null,
        SqlExportFormat.NDJSON,
        false,
        Set.of(),
        null);
  }
}
