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
import au.csiro.pathling.operations.sqlquery.ResolvedDependency;
import au.csiro.pathling.operations.sqlquery.ResolvedDependencyGraph;
import au.csiro.pathling.operations.sqlquery.ResolvedExternalTable;
import au.csiro.pathling.operations.sqlquery.ResolvedSqlView;
import au.csiro.pathling.operations.sqlquery.ResolvedViewDefinition;
import au.csiro.pathling.operations.sqlquery.SqlLibraryParser;
import au.csiro.pathling.operations.sqlquery.SqlQueryOutputFormat;
import au.csiro.pathling.operations.sqlquery.SqlQueryRequest;
import au.csiro.pathling.views.FhirView;
import jakarta.annotation.Nonnull;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
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
    final FhirView view =
        FhirView.ofResource("Patient").select(FhirView.columns(FhirView.column("v", path))).build();
    return graph(Map.of("crit_a", VIEW_URL), new ResolvedViewDefinition(VIEW_URL, view));
  }

  /** A graph whose single dependency is an external table at the given path and format. */
  @Nonnull
  private static ResolvedDependencyGraph externalTableGraph(
      @Nonnull final String path, @Nonnull final String format) {
    return graph(Map.of("crit_a", VIEW_URL), new ResolvedExternalTable(VIEW_URL, path, format));
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
    final ParsedSqlQuery parsed =
        new ParsedSqlQuery(SUBJECT_SQL, List.of(), List.of(), SqlLibraryParser.SQL_QUERY_TYPE_CODE);
    final PreparedSqlQuery prepared =
        new PreparedSqlQuery(
            new SqlQueryRequest(parsed, SqlQueryOutputFormat.NDJSON, false, null, Map.of()), graph);
    return new SqlExportRequest(
        "http://localhost/fhir/$sql-export",
        "http://localhost/fhir",
        List.of(SubjectInput.ofSql(SubjectKind.SQL_QUERY, "subject", prepared)),
        null,
        SqlExportFormat.NDJSON,
        false,
        Set.of(),
        null);
  }

  // -------------------------------------------------------------------------
  // Delimiter-forging resistance (aehrc/pathling#2768).
  // -------------------------------------------------------------------------

  // Every test in this section fails against the pre-fix rendering and passes with the encoding
  // applied: reverting just the main source must turn each of them red.
  @Test
  void sqlCannotForgeSubjectBoundaries() {
    // Regression test for issue #2768. Before the fix, subject descriptions were rendered as
    // "name:kind:sql:bindings:topLevelKeysByLabel" with unescaped delimiters, joined by ",".
    // Request A has two subjects; request B has a single subject whose SQL embeds the exact
    // renderer grammar (",", ":", "{", "}", "="), so both requests rendered to the same key and
    // B's kick-off was deduplicated onto A's job. (In the real exploit the payload sits after a
    // "--" line comment so the SQL stays valid; the string-level ambiguity is what this tests.)
    final SqlExportRequest twoSubjects =
        requestWith(sqlSubject("s1", "SELECT 1"), sqlSubject("s2", "SELECT 2"));
    final SqlExportRequest forgedSingleSubject =
        requestWith(sqlSubject("s1", "SELECT 1:{}:{},s2:SQL_QUERY:SELECT 2"));

    assertThat(support.computeCacheKeyComponent(twoSubjects))
        .isNotEqualTo(support.computeCacheKeyComponent(forgedSingleSubject));
  }

  @Test
  void topLevelKeysCannotForgeMapBoundaries() {
    // The same delimiter-forging class applied to the label->key map: a map renders its entries
    // joined by ", " without escaping, so one entry whose value contains ", " renders exactly
    // like two entries. Without encoding these two label mappings share a key.
    final SqlExportRequest oneEntry =
        requestWith(
            sqlSubjectWithLabels(
                "s1",
                "SELECT * FROM lbl",
                Map.of(
                    "lbl",
                    "https://example.org/Library/v1, other=https://example.org/Library/v2")));
    final Map<String, String> twoEntries = new LinkedHashMap<>();
    twoEntries.put("lbl", "https://example.org/Library/v1");
    twoEntries.put("other", "https://example.org/Library/v2");
    final SqlExportRequest twoEntryRequest =
        requestWith(sqlSubjectWithLabels("s1", "SELECT * FROM lbl", twoEntries));

    assertThat(support.computeCacheKeyComponent(oneEntry))
        .isNotEqualTo(support.computeCacheKeyComponent(twoEntryRequest));
  }

  @Test
  void dependencyChildWiringCannotForgeMapBoundaries() {
    // describeContent renders the label->child-key mapping with the same unescaped "{k=v, ...}"
    // grammar: one entry whose value contains ", " renders exactly like two entries. These two
    // closures wire the SQLView's label to different children but rendered to the same key
    // before the fix.
    final String oneEntry =
        support.computeCacheKeyComponent(
            requestOver(
                graph(
                    Map.of("crit_a", VIEW_URL),
                    new ResolvedSqlView(
                        VIEW_URL,
                        "SELECT * FROM crit_b",
                        Map.of("crit_b", "https://x/one, crit_c=https://x/two")))));
    final Map<String, String> twoEntries = new LinkedHashMap<>();
    twoEntries.put("crit_b", "https://x/one");
    twoEntries.put("crit_c", "https://x/two");
    final String twoEntryLabels =
        support.computeCacheKeyComponent(
            requestOver(
                graph(
                    Map.of("crit_a", VIEW_URL),
                    new ResolvedSqlView(VIEW_URL, "SELECT * FROM crit_b", twoEntries))));

    assertThat(twoEntryLabels).isNotEqualTo(oneEntry);
  }

  @Test
  void clientTrackingIdIsLengthPrefixedInTheKey() {
    // The tracking id is a client-controlled free-text section: it is length-prefixed so that
    // whatever delimiters it contains, it cannot be mistaken for the neighbouring sections.
    final String key = support.computeCacheKeyComponent(requestWithTrackingId("abc|format=csv"));

    assertThat(key).contains("|clientTrackingId=14:abc|format=csv");
  }

  @Test
  void mapIterationOrderDoesNotChangeKey() {
    // Bindings and label maps render sorted, so identical kick-offs deduplicate regardless of the
    // map implementation's iteration order.
    final Map<String, Object> bindingsInsertion = new LinkedHashMap<>();
    bindingsInsertion.put("b", 2);
    bindingsInsertion.put("a", 1);
    final Map<String, Object> bindingsSorted = new TreeMap<>(bindingsInsertion);
    final SqlExportRequest first = requestWith(sqlSubject("s1", "SELECT 1", bindingsInsertion));
    final SqlExportRequest second = requestWith(sqlSubject("s1", "SELECT 1", bindingsSorted));

    assertThat(support.computeCacheKeyComponent(first))
        .isEqualTo(support.computeCacheKeyComponent(second));
  }

  @Nonnull
  private static SqlExportRequest requestWith(@Nonnull final SubjectInput... subjects) {
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

  @Nonnull
  private static SqlExportRequest requestWithTrackingId(@Nonnull final String trackingId) {
    return new SqlExportRequest(
        "http://localhost/fhir/$sql-export",
        "http://localhost/fhir",
        List.of(sqlSubject("s1", "SELECT 1 AS n")),
        trackingId,
        SqlExportFormat.NDJSON,
        false,
        Set.of(),
        null);
  }

  @Nonnull
  private static SubjectInput sqlSubject(@Nonnull final String name, @Nonnull final String sql) {
    return sqlSubject(name, sql, Map.<String, Object>of());
  }

  @Nonnull
  private static SubjectInput sqlSubject(
      @Nonnull final String name,
      @Nonnull final String sql,
      @Nonnull final Map<String, Object> bindings) {
    final ParsedSqlQuery parsed =
        new ParsedSqlQuery(sql, List.of(), List.of(), SqlLibraryParser.SQL_QUERY_TYPE_CODE);
    final SqlQueryRequest request =
        new SqlQueryRequest(parsed, SqlQueryOutputFormat.NDJSON, false, null, bindings);
    final ResolvedDependencyGraph graph =
        new ResolvedDependencyGraph(List.of(), Map.of(), Map.of());
    return SubjectInput.ofSql(SubjectKind.SQL_QUERY, name, new PreparedSqlQuery(request, graph));
  }

  @Nonnull
  private static SubjectInput sqlSubjectWithLabels(
      @Nonnull final String name,
      @Nonnull final String sql,
      @Nonnull final Map<String, String> topLevelKeysByLabel) {
    final ParsedSqlQuery parsed =
        new ParsedSqlQuery(sql, List.of(), List.of(), SqlLibraryParser.SQL_QUERY_TYPE_CODE);
    final SqlQueryRequest request =
        new SqlQueryRequest(parsed, SqlQueryOutputFormat.NDJSON, false, null, Map.of());
    final ResolvedDependencyGraph graph =
        new ResolvedDependencyGraph(List.of(), topLevelKeysByLabel, Map.of());
    return SubjectInput.ofSql(SubjectKind.SQL_QUERY, name, new PreparedSqlQuery(request, graph));
  }
}
