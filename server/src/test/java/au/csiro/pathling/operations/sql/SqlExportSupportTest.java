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

import au.csiro.pathling.operations.sqlquery.ParsedSqlQuery;
import au.csiro.pathling.operations.sqlquery.PreparedSqlQuery;
import au.csiro.pathling.operations.sqlquery.ResolvedDependencyGraph;
import au.csiro.pathling.operations.sqlquery.SqlQueryOutputFormat;
import au.csiro.pathling.operations.sqlquery.SqlQueryRequest;
import jakarta.annotation.Nonnull;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for {@link SqlExportSupport#computeCacheKeyComponent}, covering the cache-key
 * contract: identical kick-offs deduplicate onto the same job, differing kick-offs do not, and no
 * client-controlled value can forge the structural delimiters to make two different requests share
 * a key (issue #2768).
 */
class SqlExportSupportTest {

  private SqlExportSupport support;

  @BeforeEach
  void setUp() {
    // computeCacheKeyComponent is pure with respect to the request; the collaborators are unused.
    support = new SqlExportSupport(null, null, null, null, null, null);
  }

  @Test
  void identicalRequestsShareKey() {
    final SqlExportRequest first = requestWith(sqlSubject("s1", "SELECT 1"));
    final SqlExportRequest second = requestWith(sqlSubject("s1", "SELECT 1"));

    assertThat(support.computeCacheKeyComponent(first))
        .isEqualTo(support.computeCacheKeyComponent(second));
  }

  @Test
  void differentSqlProducesDifferentKey() {
    final SqlExportRequest first = requestWith(sqlSubject("s1", "SELECT 1"));
    final SqlExportRequest second = requestWith(sqlSubject("s1", "SELECT 2"));

    assertThat(support.computeCacheKeyComponent(first))
        .isNotEqualTo(support.computeCacheKeyComponent(second));
  }

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
    // The same delimiter-forging class applied to the label->key map: without encoding, a label
    // containing "}," could terminate one entry and open another.
    final SqlExportRequest first =
        requestWith(
            sqlSubject("s1", "SELECT * FROM lbl", Map.of("lbl", "https://example.org/Library/v1")));
    final SqlExportRequest second =
        requestWith(
            sqlSubject(
                "s1",
                "SELECT * FROM lbl",
                Map.of("lbl},x={lbl", "https://example.org/Library/v1")));

    assertThat(support.computeCacheKeyComponent(first))
        .isNotEqualTo(support.computeCacheKeyComponent(second));
  }

  @Test
  void clientTrackingIdCannotForgeSectionBoundaries() {
    // A tracking id containing the section delimiter must not be able to mimic another section.
    final SqlExportRequest first = requestWithTrackingId("abc|format=csv");
    final SqlExportRequest second = requestWithTrackingId("abc");

    assertThat(support.computeCacheKeyComponent(first))
        .isNotEqualTo(support.computeCacheKeyComponent(second));
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
  private SqlExportRequest requestWith(@Nonnull final SubjectInput... subjects) {
    return new SqlExportRequest(
        "http://example.org/fhir/$sql-export",
        "http://example.org/fhir",
        List.of(subjects),
        null,
        SqlExportFormat.NDJSON,
        false,
        Set.of(),
        null);
  }

  @Nonnull
  private SqlExportRequest requestWithTrackingId(@Nonnull final String trackingId) {
    return new SqlExportRequest(
        "http://example.org/fhir/$sql-export",
        "http://example.org/fhir",
        List.of(sqlSubject("s1", "SELECT 1")),
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
        new ParsedSqlQuery(sql, List.of(), List.of(), "sql-query");
    final SqlQueryRequest request =
        new SqlQueryRequest(parsed, SqlQueryOutputFormat.NDJSON, false, null, bindings);
    final ResolvedDependencyGraph graph =
        new ResolvedDependencyGraph(List.of(), Map.of(), Map.of());
    return SubjectInput.ofSql(SubjectKind.SQL_QUERY, name, new PreparedSqlQuery(request, graph));
  }

  @Nonnull
  private static SubjectInput sqlSubject(
      @Nonnull final String name,
      @Nonnull final String sql,
      @Nonnull final Map<String, String> topLevelKeysByLabel) {
    final ParsedSqlQuery parsed =
        new ParsedSqlQuery(sql, List.of(), List.of(), "sql-query");
    final SqlQueryRequest request =
        new SqlQueryRequest(parsed, SqlQueryOutputFormat.NDJSON, false, null, Map.of());
    final ResolvedDependencyGraph graph =
        new ResolvedDependencyGraph(List.of(), topLevelKeysByLabel, Map.of());
    return SubjectInput.ofSql(SubjectKind.SQL_QUERY, name, new PreparedSqlQuery(request, graph));
  }
}
