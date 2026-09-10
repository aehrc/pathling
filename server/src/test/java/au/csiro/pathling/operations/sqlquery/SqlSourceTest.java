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

import jakarta.annotation.Nonnull;
import org.junit.jupiter.api.Test;

/**
 * Tests for {@link SqlSource}, the token view {@link SqlLabelRewriter} uses to place the table
 * alias it injects after a substituted relation reference.
 *
 * <p>The rewriter's own tests cover the cases that arise from queries Spark will parse. These tests
 * exercise the token rules directly, which is the only way to reach the fail-safe refusal for a
 * temporal clause (a time-travel relation parses to an unresolved leaf, so the walk never reaches
 * the relation) and the requirement that an options clause be parenthesised.
 *
 * @author John Grimes
 */
class SqlSourceTest {

  @Test
  void insertsAliasDirectlyAfterAPlainRelationReference() {
    assertThat(insertionPoint("SELECT * FROM age", "age")).isEqualTo(17);
  }

  @Test
  void refusesAliasForTheTargetOfATableQueryPrimary() {
    assertThat(insertionPoint("TABLE age", "age")).isEqualTo(SqlSource.NO_ALIAS);
  }

  @Test
  void refusesAliasForATableQueryPrimaryTargetSeparatedByComments() {
    // Comments sit on the hidden channel, so the look-back reaches the keyword across them.
    assertThat(insertionPoint("TABLE /* c */ age", "age")).isEqualTo(SqlSource.NO_ALIAS);
    assertThat(insertionPoint("TABLE -- c\nage", "age")).isEqualTo(SqlSource.NO_ALIAS);
  }

  @Test
  void allowsAliasWhenTheWordTableAppearsOnlyWithinAComment() {
    // The look-back is over tokens rather than text, so the comment cannot supply the keyword.
    assertThat(insertionPoint("-- FROM A TABLE\nSELECT * FROM age", "age")).isEqualTo(33);
  }

  @Test
  void placesAliasAfterASampleClause() {
    final String sql = "SELECT * FROM age TABLESAMPLE (10 PERCENT)";
    assertThat(insertionPoint(sql, "age")).isEqualTo(sql.length());
  }

  @Test
  void placesAliasAfterAnOptionsClause() {
    final String sql = "SELECT * FROM age WITH (`k` = 'v')";
    assertThat(insertionPoint(sql, "age")).isEqualTo(sql.length());
  }

  @Test
  void placesAliasAfterBothAnOptionsAndASampleClause() {
    final String sql = "SELECT * FROM age WITH (`k` = 'v') TABLESAMPLE (2 ROWS)";
    assertThat(insertionPoint(sql, "age")).isEqualTo(sql.length());
  }

  @Test
  void placesAliasAfterASampleClauseContainingNestedParentheses() {
    // The group is closed by matching parentheses rather than by the first one seen.
    final String sql = "SELECT * FROM age TABLESAMPLE ((1 + 1) PERCENT)";
    assertThat(insertionPoint(sql, "age")).isEqualTo(sql.length());
  }

  @Test
  void treatsAnUnparenthesisedWithAsSomethingOtherThanAnOptionsClause() {
    // Only "WITH (" opens an options clause. A WITH that introduces a common table expression is
    // followed by a name, and must not be swallowed into the relation primary.
    assertThat(insertionPoint("SELECT * FROM age WITH c AS (SELECT 1)", "age")).isEqualTo(17);
  }

  @Test
  void refusesAliasWhenATemporalClauseFollowsTheReference() {
    // Time travel is rejected before the rewriter runs, and refusing an alias only costs the
    // resolution of a label-qualified column, whereas guessing where the clause ends could produce
    // text that does not parse.
    assertThat(insertionPoint("SELECT * FROM age VERSION AS OF 1", "age"))
        .isEqualTo(SqlSource.NO_ALIAS);
    assertThat(insertionPoint("SELECT * FROM age TIMESTAMP AS OF '2020-01-01'", "age"))
        .isEqualTo(SqlSource.NO_ALIAS);
    assertThat(insertionPoint("SELECT * FROM age FOR VERSION AS OF 1", "age"))
        .isEqualTo(SqlSource.NO_ALIAS);
    assertThat(insertionPoint("SELECT * FROM age SYSTEM_VERSION AS OF 1", "age"))
        .isEqualTo(SqlSource.NO_ALIAS);
    assertThat(insertionPoint("SELECT * FROM age SYSTEM_TIME AS OF '2020-01-01'", "age"))
        .isEqualTo(SqlSource.NO_ALIAS);
  }

  @Test
  void returnsTheTextBetweenTwoOffsets() {
    assertThat(new SqlSource("SELECT * FROM age").substring(14, 17)).isEqualTo("age");
  }

  /** Returns the alias insertion point for the first occurrence of the given identifier. */
  private int insertionPoint(@Nonnull final String sql, @Nonnull final String identifier) {
    final int start = sql.indexOf(identifier);
    return new SqlSource(sql).aliasInsertionPoint(start, start + identifier.length() - 1);
  }
}
