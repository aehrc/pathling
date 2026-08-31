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

import au.csiro.pathling.test.SpringBootUnitTest;
import jakarta.annotation.Nonnull;
import java.util.LinkedHashMap;
import java.util.Map;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.parser.ParserInterface;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;

/**
 * Tests for {@link SqlLabelRewriter}, the parse-guided substitution of dependency table labels with
 * the request-scoped temporary view names their dependencies are materialised under.
 *
 * <p>The central contract is that only <em>table references</em> are substituted. A column that
 * happens to share its name with a label occupies a different namespace and must survive verbatim,
 * which is the defect reported in <a href="https://github.com/aehrc/pathling/issues/2730">issue
 * 2730</a>. Where a relation reference carries no alias, the rewriter injects {@code AS <label>} so
 * that a column qualified by the label (for example {@code age.age FROM age}) keeps resolving.
 *
 * @author John Grimes
 */
@SpringBootUnitTest
class SqlLabelRewriterTest {

  /** The temporary view name the {@code age} label is taken to be materialised under. */
  private static final String AGE_VIEW = "sqlquery_req1_age";

  /** The temporary view name the {@code patients} label is taken to be materialised under. */
  private static final String PATIENTS_VIEW = "sqlquery_req1_patients";

  @Autowired private SparkSession spark;

  private ParserInterface parser;

  @BeforeEach
  void setUp() {
    parser = spark.sessionState().sqlParser();
  }

  // ---------------------------------------------------------------------------
  // Column names colliding with a label (issue 2730).
  // ---------------------------------------------------------------------------

  @Test
  void rewritesRelationButNotAliasQualifiedColumnOfTheSameName() {
    // Issue variant A. The relation reference is substituted; the column reference t.age, which
    // merely shares the label's name, is not.
    assertThat(rewriteAge("SELECT t.age FROM age AS t"))
        .isEqualTo("SELECT t.age FROM " + AGE_VIEW + " AS t");
  }

  @Test
  void rewritesRelationButNotUnqualifiedColumnOfTheSameName() {
    // Issue variant B. The bare "age" in the projection is a column reference and must survive; the
    // relation gains an injected alias so the label still names a table.
    assertThat(rewriteAge("SELECT age FROM age"))
        .isEqualTo("SELECT age FROM " + AGE_VIEW + " AS age");
  }

  @Test
  void injectsAliasSoLabelQualifiedColumnReferencesKeepResolving() {
    // The qualifier "age" names the labelled table rather than an alias, so the substituted
    // relation must be re-exposed under the label's own name.
    assertThat(rewriteAge("SELECT age.age FROM age"))
        .isEqualTo("SELECT age.age FROM " + AGE_VIEW + " AS age");
  }

  @Test
  void leavesUnrelatedColumnNamesAlone() {
    // Issue variant C, the control: a column whose name merely starts with the label is untouched.
    assertThat(rewriteAge("SELECT t.age_years FROM age AS t"))
        .isEqualTo("SELECT t.age_years FROM " + AGE_VIEW + " AS t");
  }

  // ---------------------------------------------------------------------------
  // Alias handling.
  // ---------------------------------------------------------------------------

  @Test
  void doesNotInjectAliasWhenRelationIsAliasedWithAsKeyword() {
    assertThat(rewriteAge("SELECT * FROM age AS t"))
        .isEqualTo("SELECT * FROM " + AGE_VIEW + " AS t");
  }

  @Test
  void doesNotInjectAliasWhenRelationIsAliasedWithoutAsKeyword() {
    assertThat(rewriteAge("SELECT * FROM age t")).isEqualTo("SELECT * FROM " + AGE_VIEW + " t");
  }

  @Test
  void doesNotInjectAliasWhenRelationCarriesColumnAliases() {
    // AS t(a, b) parses to an UnresolvedSubqueryColumnAliases wrapped in a SubqueryAlias; the
    // relation is aliased either way.
    assertThat(rewriteAge("SELECT * FROM age AS t(a, b)"))
        .isEqualTo("SELECT * FROM " + AGE_VIEW + " AS t(a, b)");
  }

  @Test
  void leavesTableAliasNamedAfterALabelAlone() {
    // "age" here is an alias for another relation, not a table reference.
    assertThat(rewriteAge("SELECT * FROM pt AS age")).isEqualTo("SELECT * FROM pt AS age");
  }

  @Test
  void leavesColumnAliasNamedAfterALabelAlone() {
    assertThat(rewriteAge("SELECT 1 AS age FROM pt")).isEqualTo("SELECT 1 AS age FROM pt");
  }

  @Test
  void leavesFunctionNamedAfterALabelAlone() {
    assertThat(rewriteAge("SELECT age(x) FROM pt")).isEqualTo("SELECT age(x) FROM pt");
  }

  // ---------------------------------------------------------------------------
  // Identifier shapes and word boundaries.
  // ---------------------------------------------------------------------------

  @Test
  void rewritesBacktickQuotedRelation() {
    // The parse origin covers the backticks, so the whole delimited identifier is replaced.
    assertThat(rewriteAge("SELECT * FROM `age`"))
        .isEqualTo("SELECT * FROM " + AGE_VIEW + " AS age");
  }

  @Test
  void leavesRelationWhoseNameMerelyStartsWithALabelAlone() {
    assertThat(
            rewrite(
                "SELECT * FROM patients JOIN patients_archive ON patients.id ="
                    + " patients_archive.id",
                Map.of("patients", PATIENTS_VIEW)))
        .isEqualTo(
            "SELECT * FROM "
                + PATIENTS_VIEW
                + " AS patients JOIN patients_archive ON patients.id = patients_archive.id");
  }

  @Test
  void rewritesEveryReferenceToTheSameLabel() {
    assertThat(rewriteAge("SELECT * FROM age AS a JOIN age AS b ON a.id = b.id"))
        .isEqualTo("SELECT * FROM " + AGE_VIEW + " AS a JOIN " + AGE_VIEW + " AS b ON a.id = b.id");
  }

  @Test
  void rewritesDistinctLabelsIndependently() {
    final Map<String, String> labels = new LinkedHashMap<>();
    labels.put("age", AGE_VIEW);
    labels.put("patients", PATIENTS_VIEW);
    assertThat(rewrite("SELECT * FROM age JOIN patients ON age.id = patients.id", labels))
        .isEqualTo(
            "SELECT * FROM "
                + AGE_VIEW
                + " AS age JOIN "
                + PATIENTS_VIEW
                + " AS patients ON age.id = patients.id");
  }

  // ---------------------------------------------------------------------------
  // Relation-primary wrappers.
  // ---------------------------------------------------------------------------

  @Test
  void injectsAliasAfterSampleClauseWhenRelationIsNotAliased() {
    // The grammar orders the clauses as identifier, sample, alias, so the injected alias must
    // follow the TABLESAMPLE clause rather than the identifier.
    assertThat(rewriteAge("SELECT * FROM age TABLESAMPLE (10 PERCENT)"))
        .isEqualTo("SELECT * FROM " + AGE_VIEW + " TABLESAMPLE (10 PERCENT) AS age");
  }

  @Test
  void doesNotInjectAliasAfterSampleClauseWhenRelationIsAliased() {
    assertThat(rewriteAge("SELECT * FROM age TABLESAMPLE (10 PERCENT) AS t"))
        .isEqualTo("SELECT * FROM " + AGE_VIEW + " TABLESAMPLE (10 PERCENT) AS t");
  }

  // ---------------------------------------------------------------------------
  // Nested plans.
  // ---------------------------------------------------------------------------

  @Test
  void rewritesRelationInsideASubqueryExpression() {
    assertThat(rewriteAge("SELECT * FROM pt WHERE EXISTS (SELECT 1 FROM age)"))
        .isEqualTo("SELECT * FROM pt WHERE EXISTS (SELECT 1 FROM " + AGE_VIEW + " AS age)");
  }

  @Test
  void rewritesRelationInsideADerivedTable() {
    assertThat(rewriteAge("SELECT * FROM (SELECT age FROM age) AS t"))
        .isEqualTo("SELECT * FROM (SELECT age FROM " + AGE_VIEW + " AS age) AS t");
  }

  // ---------------------------------------------------------------------------
  // Degenerate inputs.
  // ---------------------------------------------------------------------------

  @Test
  void returnsInputUnchangedWhenThereAreNoLabels() {
    final String sql = "SELECT age FROM age";
    assertThat(rewrite(sql, Map.of())).isEqualTo(sql);
  }

  /** Rewrites the given SQL against the single {@code age} label. */
  @Nonnull
  private String rewriteAge(@Nonnull final String sql) {
    return rewrite(sql, Map.of("age", AGE_VIEW));
  }

  /** Rewrites the given SQL against the given label mapping, using the session's SQL parser. */
  @Nonnull
  private String rewrite(
      @Nonnull final String sql, @Nonnull final Map<String, String> labelToViewName) {
    return SqlLabelRewriter.rewrite(parser, sql, labelToViewName);
  }
}
