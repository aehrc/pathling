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
  // String literals and comments.
  // ---------------------------------------------------------------------------

  @Test
  void preservesSingleQuotedLiteralContainingALabel() {
    // The parse spans cover relation identifiers only, so a literal that happens to contain the
    // label text cannot be touched.
    assertThat(rewriteAge("SELECT * FROM age WHERE label = 'age'"))
        .isEqualTo("SELECT * FROM " + AGE_VIEW + " AS age WHERE label = 'age'");
  }

  @Test
  void preservesDoubleQuotedLiteralContainingALabel() {
    assertThat(rewriteAge("SELECT * FROM age WHERE note = \"age of patient\""))
        .isEqualTo("SELECT * FROM " + AGE_VIEW + " AS age WHERE note = \"age of patient\"");
  }

  @Test
  void preservesDoubledQuoteEscapedLiteralContainingALabel() {
    // Spark accepts a doubled single quote as an embedded apostrophe, and the label text inside
    // such a literal must survive along with the escape.
    assertThat(rewriteAge("SELECT * FROM age WHERE label = 'pat''s age'"))
        .isEqualTo("SELECT * FROM " + AGE_VIEW + " AS age WHERE label = 'pat''s age'");
  }

  @Test
  void preservesLineCommentMentioningALabel() {
    assertThat(rewriteAge("SELECT * FROM age -- age comment\nWHERE x = 1"))
        .isEqualTo("SELECT * FROM " + AGE_VIEW + " AS age -- age comment\nWHERE x = 1");
  }

  @Test
  void preservesBlockCommentMentioningALabel() {
    assertThat(rewriteAge("SELECT /* age in here */ * FROM age"))
        .isEqualTo("SELECT /* age in here */ * FROM " + AGE_VIEW + " AS age");
  }

  @Test
  void preservesLineCommentMentioningALabelAheadOfARewrittenRelation() {
    // A comment ahead of the reference pins that the rewrite offsets are counted over the
    // submitted text, newline and comment included, rather than over any normalised form of it.
    assertThat(rewriteAge("-- age comment\nSELECT * FROM age"))
        .isEqualTo("-- age comment\nSELECT * FROM " + AGE_VIEW + " AS age");
  }

  @Test
  void leavesBacktickQuotedIdentifierThatIsNotALabelAlone() {
    assertThat(rewriteAge("SELECT * FROM age WHERE `random col` = 'x'"))
        .isEqualTo("SELECT * FROM " + AGE_VIEW + " AS age WHERE `random col` = 'x'");
  }

  // ---------------------------------------------------------------------------
  // Named parameters.
  // ---------------------------------------------------------------------------

  @Test
  void preservesNamedParameterMarkersAndTheSpansAroundThem() {
    // A named parameter marker is not an identifier, so it survives; the marker before the relation
    // reference also proves the rewrite spans are offsets into the submitted text.
    assertThat(rewriteAge("SELECT :threshold AS t FROM age WHERE v > :threshold"))
        .isEqualTo("SELECT :threshold AS t FROM " + AGE_VIEW + " AS age WHERE v > :threshold");
  }

  // ---------------------------------------------------------------------------
  // Case sensitivity.
  // ---------------------------------------------------------------------------

  @Test
  void doesNotRewriteARelationDifferingFromTheLabelOnlyByCase() {
    // Label matching is case-sensitive, as it is in the validator, which rejects the reference as
    // an undeclared table before execution.
    final String sql = "SELECT * FROM AGE";
    assertThat(rewriteAge(sql)).isEqualTo(sql);
  }

  // ---------------------------------------------------------------------------
  // DESCRIBE statements.
  // ---------------------------------------------------------------------------

  @Test
  void rewritesDescribeTargetWithoutInjectingAnAlias() {
    // The grammar permits no alias on a DESCRIBE target, and there is nothing to qualify.
    assertThat(rewriteAge("DESCRIBE age")).isEqualTo("DESCRIBE " + AGE_VIEW);
  }

  @Test
  void rewritesDescribeTableTargetWithoutInjectingAnAlias() {
    assertThat(rewriteAge("DESCRIBE TABLE age")).isEqualTo("DESCRIBE TABLE " + AGE_VIEW);
  }

  @Test
  void rewritesOnlyTheRelationWithinDescribeQuery() {
    // The described query is a constructor argument of the command rather than a tree child, so
    // this also pins that the walk reaches it.
    assertThat(rewriteAge("DESCRIBE QUERY SELECT age FROM age"))
        .isEqualTo("DESCRIBE QUERY SELECT age FROM " + AGE_VIEW + " AS age");
  }

  // ---------------------------------------------------------------------------
  // Common table expression scoping.
  // ---------------------------------------------------------------------------

  @Test
  void leavesReferenceShadowedByACteOfTheSameName() {
    // The definition is in scope throughout the main query, so both occurrences of "age" name the
    // CTE rather than the labelled table, and the label is simply unused.
    final String sql = "WITH age AS (SELECT 99 AS v) SELECT v FROM age";
    assertThat(rewriteAge(sql)).isEqualTo(sql);
  }

  @Test
  void rewritesOnlyTheSelfReferenceWithinACteBodyNamedAfterALabel() {
    // A CTE without RECURSIVE cannot refer to itself, so the reference inside the body resolves
    // outward to the labelled table while the reference in the main query resolves to the CTE.
    assertThat(rewriteAge("WITH age AS (SELECT * FROM age) SELECT * FROM age"))
        .isEqualTo("WITH age AS (SELECT * FROM " + AGE_VIEW + " AS age) SELECT * FROM age");
  }

  @Test
  void rewritesLabelReferenceInsideADifferentlyNamedCteBody() {
    // Nothing named "age" is in scope, so the body's reference is a reference to the label.
    assertThat(rewriteAge("WITH c AS (SELECT * FROM age) SELECT * FROM c"))
        .isEqualTo("WITH c AS (SELECT * FROM " + AGE_VIEW + " AS age) SELECT * FROM c");
  }

  @Test
  void leavesReferenceToAnEarlierSiblingCteNamedAfterALabel() {
    // A definition sees the definitions declared before it, so "age" within the body of c names
    // the first CTE.
    final String sql = "WITH age AS (SELECT 1 AS v), c AS (SELECT * FROM age) SELECT * FROM c";
    assertThat(rewriteAge(sql)).isEqualTo(sql);
  }

  @Test
  void rewritesLabelReferenceInASubqueryOutsideACollidingCteScope() {
    // The CTE defined within the first derived table is out of scope in the second, so the
    // sibling's reference is still a reference to the labelled table.
    assertThat(
            rewriteAge(
                "SELECT * FROM (WITH age AS (SELECT 1 AS v) SELECT v FROM age) AS a"
                    + " JOIN (SELECT v FROM age) AS b ON a.v = b.v"))
        .isEqualTo(
            "SELECT * FROM (WITH age AS (SELECT 1 AS v) SELECT v FROM age) AS a"
                + " JOIN (SELECT v FROM "
                + AGE_VIEW
                + " AS age) AS b ON a.v = b.v");
  }

  @Test
  void leavesReferenceShadowedByACteDefinedWithinAnEnclosingCteBody() {
    // An inner WITH shadows the label for the remainder of the enclosing definition's body.
    final String sql = "WITH c AS (WITH age AS (SELECT 1 AS v) SELECT v FROM age) SELECT * FROM c";
    assertThat(rewriteAge(sql)).isEqualTo(sql);
  }

  @Test
  void leavesReferenceShadowedByACteFromWithinASubqueryOfTheMainQuery() {
    // A definition's scope covers the whole main query, nested subqueries included.
    final String sql = "WITH age AS (SELECT 1 AS v) SELECT * FROM (SELECT v FROM age) AS t";
    assertThat(rewriteAge(sql)).isEqualTo(sql);
  }

  @Test
  void rewritesReferenceMatchingACteNameOnlyByCase() {
    // CTE names are compared case-sensitively, exactly as labels are, so a CTE named AGE does not
    // shadow a reference that matches the label "age" exactly.
    assertThat(rewriteAge("WITH AGE AS (SELECT 99 AS v) SELECT v FROM age"))
        .isEqualTo("WITH AGE AS (SELECT 99 AS v) SELECT v FROM " + AGE_VIEW + " AS age");
  }

  @Test
  void leavesSelfReferenceWithinARecursiveCteNamedAfterALabel() {
    // RECURSIVE puts a definition's own name in scope for its body, so there the self-reference
    // is the recursion rather than the labelled table.
    final String sql =
        "WITH RECURSIVE age AS (SELECT 1 AS v UNION ALL SELECT v + 1 FROM age WHERE v < 3)"
            + " SELECT v FROM age";
    assertThat(rewriteAge(sql)).isEqualTo(sql);
  }

  // ---------------------------------------------------------------------------
  // Degenerate inputs.
  // ---------------------------------------------------------------------------

  @Test
  void returnsInputUnchangedWhenThereAreNoLabels() {
    final String sql = "SELECT age FROM age";
    // With nothing to substitute the input is returned as-is, without even being parsed.
    assertThat(rewrite(sql, Map.of())).isSameAs(sql);
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
