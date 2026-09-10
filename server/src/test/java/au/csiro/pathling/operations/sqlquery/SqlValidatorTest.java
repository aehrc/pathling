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

import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import au.csiro.pathling.test.SpringBootUnitTest;
import ca.uhn.fhir.rest.server.exceptions.InvalidRequestException;
import jakarta.annotation.Nonnull;
import java.util.Set;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.parser.ParseException;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.catalyst.plans.logical.WithWindowDefinition;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Import;

/** Unit tests for {@link SqlValidator}. */
@Import(SqlValidator.class)
@SpringBootUnitTest
class SqlValidatorTest {

  private static final String VIEW_NAME = "sql_validator_test_view";

  @Autowired private SqlValidator sqlValidator;

  @Autowired private SparkSession sparkSession;

  @AfterEach
  void dropView() {
    sparkSession.catalog().dropTempView(VIEW_NAME);
  }

  /** Convenience wrapper threading a varargs label set into {@link SqlValidator#validate}. */
  private void validate(@Nonnull final String sql, @Nonnull final String... labels) {
    sqlValidator.validate(sql, Set.of(labels));
  }

  // -------------------------------------------------------------------------
  // Valid SQL queries — should not throw.
  // -------------------------------------------------------------------------

  @Test
  void acceptsSimpleSelect() {
    assertThatCode(() -> validate("SELECT 1")).doesNotThrowAnyException();
  }

  @Test
  void acceptsSelectWithAlias() {
    assertThatCode(() -> validate("SELECT 1 AS value")).doesNotThrowAnyException();
  }

  @Test
  void acceptsSelectFromTable() {
    assertThatCode(() -> validate("SELECT * FROM my_view", "my_view")).doesNotThrowAnyException();
  }

  @Test
  void acceptsSelectWithWhere() {
    assertThatCode(() -> validate("SELECT a, b FROM t WHERE a > 10", "t"))
        .doesNotThrowAnyException();
  }

  @Test
  void acceptsSelectWithJoin() {
    assertThatCode(() -> validate("SELECT a.id, b.name FROM a JOIN b ON a.id = b.a_id", "a", "b"))
        .doesNotThrowAnyException();
  }

  @Test
  void acceptsSelectWithAggregation() {
    assertThatCode(() -> validate("SELECT count(*), sum(x) FROM t GROUP BY y", "t"))
        .doesNotThrowAnyException();
  }

  @Test
  void acceptsSelectWithSubquery() {
    assertThatCode(() -> validate("SELECT * FROM (SELECT 1 AS x) sub")).doesNotThrowAnyException();
  }

  @Test
  void acceptsSelectWithCte() {
    // The CTE name 'cte' is defined in the query itself, so no label is required.
    assertThatCode(() -> validate("WITH cte AS (SELECT 1 AS x) SELECT * FROM cte"))
        .doesNotThrowAnyException();
  }

  @Test
  void acceptsCteReferencingDeclaredLabel() {
    // A CTE can wrap a declared label; the declared label is required, the CTE name is not.
    assertThatCode(
            () -> validate("WITH foo AS (SELECT * FROM patients) SELECT * FROM foo", "patients"))
        .doesNotThrowAnyException();
  }

  @Test
  void acceptsNestedCtes() {
    // Nested CTEs - both 'a' and 'b' are CTE-local names.
    assertThatCode(
            () -> validate("WITH a AS (SELECT 1 AS x), b AS (SELECT * FROM a) SELECT * FROM b"))
        .doesNotThrowAnyException();
  }

  @Test
  void acceptsSelectWithOrderByAndLimit() {
    assertThatCode(() -> validate("SELECT * FROM t ORDER BY id LIMIT 10", "t"))
        .doesNotThrowAnyException();
  }

  @Test
  void acceptsSelectWithCaseWhen() {
    assertThatCode(
            () ->
                validate("SELECT CASE WHEN x > 0 THEN 'positive' ELSE 'negative' END FROM t", "t"))
        .doesNotThrowAnyException();
  }

  @Test
  void acceptsSelectWithStringFunctions() {
    assertThatCode(() -> validate("SELECT upper(name), lower(name), length(name) FROM t", "t"))
        .doesNotThrowAnyException();
  }

  @Test
  void acceptsSelectWithMathFunctions() {
    assertThatCode(() -> validate("SELECT abs(-1), sqrt(4), round(3.14, 1)"))
        .doesNotThrowAnyException();
  }

  @Test
  void acceptsSelectWithDateFunctions() {
    assertThatCode(() -> validate("SELECT current_date(), current_timestamp()"))
        .doesNotThrowAnyException();
  }

  @Test
  void acceptsSelectWithUnion() {
    assertThatCode(() -> validate("SELECT 1 AS x UNION ALL SELECT 2 AS x"))
        .doesNotThrowAnyException();
  }

  @Test
  void acceptsSelectWithWindowFunction() {
    assertThatCode(() -> validate("SELECT id, row_number() OVER (ORDER BY id) AS rn FROM t", "t"))
        .doesNotThrowAnyException();
  }

  // -------------------------------------------------------------------------
  // Explicit window frames (US1, #2650). An inline frame with the CURRENT ROW,
  // UNBOUNDED PRECEDING, and UNBOUNDED FOLLOWING boundary markers must be
  // accepted. Before the fix, the CURRENT ROW boundary was rejected as
  // "CurrentRow$" (the Scala case-object runtime name). Value-based bounds
  // (N PRECEDING / N FOLLOWING) were already permitted and are covered here as
  // a non-regression guard.
  // -------------------------------------------------------------------------

  @Test
  void acceptsWindowFrameWithCurrentRow() {
    // The reporter's "rolling worst value over the prior 24 hours per patient"
    // form, reduced to the window construct over a declared label.
    assertThatCode(
            () ->
                validate(
                    "SELECT patient_id, min(value) OVER ("
                        + "  PARTITION BY patient_id"
                        + "  ORDER BY obs_time"
                        + "  RANGE BETWEEN INTERVAL '24' HOUR PRECEDING AND CURRENT ROW"
                        + ") AS worst_value FROM t",
                    "t"))
        .doesNotThrowAnyException();
  }

  @Test
  void acceptsWindowFrameWithUnboundedPrecedingToCurrentRow() {
    assertThatCode(
            () ->
                validate(
                    "SELECT sum(x) OVER (ORDER BY id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT"
                        + " ROW) FROM t",
                    "t"))
        .doesNotThrowAnyException();
  }

  @Test
  void acceptsWindowFrameWithUnboundedBounds() {
    assertThatCode(
            () ->
                validate(
                    "SELECT sum(x) OVER (ORDER BY id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED"
                        + " FOLLOWING) FROM t",
                    "t"))
        .doesNotThrowAnyException();
  }

  @Test
  void acceptsValueBoundedWindowFrame() {
    // Value bounds parse to ordinary arithmetic expressions (UnaryMinus over a
    // Literal), which are already permitted, so this needs no new allow-list
    // entry; it guards against a regression in that reasoning.
    assertThatCode(
            () ->
                validate(
                    "SELECT avg(x) OVER (ORDER BY id ROWS BETWEEN 3 PRECEDING AND 1 FOLLOWING)"
                        + " FROM t",
                    "t"))
        .doesNotThrowAnyException();
  }

  // -------------------------------------------------------------------------
  // Named window definitions (US2, #2649). A SELECT that references a named
  // window via OVER w with a matching WINDOW w AS (...) clause must be
  // accepted. Before the fix, the named-window reference was rejected as
  // "UnresolvedWindowExpression". The frameless form isolates that gate; the
  // frame-bearing form additionally relies on US1's boundary additions.
  // -------------------------------------------------------------------------

  @Test
  void acceptsFramelessNamedWindow() {
    // No explicit frame, so this fails only on UnresolvedWindowExpression and
    // remains red until US2, isolating that gate from US1's frame boundaries.
    assertThatCode(
            () ->
                validate(
                    "SELECT patient_id, min(value) OVER w AS worst_value FROM t"
                        + " WINDOW w AS (PARTITION BY patient_id ORDER BY obs_time)",
                    "t"))
        .doesNotThrowAnyException();
  }

  @Test
  void rejectsReflectionInsideNamedWindowDefinition() {
    // A named-window definition body is a plan sub-structure the strict walk must
    // descend into: a reflection-style function hidden in its PARTITION BY must
    // still be rejected, exactly as it is in the inline form.
    assertThatThrownBy(
            () ->
                validate(
                    "SELECT sum(x) OVER w FROM t"
                        + " WINDOW w AS (PARTITION BY java_method('java.lang.Math', 'random')"
                        + " ORDER BY id)",
                    "t"))
        .isInstanceOf(InvalidRequestException.class)
        .hasMessageContaining("disallowed function");
  }

  @Test
  void rejectsUdfInsideNamedWindowDefinitionOrderBy() {
    // A non-built-in (terminology UDF) function in the ORDER BY of a named window
    // must also be rejected.
    assertThatThrownBy(
            () ->
                validate(
                    "SELECT sum(x) OVER w FROM t"
                        + " WINDOW w AS (PARTITION BY id"
                        + " ORDER BY member_of(coding, 'http://snomed.info/sct?fhir_vs'))",
                    "t"))
        .isInstanceOf(InvalidRequestException.class)
        .hasMessageContaining("non-built-in function");
  }

  @Test
  void rejectsNonLiteralFunctionInNamedWindowFrameBound() {
    // A frame bound cannot smuggle a function: Spark's grammar requires the bound
    // to be a literal, so a function-valued bound is rejected at parse time. This
    // guards the frame-bound position of a named window's definition body.
    assertThatThrownBy(
            () ->
                validate(
                    "SELECT sum(x) OVER w FROM t"
                        + " WINDOW w AS (ORDER BY id"
                        + " ROWS BETWEEN CAST(java_method('java.lang.Math', 'random') AS INT)"
                        + " PRECEDING AND CURRENT ROW)",
                    "t"))
        .isInstanceOf(InvalidRequestException.class)
        .hasMessageContaining("Invalid SQL syntax");
  }

  @Test
  void acceptsNamedWindowWithRangeFrame() {
    // The reporter's ten-column case reduced to two aggregates that share one
    // named window carrying a RANGE frame with a CURRENT ROW boundary.
    assertThatCode(
            () ->
                validate(
                    "SELECT patient_id,"
                        + "  min(value) OVER w AS worst_min,"
                        + "  max(value) OVER w AS worst_max"
                        + " FROM t"
                        + " WINDOW w AS ("
                        + "  PARTITION BY patient_id"
                        + "  ORDER BY obs_time"
                        + "  RANGE BETWEEN INTERVAL '24' HOUR PRECEDING AND CURRENT ROW"
                        + ")",
                    "t"))
        .doesNotThrowAnyException();
  }

  // -------------------------------------------------------------------------
  // Analysed-plan defence in depth for named windows (US2, #2649). Ordinary SQL
  // substitutes named windows into Window nodes before analysis, so a
  // WithWindowDefinition normally never reaches the analysed-plan walk, and its
  // windowDefinitions map is not reachable through plan.expressions(). The walk
  // nonetheless descends into any WithWindowDefinition that survives, so that a
  // definition body cannot become a blind spot. These tests force that branch
  // by wrapping a real named-window definition map around an already-validated
  // child.
  // -------------------------------------------------------------------------

  @Test
  void walksCleanBodyOfSurvivingWithWindowDefinition() throws ParseException {
    // A clean named-window body passes: the walk reaches into the definition and
    // finds nothing disallowed.
    final WithWindowDefinition node =
        withWindowNode("SELECT 1 FROM t WINDOW w AS (PARTITION BY id ORDER BY ts)");
    assertThatCode(() -> sqlValidator.validateAnalyzed(node, Set.of())).doesNotThrowAnyException();
  }

  @Test
  void rejectsReflectionInBodyOfSurvivingWithWindowDefinition() throws ParseException {
    // A reflection-style function hidden in a surviving named-window definition
    // must still be rejected by the analysed-plan walk, mirroring the strict-walk
    // carve-out.
    final WithWindowDefinition node =
        withWindowNode(
            "SELECT 1 FROM t WINDOW w AS ("
                + "PARTITION BY java_method('java.lang.Math', 'random') ORDER BY id)");
    assertThatThrownBy(() -> sqlValidator.validateAnalyzed(node, Set.of()))
        .isInstanceOf(InvalidRequestException.class)
        .hasMessageContaining("disallowed function");
  }

  /**
   * Builds a {@link WithWindowDefinition} that carries a real named-window definition map (parsed
   * from the given WINDOW-clause SQL) but a trivial, already-validated child. This forces the
   * analysed-plan walk through the WithWindowDefinition branch that ordinary SQL elides during
   * analysis, where named windows are substituted into Window nodes before the walk runs.
   */
  @Nonnull
  private WithWindowDefinition withWindowNode(@Nonnull final String windowSql)
      throws ParseException {
    final LogicalPlan source = sparkSession.sessionState().sqlParser().parsePlan(windowSql);
    final WithWindowDefinition parsed = (WithWindowDefinition) source;
    final LogicalPlan cleanChild = sparkSession.sql("SELECT 1 AS id").queryExecution().analyzed();
    return new WithWindowDefinition(parsed.windowDefinitions(), cleanChild, false);
  }

  @Test
  void acceptsSelectWithDistinct() {
    assertThatCode(() -> validate("SELECT DISTINCT name FROM t", "t")).doesNotThrowAnyException();
  }

  @Test
  void acceptsSelectWithInClause() {
    assertThatCode(() -> validate("SELECT * FROM t WHERE id IN (1, 2, 3)", "t"))
        .doesNotThrowAnyException();
  }

  @Test
  void acceptsSelectWithCast() {
    assertThatCode(() -> validate("SELECT CAST(x AS STRING) FROM t", "t"))
        .doesNotThrowAnyException();
  }

  @Test
  void acceptsSelectWithCoalesce() {
    assertThatCode(() -> validate("SELECT coalesce(a, b, 'default') FROM t", "t"))
        .doesNotThrowAnyException();
  }

  // -------------------------------------------------------------------------
  // String concatenation. The || operator parses directly to a Concat
  // expression (AstBuilder maps CONCAT_PIPE to Concat), so it never appears
  // as an UnresolvedFunction and must be present in the expression allow-list
  // in its own right. The equivalent concat() function call resolves to an
  // UnresolvedFunction named 'concat', which is already permitted because it
  // is a built-in, so the two spellings must behave consistently.
  // -------------------------------------------------------------------------

  @Test
  void acceptsStringConcatenationOperator() {
    assertThatCode(() -> validate("SELECT a || b FROM t", "t")).doesNotThrowAnyException();
  }

  @Test
  void acceptsConcatFunction() {
    assertThatCode(() -> validate("SELECT concat(a, b) FROM t", "t")).doesNotThrowAnyException();
  }

  @Test
  void acceptsDrugStrengthStyleConcatenation() {
    // Mirrors the real-world "Drug strength" view that triggered this fix: a
    // human-readable strength label assembled from several columns using ||.
    assertThatCode(
            () ->
                validate(
                    "SELECT numerator_value || ' ' || numerator_unit || '/' || denominator_value"
                        + " || ' ' || denominator_unit AS strength FROM t",
                    "t"))
        .doesNotThrowAnyException();
  }

  // -------------------------------------------------------------------------
  // Invalid SQL — should reject DDL and DML.
  // -------------------------------------------------------------------------

  @Test
  void rejectsDropTable() {
    assertThatThrownBy(() -> validate("DROP TABLE my_table"))
        .isInstanceOf(InvalidRequestException.class)
        .hasMessageContaining("disallowed");
  }

  @Test
  void rejectsCreateTable() {
    assertThatThrownBy(() -> validate("CREATE TABLE my_table (id INT)"))
        .isInstanceOf(InvalidRequestException.class)
        .hasMessageContaining("disallowed");
  }

  @Test
  void rejectsInsertInto() {
    assertThatThrownBy(() -> validate("INSERT INTO my_table VALUES (1, 'a')"))
        .isInstanceOf(InvalidRequestException.class)
        .hasMessageContaining("disallowed");
  }

  @Test
  void rejectsDeleteFrom() {
    assertThatThrownBy(() -> validate("DELETE FROM my_table WHERE id = 1"))
        .isInstanceOf(InvalidRequestException.class)
        .hasMessageContaining("disallowed");
  }

  @Test
  void rejectsUpdateStatement() {
    assertThatThrownBy(() -> validate("UPDATE my_table SET name = 'x' WHERE id = 1"))
        .isInstanceOf(InvalidRequestException.class)
        .hasMessageContaining("disallowed");
  }

  @Test
  void rejectsCreateView() {
    assertThatThrownBy(() -> validate("CREATE VIEW my_view AS SELECT 1"))
        .isInstanceOf(InvalidRequestException.class)
        .hasMessageContaining("disallowed");
  }

  // -------------------------------------------------------------------------
  // Invalid SQL — should reject dangerous functions.
  // -------------------------------------------------------------------------

  @Test
  void rejectsReflectFunction() {
    assertThatThrownBy(
            () -> validate("SELECT reflect('java.lang.Runtime', 'getRuntime') FROM t", "t"))
        .isInstanceOf(InvalidRequestException.class)
        .hasMessageContaining("disallowed function");
  }

  @Test
  void rejectsJavaMethodFunction() {
    assertThatThrownBy(() -> validate("SELECT java_method('java.lang.Math', 'random') FROM t", "t"))
        .isInstanceOf(InvalidRequestException.class)
        .hasMessageContaining("disallowed function");
  }

  // -------------------------------------------------------------------------
  // Invalid SQL — Pathling-registered UDFs are not allowed in user SQL.
  // The analytic surface stays portable; terminology and FHIRPath helpers
  // belong in ViewDefinitions.
  // -------------------------------------------------------------------------

  @Test
  void rejectsTerminologyUdfMemberOf() {
    assertThatThrownBy(
            () ->
                validate("SELECT member_of(coding, 'http://snomed.info/sct?fhir_vs') FROM t", "t"))
        .isInstanceOf(InvalidRequestException.class)
        .hasMessageContaining("non-built-in function")
        .hasMessageContaining("member_of");
  }

  @Test
  void rejectsTerminologyUdfDisplay() {
    assertThatThrownBy(() -> validate("SELECT display(coding) FROM t", "t"))
        .isInstanceOf(InvalidRequestException.class)
        .hasMessageContaining("non-built-in function")
        .hasMessageContaining("display");
  }

  @Test
  void rejectsTerminologyUdfTranslateCoding() {
    assertThatThrownBy(
            () -> validate("SELECT translate_coding(coding, 'http://example.org/cm') FROM t", "t"))
        .isInstanceOf(InvalidRequestException.class)
        .hasMessageContaining("non-built-in function");
  }

  @Test
  void rejectsFhirpathUdfStringToQuantity() {
    assertThatThrownBy(() -> validate("SELECT string_to_quantity('5 mg') FROM t", "t"))
        .isInstanceOf(InvalidRequestException.class)
        .hasMessageContaining("non-built-in function");
  }

  // -------------------------------------------------------------------------
  // Arbitrary local file read protection — the datasource short-name syntax
  // (parquet/csv/text/binaryFile/...) parses as a 2-part UnresolvedRelation
  // and must always be rejected, even when its first part happens to match a
  // declared label.
  // -------------------------------------------------------------------------

  @Test
  void rejectsCsvShortNameFileRead() {
    assertThatThrownBy(() -> validate("SELECT * FROM csv.`/etc/passwd`"))
        .isInstanceOf(InvalidRequestException.class)
        .hasMessageContaining("undeclared table");
  }

  @Test
  void rejectsTextShortNameFileRead() {
    assertThatThrownBy(() -> validate("SELECT * FROM text.`/Users/gri306/.ssh/known_hosts`"))
        .isInstanceOf(InvalidRequestException.class)
        .hasMessageContaining("undeclared table");
  }

  @Test
  void rejectsBinaryFileShortNameRead() {
    assertThatThrownBy(
            () -> validate("SELECT base64(content) FROM binaryFile.`/Users/gri306/.ssh/personal`"))
        .isInstanceOf(InvalidRequestException.class)
        .hasMessageContaining("undeclared table");
  }

  @Test
  void rejectsParquetShortNameEvenWhenLabelMatchesDatasource() {
    // Even when 'Patient' is declared, parquet.`...` parses as parquet/`...`, so the relation
    // identifier is 'parquet'.'<path>', not 'Patient'. The rule still fires.
    assertThatThrownBy(
            () ->
                validate(
                    "SELECT id, gender FROM"
                        + " parquet.`/Users/gri306/Warehouse/default/Patient.parquet`",
                    "Patient"))
        .isInstanceOf(InvalidRequestException.class)
        .hasMessageContaining("undeclared table");
  }

  @Test
  void rejectsBinaryFileGlobShortName() {
    assertThatThrownBy(() -> validate("SELECT path FROM binaryFile.`/etc/*.conf`"))
        .isInstanceOf(InvalidRequestException.class)
        .hasMessageContaining("undeclared table");
  }

  @Test
  void rejectsTwoPartCatalogReference() {
    assertThatThrownBy(() -> validate("SELECT * FROM default.sometable"))
        .isInstanceOf(InvalidRequestException.class)
        .hasMessageContaining("undeclared table");
  }

  @Test
  void rejectsRelationNotInLabelSet() {
    assertThatThrownBy(() -> validate("SELECT * FROM not_declared", "patients"))
        .isInstanceOf(InvalidRequestException.class)
        .hasMessageContaining("undeclared table");
  }

  @Test
  void acceptsRelationInLabelSet() {
    assertThatCode(() -> validate("SELECT * FROM patients", "patients")).doesNotThrowAnyException();
  }

  // -------------------------------------------------------------------------
  // Relation references inside CTE definition bodies. A WITH node exposes its
  // CTE bodies as innerChildren rather than children, so the strict walk must
  // descend into them explicitly. Without this, an undeclared table referenced
  // inside a CTE body (for example an OMOP 'concept' vocabulary table) slips
  // past static validation and surfaces as an opaque 500 from Spark's analyser
  // instead of a clean rejection.
  // -------------------------------------------------------------------------

  @Test
  void rejectsUndeclaredTableInsideCteBody() {
    // The relation is named only inside the CTE definition, never at the top level.
    assertThatThrownBy(() -> validate("WITH cr AS (SELECT * FROM concept) SELECT * FROM cr"))
        .isInstanceOf(InvalidRequestException.class)
        .hasMessageContaining("undeclared table")
        .hasMessageContaining("concept");
  }

  @Test
  void rejectsUndeclaredTableInJoinInsideCteBody() {
    // Mirrors the real OMOP concept-resolution CTE, which joins several undeclared
    // vocabulary tables within its body.
    assertThatThrownBy(
            () ->
                validate(
                    "WITH cr AS ("
                        + "  SELECT * FROM concept c"
                        + "  LEFT JOIN concept_relationship rel ON c.concept_id = rel.concept_id_1"
                        + ") SELECT * FROM cr"))
        .isInstanceOf(InvalidRequestException.class)
        .hasMessageContaining("undeclared table");
  }

  @Test
  void rejectsUndeclaredLabelInsideCteBody() {
    // A label reference inside a CTE body must still be checked against the declared
    // set: when 'patients' is not declared, the relation is undeclared and rejected.
    assertThatThrownBy(() -> validate("WITH foo AS (SELECT * FROM patients) SELECT * FROM foo"))
        .isInstanceOf(InvalidRequestException.class)
        .hasMessageContaining("undeclared table")
        .hasMessageContaining("patients");
  }

  @Test
  void rejectsDatasourceShortNameFileReadInsideCteBody() {
    // The datasource short-name file-read lane must stay closed inside CTE bodies too.
    assertThatThrownBy(
            () -> validate("WITH leak AS (SELECT * FROM csv.`/etc/passwd`) SELECT * FROM leak"))
        .isInstanceOf(InvalidRequestException.class)
        .hasMessageContaining("undeclared table");
  }

  @Test
  void rejectsUndeclaredTableInNestedCteBody() {
    // Nested WITH: the inner CTE 'b' is defined inside the body of the outer CTE 'a'.
    assertThatThrownBy(
            () ->
                validate(
                    "WITH a AS (WITH b AS (SELECT * FROM concept) SELECT * FROM b) SELECT * FROM"
                        + " a"))
        .isInstanceOf(InvalidRequestException.class)
        .hasMessageContaining("undeclared table")
        .hasMessageContaining("concept");
  }

  @Test
  void acceptsCteBodyReferencingDeclaredLabel() {
    // The positive counterpart: once the body is walked, a declared label inside it
    // resolves cleanly and the query is accepted.
    assertThatCode(() -> validate("WITH cr AS (SELECT * FROM concept) SELECT * FROM cr", "concept"))
        .doesNotThrowAnyException();
  }

  // -------------------------------------------------------------------------
  // View-derived Pathling UDFs flow through unaffected. ViewDefinition
  // FHIRPath expressions compile to Spark plans that contain ScalaUDFs (for
  // terminology and FHIRPath helpers). When user SQL references the resulting
  // temp view, Spark substitutes the view's analyzed plan into the user
  // plan, so those ScalaUDFs appear in the analyzed-plan walk but never in
  // the unresolved-plan walk where the non-built-in rejection fires.
  // -------------------------------------------------------------------------

  @Test
  void permitsViewDerivedScalaUdfInAnalyzedPlan() {
    // Build a dataset whose plan contains a Pathling ScalaUDF (decimal_to_literal),
    // simulating what FhirViewExecutor produces when the view's FHIRPath uses one
    // of these helpers.
    final org.apache.spark.sql.Dataset<org.apache.spark.sql.Row> viewBacking =
        sparkSession.sql("SELECT decimal_to_literal(CAST(1.5 AS DECIMAL(10, 2)), 1) AS literal");
    viewBacking.createOrReplaceTempView(VIEW_NAME);

    // User SQL is plain ANSI — no UDF reference. The unresolved walk sees only
    // an UnresolvedRelation; the analyzed walk sees the substituted ScalaUDF.
    final String userSql = "SELECT literal FROM " + VIEW_NAME;
    assertThatCode(() -> validate(userSql, VIEW_NAME)).doesNotThrowAnyException();

    final org.apache.spark.sql.catalyst.plans.logical.LogicalPlan analyzed =
        sparkSession.sql(userSql).queryExecution().analyzed();
    assertThatCode(() -> sqlValidator.validateAnalyzed(analyzed, Set.of(VIEW_NAME)))
        .doesNotThrowAnyException();
  }

  // -------------------------------------------------------------------------
  // Spark normalises identifiers to lowercase when resolving SQL references
  // against the catalog (default spark.sql.caseSensitive=false), so the
  // SubqueryAlias produced by ResolveRelations carries a lowercased name even
  // when the temp view was registered with mixed case. The trust window must
  // be matched case-insensitively, otherwise every LogicalRelation reachable
  // through a request-scoped temp view registered with a mixed-case request
  // id (HAPI's default) would be rejected.
  // -------------------------------------------------------------------------

  @Test
  void permitsLogicalRelationUnderTempViewRegisteredWithMixedCaseName(
      @org.junit.jupiter.api.io.TempDir final java.nio.file.Path tmp) {
    // A parquet file produces a LogicalRelation in the analyzed plan, so the
    // trust window must extend through the SubqueryAlias to allow the read.
    final String parquetPath = tmp.resolve("data").toString();
    sparkSession.range(3).toDF("id").write().mode("overwrite").parquet(parquetPath);

    // Mixed-case name mirrors what ViewRegistrationService produces from a
    // HAPI request id like "2JmwEs1TyviJBzyP".
    final String mixedCaseName = "sqlquery_2JmwEs1TyviJBzyP_patients";
    sparkSession.read().parquet(parquetPath).createOrReplaceTempView(mixedCaseName);

    try {
      // Spark accepts either case in user SQL and resolves to the same temp view; the
      // SubqueryAlias name in the analyzed plan is the lowercased form.
      final org.apache.spark.sql.catalyst.plans.logical.LogicalPlan analyzed =
          sparkSession.sql("SELECT id FROM " + mixedCaseName).queryExecution().analyzed();
      assertThatCode(() -> sqlValidator.validateAnalyzed(analyzed, Set.of(mixedCaseName)))
          .doesNotThrowAnyException();
    } finally {
      sparkSession.catalog().dropTempView(mixedCaseName);
    }
  }

  // -------------------------------------------------------------------------
  // Table-valued function (TVF) rejection. Built-in TVFs in the FROM clause
  // (range, explode, json_tuple, ...) are an open denial-of-service lane:
  // `SELECT count(*) FROM range(0, 1e6) a CROSS JOIN range(0, 1e6) b` is a
  // 1e12-row Cartesian product. Drop the entire UnresolvedTableValuedFunction
  // node class from the allow-list.
  // -------------------------------------------------------------------------

  @Test
  void rejectsRangeTvfInFromClause() {
    assertThatThrownBy(() -> validate("SELECT * FROM range(0, 100)"))
        .isInstanceOf(InvalidRequestException.class)
        .hasMessageContaining("disallowed plan node");
  }

  @Test
  void rejectsRangeCrossJoinTvfDosAttack() {
    // The literal exploit recorded in the threat model.
    assertThatThrownBy(
            () ->
                validate("SELECT count(*) FROM range(0, 1000000) a CROSS JOIN range(0, 1000000) b"))
        .isInstanceOf(InvalidRequestException.class)
        .hasMessageContaining("disallowed plan node");
  }

  @Test
  void rejectsExplodeTvfInFromClause() {
    assertThatThrownBy(() -> validate("SELECT * FROM explode(array(1, 2, 3))"))
        .isInstanceOf(InvalidRequestException.class)
        .hasMessageContaining("disallowed plan node");
  }

  // -------------------------------------------------------------------------
  // Non-regression: explode in projection position is rewritten by the
  // analyser to a Generate node with an Explode expression. Both are
  // allow-listed, so closing the TVF lane must not affect this form.
  // -------------------------------------------------------------------------

  @Test
  void acceptsExplodeInProjection() {
    assertThatCode(() -> validate("SELECT explode(array(1, 2, 3)) AS x"))
        .doesNotThrowAnyException();
  }

  // -------------------------------------------------------------------------
  // DESCRIBE [TABLE] <label> — the plain named-view introspection form is
  // allowed for declared labels; every unsafe variant is rejected at parse
  // time (issue #2651, spec 029 US1/US3).
  // -------------------------------------------------------------------------

  @Test
  void acceptsDescribeOfDeclaredLabel() {
    assertThatCode(() -> validate("DESCRIBE patients", "patients")).doesNotThrowAnyException();
  }

  @Test
  void acceptsDescribeTableOfDeclaredLabel() {
    assertThatCode(() -> validate("DESCRIBE TABLE patients", "patients"))
        .doesNotThrowAnyException();
  }

  @Test
  void acceptsDescSynonymOfDeclaredLabel() {
    assertThatCode(() -> validate("DESC patients", "patients")).doesNotThrowAnyException();
  }

  @Test
  void acceptsLowerCaseDescribeOfDeclaredLabel() {
    assertThatCode(() -> validate("describe patients", "patients")).doesNotThrowAnyException();
  }

  @Test
  void rejectsDescribeExtended() {
    assertThatThrownBy(() -> validate("DESCRIBE EXTENDED patients", "patients"))
        .isInstanceOf(InvalidRequestException.class)
        .hasMessageContaining("disallowed operation")
        .hasMessageContaining("EXTENDED");
  }

  @Test
  void rejectsDescribeFormatted() {
    assertThatThrownBy(() -> validate("DESCRIBE FORMATTED patients", "patients"))
        .isInstanceOf(InvalidRequestException.class)
        .hasMessageContaining("disallowed operation")
        .hasMessageContaining("EXTENDED");
  }

  @Test
  void rejectsDescribeWithPartitionSpec() {
    assertThatThrownBy(() -> validate("DESCRIBE patients PARTITION (x = 1)", "patients"))
        .isInstanceOf(InvalidRequestException.class)
        .hasMessageContaining("disallowed operation")
        .hasMessageContaining("partition");
  }

  @Test
  void rejectsDescribeColumnForm() {
    assertThatThrownBy(() -> validate("DESCRIBE patients id", "patients"))
        .isInstanceOf(InvalidRequestException.class)
        .hasMessageContaining("disallowed operation");
  }

  @Test
  void rejectsDescribeExtendedAsJson() {
    assertThatThrownBy(() -> validate("DESCRIBE EXTENDED patients AS JSON", "patients"))
        .isInstanceOf(InvalidRequestException.class)
        .hasMessageContaining("disallowed operation");
  }

  @Test
  void rejectsDescribeOfUndeclaredLabel() {
    assertThatThrownBy(() -> validate("DESCRIBE undeclared", "patients"))
        .isInstanceOf(InvalidRequestException.class)
        .hasMessageContaining("undeclared table");
  }

  @Test
  void rejectsDescribeOfMultiPartIdentifier() {
    assertThatThrownBy(() -> validate("DESCRIBE db.patients", "patients"))
        .isInstanceOf(InvalidRequestException.class)
        .hasMessageContaining("undeclared table");
  }

  @Test
  void rejectsDescribeOfTempViewShapedNameNotInLabelSet() {
    // A name shaped like a request-scoped temp view but not a declared label must be rejected,
    // so one request cannot introspect another request's views.
    assertThatThrownBy(() -> validate("DESCRIBE sqlquery_abc123_patients", "patients"))
        .isInstanceOf(InvalidRequestException.class)
        .hasMessageContaining("undeclared table");
  }

  // -------------------------------------------------------------------------
  // DESCRIBE [QUERY] <query> — the query-introspection form is allowed and its
  // inner query is validated exactly like a directly submitted query (spec 029
  // US2). The QUERY keyword is optional in the Spark grammar.
  // -------------------------------------------------------------------------

  @Test
  void acceptsDescribeQueryOverDeclaredLabel() {
    assertThatCode(() -> validate("DESCRIBE QUERY SELECT id FROM patients", "patients"))
        .doesNotThrowAnyException();
  }

  @Test
  void acceptsDescQueryForm() {
    assertThatCode(() -> validate("DESC QUERY SELECT id FROM patients", "patients"))
        .doesNotThrowAnyException();
  }

  @Test
  void acceptsKeywordlessDescribeQuery() {
    // The QUERY keyword is optional; DESCRIBE SELECT ... parses to the same command.
    assertThatCode(() -> validate("DESCRIBE SELECT id FROM patients", "patients"))
        .doesNotThrowAnyException();
  }

  @Test
  void acceptsDescribeQueryWithCte() {
    assertThatCode(
            () ->
                validate(
                    "DESCRIBE QUERY WITH x AS (SELECT id FROM patients) SELECT * FROM x",
                    "patients"))
        .doesNotThrowAnyException();
  }

  @Test
  void rejectsDescribeQueryOverUndeclaredTable() {
    assertThatThrownBy(() -> validate("DESCRIBE QUERY SELECT * FROM undeclared", "patients"))
        .isInstanceOf(InvalidRequestException.class)
        .hasMessageContaining("undeclared table");
  }

  @Test
  void rejectsDescribeQueryWithReflectFunction() {
    assertThatThrownBy(
            () -> validate("DESCRIBE QUERY SELECT reflect('java.lang.System', 'getenv')"))
        .isInstanceOf(InvalidRequestException.class)
        .hasMessageContaining("disallowed function");
  }

  @Test
  void rejectsDescribeQueryWithNonBuiltInFunction() {
    assertThatThrownBy(
            () ->
                validate(
                    "DESCRIBE QUERY SELECT member_of(coding, 'http://snomed.info/sct?fhir_vs')"
                        + " FROM patients",
                    "patients"))
        .isInstanceOf(InvalidRequestException.class)
        .hasMessageContaining("non-built-in function");
  }

  @Test
  void rejectsDescribeQueryWithDisallowedPlanNode() {
    // The range TVF is a disallowed plan node in the inner query, just as in a submitted query.
    assertThatThrownBy(() -> validate("DESCRIBE QUERY SELECT * FROM range(0, 100)"))
        .isInstanceOf(InvalidRequestException.class)
        .hasMessageContaining("disallowed plan node");
  }

  // -------------------------------------------------------------------------
  // Invalid SQL syntax.
  // -------------------------------------------------------------------------

  @Test
  void rejectsInvalidSqlSyntax() {
    assertThatThrownBy(() -> validate("NOT VALID SQL AT ALL"))
        .isInstanceOf(InvalidRequestException.class)
        .hasMessageContaining("Invalid SQL syntax");
  }

  // -------------------------------------------------------------------------
  // Regression corpus — known-bad queries the validator must reject.
  // -------------------------------------------------------------------------

  /**
   * Documents the queries that must always be rejected. New entries here lock current rejection
   * behaviour into the test suite and double as a security-model checklist for future readers.
   */
  @org.junit.jupiter.params.ParameterizedTest(name = "rejects: {0}")
  @org.junit.jupiter.params.provider.ValueSource(
      strings = {
        // Hive metastore commands.
        "LOAD DATA INPATH '/tmp/x' INTO TABLE my_table",
        "MSCK REPAIR TABLE my_table",
        // Session configuration.
        "SET spark.sql.shuffle.partitions = 200",
        "SET",
        "RESET",
        // INSERT variants.
        "INSERT OVERWRITE TABLE my_table SELECT 1",
        "INSERT INTO my_table SELECT 1",
        // Data manipulation.
        "TRUNCATE TABLE my_table",
        "ALTER TABLE my_table ADD COLUMNS (x INT)",
        "MERGE INTO target USING source ON target.id = source.id WHEN MATCHED THEN UPDATE SET"
            + " target.x = source.x",
        // Catalog management.
        "CREATE DATABASE my_db",
        "DROP DATABASE my_db",
        "USE my_db",
        // Cache control.
        "CACHE TABLE my_table",
        "UNCACHE TABLE my_table",
        "REFRESH TABLE my_table",
        // Discovery / SHOW.
        "SHOW TABLES",
        "DESCRIBE my_table",
        "EXPLAIN SELECT 1",
        // Reflection-style functions.
        "SELECT reflect('java.lang.Runtime', 'getRuntime')",
        "SELECT java_method('java.lang.Math', 'random')"
      })
  void rejectsKnownDangerousQueries(final String sql) {
    assertThatThrownBy(() -> validate(sql))
        .as("validator must reject: %s", sql)
        .isInstanceOf(InvalidRequestException.class);
  }
}
