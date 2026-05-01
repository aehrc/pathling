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

import ca.uhn.fhir.rest.server.exceptions.InvalidRequestException;
import jakarta.annotation.Nonnull;
import java.util.List;
import java.util.Set;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.analysis.UnresolvedFunction;
import org.apache.spark.sql.catalyst.expressions.Expression;
import org.apache.spark.sql.catalyst.expressions.ScalaUDF;
import org.apache.spark.sql.catalyst.expressions.SubqueryExpression;
import org.apache.spark.sql.catalyst.plans.logical.Command;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import scala.Option;
import scala.jdk.javaapi.CollectionConverters;

/**
 * Validates that SQL queries are strictly read-only and contain only allowed operations. This
 * prevents SQL injection attacks and ensures that users cannot execute DDL, DML, or other dangerous
 * operations through the {@code $sqlquery-run} endpoint.
 *
 * <p>Allow-lists and reject-lists are matched on fully-qualified class names so that classes from
 * unrelated packages (for example a third-party plugin's {@code Project}) cannot slip through.
 *
 * <p>Two validation entry-points are exposed:
 *
 * <ul>
 *   <li>{@link #validate(String)} parses the SQL and walks the unresolved logical plan. Cheap and
 *       suitable for early rejection before any temp views are registered.
 *   <li>{@link #validateAnalyzed(LogicalPlan)} walks an analyzed plan. Required to catch constructs
 *       like {@code HiveSimpleUDF} / {@code HiveGenericUDF}, which are inserted by the analyser and
 *       so are absent from the unresolved tree.
 * </ul>
 *
 * <p>Three validation layers are applied:
 *
 * <ol>
 *   <li><strong>Plan node allow-list</strong> — only read-only plan nodes are permitted. All {@link
 *       Command} subclasses are rejected outright.
 *   <li><strong>Expression allow-list</strong> — only safe expression types are permitted.
 *   <li><strong>UDF allow-list</strong> — {@link ScalaUDF} is permitted only for Pathling's
 *       registered UDFs.
 * </ol>
 *
 * @see <a
 *     href="https://build.fhir.org/ig/FHIR/sql-on-fhir-v2/OperationDefinition-SQLQueryRun.html">SQLQueryRun</a>
 */
@Slf4j
@Component
public class SqlValidator {

  /**
   * FQN prefix for Pathling-defined Catalyst plan nodes and expressions. Classes from this package
   * are trusted because Pathling owns them and uses them internally (e.g. for FhirView execution).
   * Without this carve-out, every expression class injected by the FhirView executor would have to
   * be hand-listed and kept in sync with the encoders module.
   */
  private static final String PATHLING_PACKAGE_PREFIX = "au.csiro.pathling.";

  @Nonnull private final SparkSession sparkSession;

  // Fully-qualified plan node class names that are permitted. All Command subclasses are
  // additionally rejected via instanceof check.
  private static final Set<String> ALLOWED_PLAN_NODES =
      Set.of(
          // org.apache.spark.sql.catalyst.plans.logical
          "org.apache.spark.sql.catalyst.plans.logical.Project",
          "org.apache.spark.sql.catalyst.plans.logical.Filter",
          "org.apache.spark.sql.catalyst.plans.logical.Sort",
          "org.apache.spark.sql.catalyst.plans.logical.GlobalLimit",
          "org.apache.spark.sql.catalyst.plans.logical.LocalLimit",
          "org.apache.spark.sql.catalyst.plans.logical.Tail",
          "org.apache.spark.sql.catalyst.plans.logical.Offset",
          "org.apache.spark.sql.catalyst.plans.logical.ReturnAnswer",
          "org.apache.spark.sql.catalyst.plans.logical.Union",
          "org.apache.spark.sql.catalyst.plans.logical.Intersect",
          "org.apache.spark.sql.catalyst.plans.logical.Except",
          "org.apache.spark.sql.catalyst.plans.logical.Join",
          "org.apache.spark.sql.catalyst.plans.logical.LateralJoin",
          "org.apache.spark.sql.catalyst.plans.logical.AsOfJoin",
          "org.apache.spark.sql.catalyst.plans.logical.Aggregate",
          "org.apache.spark.sql.catalyst.plans.logical.Expand",
          "org.apache.spark.sql.catalyst.plans.logical.Window",
          "org.apache.spark.sql.catalyst.plans.logical.WithWindowDefinition",
          "org.apache.spark.sql.catalyst.plans.logical.SubqueryAlias",
          "org.apache.spark.sql.catalyst.plans.logical.LocalRelation",
          "org.apache.spark.sql.catalyst.plans.logical.OneRowRelation",
          "org.apache.spark.sql.catalyst.plans.logical.EmptyRelation",
          "org.apache.spark.sql.catalyst.plans.logical.Range",
          "org.apache.spark.sql.catalyst.plans.logical.View",
          "org.apache.spark.sql.catalyst.plans.logical.WithCTE",
          "org.apache.spark.sql.catalyst.plans.logical.CTERelationDef",
          "org.apache.spark.sql.catalyst.plans.logical.CTERelationRef",
          "org.apache.spark.sql.catalyst.plans.logical.UnresolvedWith",
          "org.apache.spark.sql.catalyst.plans.logical.Distinct",
          "org.apache.spark.sql.catalyst.plans.logical.Deduplicate",
          "org.apache.spark.sql.catalyst.plans.logical.Pivot",
          "org.apache.spark.sql.catalyst.plans.logical.Unpivot",
          "org.apache.spark.sql.catalyst.plans.logical.Generate",
          "org.apache.spark.sql.catalyst.plans.logical.ResolvedHint",
          "org.apache.spark.sql.catalyst.plans.logical.UnresolvedHint",
          "org.apache.spark.sql.catalyst.plans.logical.Sample",
          "org.apache.spark.sql.catalyst.plans.logical.Repartition",
          "org.apache.spark.sql.catalyst.plans.logical.RebalancePartitions",
          "org.apache.spark.sql.catalyst.plans.logical.Subquery",
          // org.apache.spark.sql.catalyst.analysis
          "org.apache.spark.sql.catalyst.analysis.UnresolvedRelation",
          "org.apache.spark.sql.catalyst.analysis.UnresolvedInlineTable",
          "org.apache.spark.sql.catalyst.analysis.UnresolvedTableValuedFunction",
          "org.apache.spark.sql.catalyst.analysis.UnresolvedSubqueryColumnAliases");

  // Function names rejected outright because they enable arbitrary code execution.
  private static final Set<String> REJECTED_FUNCTION_NAMES =
      Set.of("reflect", "java_method", "try_reflect");

  // Pathling-registered UDF names that are allowed in ScalaUDF expressions. Source of truth lives
  // here; SqlValidatorIntegrityTest fails if this drifts from the actual Spark function registry.
  private static final Set<String> ALLOWED_UDF_NAMES =
      Set.of(
          // Terminology UDFs (registered by TerminologyUdfRegistrar).
          "display",
          "member_of",
          "subsumes",
          "designation",
          "translate_coding",
          "property_string",
          "property_code",
          "property_integer",
          "property_boolean",
          "property_decimal",
          "property_dateTime",
          "property_Coding",
          // FHIRPath UDFs (registered by PathlingUdfRegistrar).
          "to_null",
          "string_to_quantity",
          "quantity_to_literal",
          "decimal_to_literal",
          "coding_to_literal",
          "convert_quantity_to_unit",
          "low_boundary_for_date",
          "high_boundary_for_date",
          "low_boundary_for_time",
          "high_boundary_for_time");

  // Fully-qualified expression class names that are permitted (besides UnresolvedFunction and
  // ScalaUDF, which receive special handling).
  @SuppressWarnings("java:S1192")
  private static final Set<String> ALLOWED_EXPRESSION_NAMES =
      Set.of(
          // Literals and references.
          "org.apache.spark.sql.catalyst.expressions.Literal",
          "org.apache.spark.sql.catalyst.analysis.UnresolvedAttribute",
          "org.apache.spark.sql.catalyst.analysis.UnresolvedStar",
          "org.apache.spark.sql.catalyst.expressions.UnresolvedNamedLambdaVariable",
          "org.apache.spark.sql.catalyst.expressions.NamedLambdaVariable",
          "org.apache.spark.sql.catalyst.expressions.objects.LambdaVariable",
          "org.apache.spark.sql.catalyst.expressions.Alias",
          "org.apache.spark.sql.catalyst.analysis.UnresolvedAlias",
          "org.apache.spark.sql.catalyst.expressions.OuterReference",
          "org.apache.spark.sql.catalyst.analysis.UnresolvedOrdinal",
          "org.apache.spark.sql.catalyst.expressions.VariableReference",
          "org.apache.spark.sql.catalyst.expressions.AttributeReference",
          "org.apache.spark.sql.catalyst.expressions.BoundReference",
          // Arithmetic.
          "org.apache.spark.sql.catalyst.expressions.Add",
          "org.apache.spark.sql.catalyst.expressions.Subtract",
          "org.apache.spark.sql.catalyst.expressions.Multiply",
          "org.apache.spark.sql.catalyst.expressions.Divide",
          "org.apache.spark.sql.catalyst.expressions.IntegralDivide",
          "org.apache.spark.sql.catalyst.expressions.Remainder",
          "org.apache.spark.sql.catalyst.expressions.Pmod",
          "org.apache.spark.sql.catalyst.expressions.UnaryMinus",
          "org.apache.spark.sql.catalyst.expressions.UnaryPositive",
          "org.apache.spark.sql.catalyst.expressions.Abs",
          // Comparison.
          "org.apache.spark.sql.catalyst.expressions.EqualTo",
          "org.apache.spark.sql.catalyst.expressions.EqualNullSafe",
          "org.apache.spark.sql.catalyst.expressions.LessThan",
          "org.apache.spark.sql.catalyst.expressions.LessThanOrEqual",
          "org.apache.spark.sql.catalyst.expressions.GreaterThan",
          "org.apache.spark.sql.catalyst.expressions.GreaterThanOrEqual",
          // Predicates.
          "org.apache.spark.sql.catalyst.expressions.In",
          "org.apache.spark.sql.catalyst.expressions.InSet",
          "org.apache.spark.sql.catalyst.expressions.InSubquery",
          "org.apache.spark.sql.catalyst.expressions.Between",
          "org.apache.spark.sql.catalyst.expressions.Like",
          "org.apache.spark.sql.catalyst.expressions.ILike",
          "org.apache.spark.sql.catalyst.expressions.RLike",
          "org.apache.spark.sql.catalyst.expressions.LikeAll",
          "org.apache.spark.sql.catalyst.expressions.LikeAny",
          "org.apache.spark.sql.catalyst.expressions.NotLikeAll",
          "org.apache.spark.sql.catalyst.expressions.NotLikeAny",
          "org.apache.spark.sql.catalyst.expressions.Contains",
          "org.apache.spark.sql.catalyst.expressions.StartsWith",
          "org.apache.spark.sql.catalyst.expressions.EndsWith",
          "org.apache.spark.sql.catalyst.expressions.StringInstr",
          "org.apache.spark.sql.catalyst.expressions.StringLocate",
          "org.apache.spark.sql.catalyst.expressions.FindInSet",
          // Logical operators.
          "org.apache.spark.sql.catalyst.expressions.And",
          "org.apache.spark.sql.catalyst.expressions.Or",
          "org.apache.spark.sql.catalyst.expressions.Not",
          // Null handling.
          "org.apache.spark.sql.catalyst.expressions.IsNull",
          "org.apache.spark.sql.catalyst.expressions.IsNotNull",
          "org.apache.spark.sql.catalyst.expressions.Coalesce",
          "org.apache.spark.sql.catalyst.expressions.NullIf",
          "org.apache.spark.sql.catalyst.expressions.Nvl",
          "org.apache.spark.sql.catalyst.expressions.Nvl2",
          "org.apache.spark.sql.catalyst.expressions.NaNvl",
          "org.apache.spark.sql.catalyst.expressions.IsNaN",
          // Conditional.
          "org.apache.spark.sql.catalyst.expressions.If",
          "org.apache.spark.sql.catalyst.expressions.CaseWhen",
          "org.apache.spark.sql.catalyst.expressions.Greatest",
          "org.apache.spark.sql.catalyst.expressions.Least",
          // Type casting.
          "org.apache.spark.sql.catalyst.expressions.Cast",
          "org.apache.spark.sql.catalyst.expressions.ToBinary",
          "org.apache.spark.sql.catalyst.expressions.TryToBinary",
          "org.apache.spark.sql.catalyst.expressions.ToNumber",
          "org.apache.spark.sql.catalyst.expressions.TryToNumber",
          "org.apache.spark.sql.catalyst.expressions.ToCharacter",
          // Sorting.
          "org.apache.spark.sql.catalyst.expressions.SortOrder",
          // Subquery expressions.
          "org.apache.spark.sql.catalyst.expressions.ScalarSubquery",
          "org.apache.spark.sql.catalyst.expressions.Exists",
          "org.apache.spark.sql.catalyst.expressions.ListQuery",
          "org.apache.spark.sql.catalyst.expressions.LateralSubquery",
          // Generator expressions.
          "org.apache.spark.sql.catalyst.expressions.Explode",
          "org.apache.spark.sql.catalyst.expressions.PosExplode",
          "org.apache.spark.sql.catalyst.expressions.Inline",
          "org.apache.spark.sql.catalyst.expressions.Stack",
          // Window expressions.
          "org.apache.spark.sql.catalyst.expressions.WindowExpression",
          "org.apache.spark.sql.catalyst.expressions.WindowSpecDefinition",
          "org.apache.spark.sql.catalyst.expressions.SpecifiedWindowFrame",
          "org.apache.spark.sql.catalyst.expressions.UnspecifiedFrame",
          // Struct, array, and map expressions.
          "org.apache.spark.sql.catalyst.expressions.CreateNamedStruct",
          "org.apache.spark.sql.catalyst.expressions.CreateArray",
          "org.apache.spark.sql.catalyst.expressions.CreateMap",
          "org.apache.spark.sql.catalyst.expressions.GetStructField",
          "org.apache.spark.sql.catalyst.expressions.GetArrayStructFields",
          "org.apache.spark.sql.catalyst.expressions.GetArrayItem",
          "org.apache.spark.sql.catalyst.expressions.GetMapValue",
          // Higher-order functions.
          "org.apache.spark.sql.catalyst.expressions.LambdaFunction",
          // Grouping.
          "org.apache.spark.sql.catalyst.expressions.Grouping",
          "org.apache.spark.sql.catalyst.expressions.GroupingID",
          // Unresolved expression types.
          "org.apache.spark.sql.catalyst.analysis.UnresolvedExtractValue",
          "org.apache.spark.sql.catalyst.analysis.UnresolvedRegex",
          "org.apache.spark.sql.catalyst.expressions.NamedArgumentExpression",
          // Non-deterministic (safe).
          "org.apache.spark.sql.catalyst.expressions.Rand",
          "org.apache.spark.sql.catalyst.expressions.Randn",
          "org.apache.spark.sql.catalyst.expressions.Uuid",
          "org.apache.spark.sql.catalyst.expressions.Shuffle",
          // Error handling.
          "org.apache.spark.sql.catalyst.expressions.RaiseError",
          "org.apache.spark.sql.catalyst.expressions.TryEval",
          "org.apache.spark.sql.catalyst.expressions.AssertTrue",
          // Utility.
          "org.apache.spark.sql.catalyst.expressions.Empty2Null",
          "org.apache.spark.sql.catalyst.expressions.TypeOf",
          // Bitwise operations.
          "org.apache.spark.sql.catalyst.expressions.BitwiseAnd",
          "org.apache.spark.sql.catalyst.expressions.BitwiseOr",
          "org.apache.spark.sql.catalyst.expressions.BitwiseXor",
          "org.apache.spark.sql.catalyst.expressions.BitwiseNot");

  // Fully-qualified expression class names that are explicitly rejected.
  private static final Set<String> REJECTED_EXPRESSION_NAMES =
      Set.of(
          "org.apache.spark.sql.catalyst.expressions.CallMethodViaReflection",
          "org.apache.spark.sql.catalyst.expressions.PythonUDF",
          "org.apache.spark.sql.hive.HiveSimpleUDF",
          "org.apache.spark.sql.hive.HiveGenericUDF",
          "org.apache.spark.sql.catalyst.expressions.PrintToStderr",
          "org.apache.spark.sql.catalyst.expressions.objects.StaticInvoke",
          "org.apache.spark.sql.catalyst.expressions.objects.Invoke",
          "org.apache.spark.sql.catalyst.expressions.objects.NewInstance");

  /**
   * Constructs a new SqlValidator.
   *
   * @param sparkSession the Spark session used for SQL parsing
   */
  @Autowired
  public SqlValidator(@Nonnull final SparkSession sparkSession) {
    this.sparkSession = sparkSession;
  }

  /**
   * Validates that the given SQL parses to an unresolved logical plan that contains only allowed
   * operations. Suitable for early rejection before any temp views are registered.
   *
   * @param sql the SQL query to validate
   * @throws InvalidRequestException if the query contains disallowed operations
   */
  public void validate(@Nonnull final String sql) {
    final LogicalPlan plan;
    try {
      plan = sparkSession.sessionState().sqlParser().parsePlan(sql);
    } catch (final Exception e) {
      throw new InvalidRequestException("Invalid SQL syntax: " + e.getMessage());
    }
    walkPlan(plan, /* strict= */ true);
  }

  /**
   * Validates an analyzed logical plan against the reject-list. Required to catch constructs like
   * {@code HiveSimpleUDF} / {@code HiveGenericUDF} that are inserted by the analyser and so are
   * absent from the unresolved tree.
   *
   * <p>Unlike {@link #validate(String)}, this walk does <em>not</em> enforce the allow-list,
   * because the analyser legitimately introduces many Spark expression types ({@code Flatten},
   * {@code GetStructField} variants, etc.) that user-written SQL would not name directly. The
   * unresolved-plan walk is the appropriate place to enforce the allow-list; the analyzed-plan
   * walk's job is to catch constructs that <em>only</em> appear post-analysis.
   *
   * @param plan an analyzed logical plan, typically obtained via {@code
   *     dataset.queryExecution().analyzed()}
   * @throws InvalidRequestException if the plan contains rejected operations
   */
  public void validateAnalyzed(@Nonnull final LogicalPlan plan) {
    walkPlan(plan, /* strict= */ false);
  }

  /** Recursively validates a logical plan node and all its children and expressions. */
  private void walkPlan(@Nonnull final LogicalPlan plan, final boolean strict) {
    validatePlanNode(plan, strict);
    final List<Expression> expressions = CollectionConverters.asJava(plan.expressions());
    for (final Expression expr : expressions) {
      walkExpression(expr, strict);
    }
    final List<LogicalPlan> children = CollectionConverters.asJava(plan.children());
    for (final LogicalPlan child : children) {
      walkPlan(child, strict);
    }
  }

  /**
   * Recursively validates an expression and all its child expressions. If the expression is a
   * subquery expression, its inner plan is also validated.
   */
  private void walkExpression(@Nonnull final Expression expr, final boolean strict) {
    validateExpression(expr, strict);
    if (expr instanceof final SubqueryExpression subquery) {
      walkPlan(subquery.plan(), strict);
    }
    final List<Expression> children = CollectionConverters.asJava(expr.children());
    for (final Expression child : children) {
      walkExpression(child, strict);
    }
  }

  /**
   * Validates that a plan node is permitted. All {@link Command} subclasses (DDL, DML, SHOW,
   * DESCRIBE, etc.) are rejected outright, regardless of package, in both modes. In strict mode,
   * the allow-list is additionally enforced.
   */
  private void validatePlanNode(@Nonnull final LogicalPlan plan, final boolean strict) {
    if (plan instanceof Command) {
      throw new InvalidRequestException(
          "SQL contains a disallowed operation: " + plan.getClass().getSimpleName());
    }
    if (!strict) {
      return;
    }
    final String fqn = canonicalClassName(plan);
    if (fqn.startsWith(PATHLING_PACKAGE_PREFIX)) {
      return;
    }
    if (!ALLOWED_PLAN_NODES.contains(fqn)) {
      throw new InvalidRequestException(
          "SQL contains a disallowed plan node: " + plan.getClass().getSimpleName());
    }
  }

  /**
   * Validates an expression. In both modes, explicitly rejected expression types and disallowed
   * function names are caught, and ScalaUDF is gated by the Pathling UDF allow-list. In strict
   * mode, the expression allow-list is additionally enforced.
   */
  private void validateExpression(@Nonnull final Expression expr, final boolean strict) {
    final String fqn = canonicalClassName(expr);

    if (REJECTED_EXPRESSION_NAMES.contains(fqn)) {
      throw new InvalidRequestException(
          "SQL contains a disallowed expression: " + expr.getClass().getSimpleName());
    }

    if (expr instanceof final UnresolvedFunction unresolvedFunc) {
      validateFunctionName(unresolvedFunc);
      return;
    }

    if (expr instanceof final ScalaUDF scalaUdf) {
      validateUdf(scalaUdf);
      return;
    }

    if (!strict) {
      return;
    }

    if (fqn.startsWith(PATHLING_PACKAGE_PREFIX)) {
      return;
    }

    if (!ALLOWED_EXPRESSION_NAMES.contains(fqn)) {
      throw new InvalidRequestException(
          "SQL contains a disallowed expression: " + expr.getClass().getSimpleName());
    }
  }

  /** Validates that an unresolved function is not a known-dangerous function. */
  private void validateFunctionName(@Nonnull final UnresolvedFunction func) {
    final List<String> nameParts = CollectionConverters.asJava(func.nameParts());
    if (!nameParts.isEmpty()) {
      final String simpleName = nameParts.getLast().toLowerCase();
      if (REJECTED_FUNCTION_NAMES.contains(simpleName)) {
        throw new InvalidRequestException(
            "SQL contains a disallowed function: " + String.join(".", nameParts));
      }
    }
  }

  /** Validates that a ScalaUDF is in the Pathling UDF allow-list. */
  private void validateUdf(@Nonnull final ScalaUDF udf) {
    final Option<String> nameOpt = udf.udfName();
    if (nameOpt.isDefined()) {
      final String name = nameOpt.get();
      if (!ALLOWED_UDF_NAMES.contains(name)) {
        throw new InvalidRequestException("SQL contains a disallowed UDF: " + name);
      }
    } else {
      throw new InvalidRequestException("SQL contains an unnamed UDF, which is not allowed");
    }
  }

  /**
   * Returns the FQN of the object's class with any trailing {@code $} stripped, so that Scala case
   * objects (whose runtime class name ends in {@code $}) match the form held in the allow-lists.
   */
  @Nonnull
  private static String canonicalClassName(@Nonnull final Object node) {
    final String name = node.getClass().getName();
    return name.endsWith("$") ? name.substring(0, name.length() - 1) : name;
  }

  /** Allow-list entries, exposed for build-time integrity tests. */
  @Nonnull
  static Set<String> allowedPlanNodes() {
    return ALLOWED_PLAN_NODES;
  }

  /** Allow-list entries, exposed for build-time integrity tests. */
  @Nonnull
  static Set<String> allowedExpressionNames() {
    return ALLOWED_EXPRESSION_NAMES;
  }

  /** Reject-list entries, exposed for build-time integrity tests. */
  @Nonnull
  static Set<String> rejectedExpressionNames() {
    return REJECTED_EXPRESSION_NAMES;
  }

  /** Allow-list entries, exposed for build-time integrity tests. */
  @Nonnull
  static Set<String> allowedUdfNames() {
    return ALLOWED_UDF_NAMES;
  }
}
