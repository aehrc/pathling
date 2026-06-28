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
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.regex.Pattern;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.FunctionIdentifier;
import org.apache.spark.sql.catalyst.analysis.UnresolvedFunction;
import org.apache.spark.sql.catalyst.analysis.UnresolvedRelation;
import org.apache.spark.sql.catalyst.catalog.HiveTableRelation;
import org.apache.spark.sql.catalyst.expressions.Expression;
import org.apache.spark.sql.catalyst.expressions.ExpressionInfo;
import org.apache.spark.sql.catalyst.expressions.SubqueryExpression;
import org.apache.spark.sql.catalyst.plans.logical.Command;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.catalyst.plans.logical.SubqueryAlias;
import org.apache.spark.sql.catalyst.plans.logical.UnresolvedWith;
import org.apache.spark.sql.execution.datasources.LogicalRelation;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import scala.Option;
import scala.Tuple2;
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
 *   <li>{@link #validate(String, Set)} parses the SQL and walks the unresolved logical plan,
 *       enforcing that every relation reference resolves to a declared label or a CTE defined
 *       within the same query. Cheap and suitable for early rejection before any temp views are
 *       registered.
 *   <li>{@link #validateAnalyzed(LogicalPlan, Set)} walks an analyzed plan. Required to catch
 *       constructs like {@code HiveSimpleUDF} / {@code HiveGenericUDF}, which are inserted by the
 *       analyser and so are absent from the unresolved tree, and to reject any leaf physical
 *       relation whose source is not one of the registered request-scoped temp views.
 * </ul>
 *
 * <p>Four validation layers are applied:
 *
 * <ol>
 *   <li><strong>Plan node allow-list</strong> — only read-only plan nodes are permitted. All {@link
 *       Command} subclasses are rejected outright.
 *   <li><strong>Relation allow-list</strong> — every {@link UnresolvedRelation} must be a
 *       single-part identifier present in the declared label set or among the CTEs defined within
 *       the query. This rejects datasource short-name file reads such as {@code
 *       parquet.`/etc/passwd`}, which would otherwise be resolved by Spark to a direct file read.
 *   <li><strong>Expression allow-list</strong> — only safe expression types are permitted.
 *   <li><strong>Built-in functions only</strong> — every {@link UnresolvedFunction} in user SQL
 *       must resolve to a built-in Spark function. Pathling-registered UDFs (terminology, FHIRPath
 *       helpers, etc.) are intended for use within ViewDefinitions; the {@code $sqlquery-run}
 *       surface stays portable and implementation-agnostic. UDFs introduced by referenced views are
 *       unaffected because they appear in the analyzed plan, not in the unresolved tree this rule
 *       walks.
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
          "org.apache.spark.sql.catalyst.analysis.UnresolvedSubqueryColumnAliases");

  // Function names rejected outright because they enable arbitrary code execution.
  private static final Set<String> REJECTED_FUNCTION_NAMES =
      Set.of("reflect", "java_method", "try_reflect");

  // Source marker used by Spark's ExpressionInfo for built-in functions. Anything else (scala_udf,
  // hive, python_udf, sql_udf, java_udf, ...) is implementation-specific and rejected from user
  // SQL.
  private static final String BUILTIN_FUNCTION_SOURCE = "built-in";

  // Fully-qualified expression class names that are permitted (besides UnresolvedFunction, which
  // receives special handling).
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
          // Named and positional query parameters (e.g. ":name", "?"). These are leaf, unevaluable
          // value placeholders bound at execution time via the parameterised SQL API, so they carry
          // no code-execution risk.
          "org.apache.spark.sql.catalyst.analysis.NamedParameter",
          "org.apache.spark.sql.catalyst.analysis.PosParameter",
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
   * Pattern that legitimate relation identifiers must match. Mirrors the {@code SqlLibraryParser}
   * label pattern so that anything Spark's parser produces as an UnresolvedRelation but which could
   * not have been a declared label is rejected outright.
   */
  private static final Pattern LABEL_PATTERN = Pattern.compile("^[A-Za-z]\\w*$");

  /**
   * Validates that the given SQL parses to an unresolved logical plan that contains only allowed
   * operations and that every relation reference resolves to either a declared label or a CTE
   * defined within the same query. Suitable for early rejection before any temp views are
   * registered.
   *
   * @param sql the SQL query to validate
   * @param allowedLabels the set of relation labels declared in the request (i.e. the labels of the
   *     {@code Library.relatedArtifact} entries) that may appear as relation references
   * @throws InvalidRequestException if the query contains disallowed operations or references an
   *     undeclared table
   */
  public void validate(@Nonnull final String sql, @Nonnull final Set<String> allowedLabels) {
    final LogicalPlan plan;
    try {
      plan = sparkSession.sessionState().sqlParser().parsePlan(sql);
    } catch (final Exception e) {
      throw new InvalidRequestException("Invalid SQL syntax: " + e.getMessage());
    }
    final Set<String> effectiveLabels = new HashSet<>(allowedLabels);
    collectCteNames(plan, effectiveLabels);
    walkPlanStrict(plan, effectiveLabels);
  }

  /**
   * Validates an analyzed logical plan against the reject-list and additionally rejects any leaf
   * relation whose source is not one of the request-scoped temp views the executor registered.
   * Required to catch constructs like {@code HiveSimpleUDF} / {@code HiveGenericUDF} that are
   * inserted by the analyser and so are absent from the unresolved tree, and to catch any future
   * parser change that would let a datasource short-name reference (e.g. {@code parquet.`/path`})
   * slip past the parse-time check.
   *
   * <p>Unlike {@link #validate(String, Set)}, this walk does <em>not</em> enforce the expression
   * allow-list, because the analyser legitimately introduces many Spark expression types ({@code
   * Flatten}, {@code GetStructField} variants, etc.) that user-written SQL would not name directly.
   *
   * @param plan an analyzed logical plan, typically obtained via {@code
   *     dataset.queryExecution().analyzed()}
   * @param registeredViewNames the names of the request-scoped temporary views that were registered
   *     for this query - any leaf physical relation must be reachable only as a descendant of a
   *     {@link SubqueryAlias} whose name appears in this set
   * @throws InvalidRequestException if the plan contains rejected operations or references a
   *     relation that is not one of the registered temp views
   */
  public void validateAnalyzed(
      @Nonnull final LogicalPlan plan, @Nonnull final Set<String> registeredViewNames) {
    walkPlanAnalyzed(plan, registeredViewNames, /* inTrustedAlias= */ false);
  }

  /**
   * Pre-walks the parsed plan to collect names of common-table-expressions defined anywhere in the
   * tree, so that {@code WITH ... SELECT} queries can refer to their CTEs without each CTE name
   * being a declared label.
   */
  private static void collectCteNames(
      @Nonnull final LogicalPlan plan, @Nonnull final Set<String> out) {
    if (plan instanceof final UnresolvedWith with) {
      final List<Tuple2<String, SubqueryAlias>> ctes =
          CollectionConverters.asJava(with.cteRelations());
      for (final Tuple2<String, SubqueryAlias> cte : ctes) {
        out.add(cte._1());
        collectCteNames(cte._2(), out);
      }
    }
    final List<LogicalPlan> children = CollectionConverters.asJava(plan.children());
    for (final LogicalPlan child : children) {
      collectCteNames(child, out);
    }
    final List<Expression> expressions = CollectionConverters.asJava(plan.expressions());
    for (final Expression expr : expressions) {
      collectCteNamesInExpression(expr, out);
    }
  }

  /** Recurses through expression subqueries to pick up CTEs defined inside them. */
  private static void collectCteNamesInExpression(
      @Nonnull final Expression expr, @Nonnull final Set<String> out) {
    if (expr instanceof final SubqueryExpression subquery) {
      collectCteNames(subquery.plan(), out);
    }
    final List<Expression> children = CollectionConverters.asJava(expr.children());
    for (final Expression child : children) {
      collectCteNamesInExpression(child, out);
    }
  }

  /** Recursively validates a parsed plan in strict mode. */
  private void walkPlanStrict(
      @Nonnull final LogicalPlan plan, @Nonnull final Set<String> allowedLabels) {
    validatePlanNodeStrict(plan, allowedLabels);
    final List<Expression> expressions = CollectionConverters.asJava(plan.expressions());
    for (final Expression expr : expressions) {
      walkExpressionStrict(expr, allowedLabels);
    }
    // A WITH node exposes its CTE definition bodies as innerChildren, not children, so the generic
    // child walk below never reaches them. Walk them explicitly so that relation references inside
    // a CTE body are validated against the declared-label set, just like top-level relations. The
    // CTE names themselves are already in allowedLabels (collectCteNames runs first), so a CTE body
    // may legitimately reference a sibling or declared label but not an undeclared external table.
    if (plan instanceof final UnresolvedWith with) {
      final List<Tuple2<String, SubqueryAlias>> ctes =
          CollectionConverters.asJava(with.cteRelations());
      for (final Tuple2<String, SubqueryAlias> cte : ctes) {
        walkPlanStrict(cte._2(), allowedLabels);
      }
    }
    final List<LogicalPlan> children = CollectionConverters.asJava(plan.children());
    for (final LogicalPlan child : children) {
      walkPlanStrict(child, allowedLabels);
    }
  }

  /** Recursively validates an analyzed plan, tracking whether we are inside a trusted alias. */
  private void walkPlanAnalyzed(
      @Nonnull final LogicalPlan plan,
      @Nonnull final Set<String> registeredViewNames,
      final boolean inTrustedAlias) {
    validatePlanNodeAnalyzed(plan, registeredViewNames, inTrustedAlias);
    final List<Expression> expressions = CollectionConverters.asJava(plan.expressions());
    for (final Expression expr : expressions) {
      walkExpressionAnalyzed(expr, registeredViewNames);
    }
    final boolean childTrust = inTrustedAlias || isTrustedAlias(plan, registeredViewNames);
    final List<LogicalPlan> children = CollectionConverters.asJava(plan.children());
    for (final LogicalPlan child : children) {
      walkPlanAnalyzed(child, registeredViewNames, childTrust);
    }
  }

  /**
   * Returns true when the given node is a SubqueryAlias whose name is a registered temp view.
   *
   * <p>The comparison is case-insensitive because Spark normalises identifiers when resolving SQL
   * references against the catalog (default {@code spark.sql.caseSensitive=false}). The {@link
   * SubqueryAlias} synthesised by {@code ResolveRelations} therefore carries a lowercased name even
   * when the temp view was registered with mixed case (HAPI's request ids, used to scope the temp
   * view names, are mixed-case alphanumerics). Without this case-insensitive match, every {@link
   * LogicalRelation} reachable through a request-scoped temp view would be rejected even though it
   * is the legitimate backing for a registered view.
   */
  private static boolean isTrustedAlias(
      @Nonnull final LogicalPlan plan, @Nonnull final Set<String> registeredViewNames) {
    if (!(plan instanceof final SubqueryAlias alias)) {
      return false;
    }
    final String aliasName = alias.identifier().name();
    for (final String registered : registeredViewNames) {
      if (registered.equalsIgnoreCase(aliasName)) {
        return true;
      }
    }
    return false;
  }

  /** Recursively validates an expression tree (strict mode). */
  private void walkExpressionStrict(
      @Nonnull final Expression expr, @Nonnull final Set<String> allowedLabels) {
    validateExpression(expr, /* strict= */ true);
    if (expr instanceof final SubqueryExpression subquery) {
      walkPlanStrict(subquery.plan(), allowedLabels);
    }
    final List<Expression> children = CollectionConverters.asJava(expr.children());
    for (final Expression child : children) {
      walkExpressionStrict(child, allowedLabels);
    }
  }

  /** Recursively validates an expression tree (analyzed mode). */
  private void walkExpressionAnalyzed(
      @Nonnull final Expression expr, @Nonnull final Set<String> registeredViewNames) {
    validateExpression(expr, /* strict= */ false);
    if (expr instanceof final SubqueryExpression subquery) {
      walkPlanAnalyzed(subquery.plan(), registeredViewNames, /* inTrustedAlias= */ false);
    }
    final List<Expression> children = CollectionConverters.asJava(expr.children());
    for (final Expression child : children) {
      walkExpressionAnalyzed(child, registeredViewNames);
    }
  }

  /**
   * Validates that a parsed plan node is permitted. All {@link Command} subclasses (DDL, DML, SHOW,
   * DESCRIBE, etc.) are rejected outright. The plan-node allow-list is enforced and relation
   * references are checked against the supplied label set.
   */
  private void validatePlanNodeStrict(
      @Nonnull final LogicalPlan plan, @Nonnull final Set<String> allowedLabels) {
    if (plan instanceof Command) {
      throw new InvalidRequestException(
          "SQL contains a disallowed operation: " + plan.getClass().getSimpleName());
    }
    final String fqn = canonicalClassName(plan);
    if (fqn.startsWith(PATHLING_PACKAGE_PREFIX)) {
      return;
    }
    if (!ALLOWED_PLAN_NODES.contains(fqn)) {
      throw new InvalidRequestException(
          "SQL contains a disallowed plan node: " + plan.getClass().getSimpleName());
    }
    if (plan instanceof final UnresolvedRelation relation) {
      validateRelationReference(relation, allowedLabels);
    }
  }

  /**
   * Validates that an analyzed plan node is permitted. All {@link Command} subclasses are rejected
   * outright. Any {@link UnresolvedRelation} surviving analysis is rejected. Leaf physical
   * relations ({@link LogicalRelation}, {@link HiveTableRelation}) are rejected unless they are
   * descendants of a trusted {@link SubqueryAlias}.
   */
  private void validatePlanNodeAnalyzed(
      @Nonnull final LogicalPlan plan,
      @Nonnull final Set<String> registeredViewNames,
      final boolean inTrustedAlias) {
    if (plan instanceof Command) {
      throw new InvalidRequestException(
          "SQL contains a disallowed operation: " + plan.getClass().getSimpleName());
    }
    if (plan instanceof final UnresolvedRelation relation) {
      throw new InvalidRequestException(
          "SQL references an undeclared table: " + joinIdentifier(relation));
    }
    if (!inTrustedAlias && (plan instanceof LogicalRelation || plan instanceof HiveTableRelation)) {
      throw new InvalidRequestException(
          "SQL references an unauthorised data source: " + plan.getClass().getSimpleName());
    }
  }

  /**
   * Enforces that an unresolved relation is a single-part identifier matching {@link
   * #LABEL_PATTERN} and present in the allowed-label set. Rejects two-part datasource short-name
   * references such as {@code parquet.`/etc/passwd`}.
   */
  private static void validateRelationReference(
      @Nonnull final UnresolvedRelation relation, @Nonnull final Set<String> allowedLabels) {
    final List<String> parts = CollectionConverters.asJava(relation.multipartIdentifier());
    if (parts.size() != 1) {
      throw new InvalidRequestException(
          "SQL references an undeclared table: " + String.join(".", parts));
    }
    final String name = parts.get(0);
    if (!LABEL_PATTERN.matcher(name).matches() || !allowedLabels.contains(name)) {
      throw new InvalidRequestException("SQL references an undeclared table: " + name);
    }
  }

  @Nonnull
  private static String joinIdentifier(@Nonnull final UnresolvedRelation relation) {
    return String.join(".", CollectionConverters.asJava(relation.multipartIdentifier()));
  }

  /**
   * Validates an expression. In both modes, explicitly rejected expression types and disallowed
   * function names are caught. In strict mode, the expression allow-list is additionally enforced
   * and {@link UnresolvedFunction}s are required to resolve to a built-in Spark function.
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

  /**
   * Validates an unresolved function reference from user SQL. Rejects known-dangerous names
   * (reflection-style functions) and any function whose Spark registry entry is not flagged as
   * built-in - that is, any UDF (Pathling-registered terminology and FHIRPath helpers, Hive UDFs,
   * Python/SQL/Java UDFs). Pathling UDFs belong in ViewDefinitions; user SQL should remain portable
   * and implementation-agnostic. Unknown function names are left alone here so that Spark's
   * analyser produces its standard "function not found" error, which is more helpful than a
   * synthetic rejection.
   */
  private void validateFunctionName(@Nonnull final UnresolvedFunction func) {
    final List<String> nameParts = CollectionConverters.asJava(func.nameParts());
    if (nameParts.isEmpty()) {
      return;
    }
    final String simpleName = nameParts.getLast().toLowerCase();
    if (REJECTED_FUNCTION_NAMES.contains(simpleName)) {
      throw new InvalidRequestException(
          "SQL contains a disallowed function: " + String.join(".", nameParts));
    }
    if (isNonBuiltInFunction(nameParts)) {
      throw new InvalidRequestException(
          "SQL contains a non-built-in function: "
              + String.join(".", nameParts)
              + ". Implementation-specific functions belong in ViewDefinitions, not in user SQL.");
    }
  }

  /**
   * Returns {@code true} if the name resolves in the Spark function registry to a non-built-in
   * function (any UDF kind: scala, hive, python, sql, java). Returns {@code false} when the name
   * does not resolve at all, so that Spark's standard "function not found" error is preserved.
   */
  private boolean isNonBuiltInFunction(@Nonnull final List<String> nameParts) {
    final FunctionIdentifier id = toFunctionIdentifier(nameParts);
    if (id == null) {
      return false;
    }
    final Option<ExpressionInfo> info;
    try {
      info = sparkSession.sessionState().functionRegistry().lookupFunction(id);
    } catch (final Exception e) {
      return false;
    }
    return info.isDefined() && !BUILTIN_FUNCTION_SOURCE.equals(info.get().getSource());
  }

  /**
   * Builds a Spark {@link FunctionIdentifier} from a 1- or 2-part name (function or
   * database.function). Three-or-more-part names (catalog-qualified) are not modelled by {@code
   * FunctionIdentifier}, so we return {@code null} and let Spark's analyser handle them.
   */
  @jakarta.annotation.Nullable
  private static FunctionIdentifier toFunctionIdentifier(@Nonnull final List<String> nameParts) {
    final int n = nameParts.size();
    if (n == 1) {
      return new FunctionIdentifier(nameParts.get(0));
    }
    if (n == 2) {
      return new FunctionIdentifier(nameParts.get(1), Option.apply(nameParts.get(0)));
    }
    return null;
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
}
