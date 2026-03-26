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
 * <p>Validation is performed on the unresolved logical plan produced by Spark's SQL parser. The
 * validator walks the entire plan tree and checks every plan node and expression against a
 * whitelist. Any node not in the whitelist causes the query to be rejected.
 *
 * <p>Three validation layers are applied:
 *
 * <ol>
 *   <li><strong>Plan node whitelist</strong> — only read-only plan nodes are permitted.
 *   <li><strong>Expression whitelist</strong> — only safe expression types are permitted.
 *   <li><strong>UDF allowlist</strong> — {@code ScalaUDF} is permitted only for Pathling's
 *       registered UDFs.
 * </ol>
 *
 * @see <a
 *     href="https://build.fhir.org/ig/FHIR/sql-on-fhir-v2/OperationDefinition-SQLQueryRun.html">SQLQueryRun</a>
 */
@Slf4j
@Component
public class SqlValidator {

  @Nonnull private final SparkSession sparkSession;

  // Allowed plan node class names (simple names). Any plan node not in this set is rejected.
  // All Command subclasses (DDL/DML) are additionally rejected via instanceof check.
  private static final Set<String> ALLOWED_PLAN_NODES =
      Set.of(
          // Basic operations.
          "Project",
          "Filter",
          "Sort",
          "GlobalLimit",
          "LocalLimit",
          "Tail",
          "Offset",
          "ReturnAnswer",
          // Set operations.
          "Union",
          "Intersect",
          "Except",
          // Joins.
          "Join",
          "LateralJoin",
          "AsOfJoin",
          // Aggregation and grouping.
          "Aggregate",
          "Expand",
          "Window",
          "WithWindowDefinition",
          // Table and relation access.
          "UnresolvedRelation",
          "SubqueryAlias",
          "LocalRelation",
          "OneRowRelation",
          "EmptyRelation",
          "Range",
          "View",
          "UnresolvedInlineTable",
          "UnresolvedTableValuedFunction",
          // Common table expressions.
          "WithCTE",
          "CTERelationDef",
          "CTERelationRef",
          "UnresolvedWith",
          // Deduplication.
          "Distinct",
          "Deduplicate",
          // Pivot and unpivot.
          "Pivot",
          "Unpivot",
          // Generate (LATERAL VIEW / EXPLODE).
          "Generate",
          // Hints.
          "ResolvedHint",
          "UnresolvedHint",
          // Sampling.
          "Sample",
          // Repartitioning (read-only).
          "Repartition",
          "RebalancePartitions",
          // Transposition.
          "Transpose",
          // Subquery wrapper.
          "Subquery",
          // Unresolved plan-specific nodes.
          "UnresolvedSubqueryColumnAliases");

  // Function names that are explicitly rejected because they enable arbitrary code execution.
  private static final Set<String> REJECTED_FUNCTION_NAMES =
      Set.of("reflect", "java_method", "try_reflect");

  // Pathling-registered UDF names that are allowed in ScalaUDF expressions.
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

  // Allowed expression class names for the unresolved plan. Expressions not in this set (and not
  // covered by special handling for UnresolvedFunction and ScalaUDF) are rejected.
  @SuppressWarnings("java:S1192")
  private static final Set<String> ALLOWED_EXPRESSION_NAMES =
      Set.of(
          // Literals and references.
          "Literal",
          "UnresolvedAttribute",
          "UnresolvedStar",
          "UnresolvedNamedLambdaVariable",
          "NamedLambdaVariable",
          "LambdaVariable",
          "Alias",
          "UnresolvedAlias",
          "OuterReference",
          "UnresolvedOrdinal",
          "VariableReference",
          "AttributeReference",
          "BoundReference",
          // Arithmetic.
          "Add",
          "Subtract",
          "Multiply",
          "Divide",
          "IntegralDivide",
          "Remainder",
          "Pmod",
          "UnaryMinus",
          "UnaryPositive",
          "Abs",
          // Comparison.
          "EqualTo",
          "EqualNullSafe",
          "LessThan",
          "LessThanOrEqual",
          "GreaterThan",
          "GreaterThanOrEqual",
          // Predicates.
          "In",
          "InSet",
          "InSubquery",
          "Between",
          "Like",
          "ILike",
          "RLike",
          "LikeAll",
          "LikeAny",
          "NotLikeAll",
          "NotLikeAny",
          "Contains",
          "StartsWith",
          "EndsWith",
          "StringInstr",
          "StringLocate",
          "FindInSet",
          // Logical operators.
          "And",
          "Or",
          "Not",
          // Null handling.
          "IsNull",
          "IsNotNull",
          "Coalesce",
          "NullIf",
          "Nvl",
          "Nvl2",
          "IfNull",
          "NaNvl",
          "IsNaN",
          // Conditional.
          "If",
          "CaseWhen",
          "Greatest",
          "Least",
          // Type casting.
          "Cast",
          "AnsiCast",
          "ToBinary",
          "TryToBinary",
          "ToNumber",
          "TryToNumber",
          "ToCharacter",
          // Sorting.
          "SortOrder",
          // Subquery expressions.
          "ScalarSubquery",
          "Exists",
          "ListQuery",
          "LateralSubquery",
          // Generator expressions.
          "Explode",
          "PosExplode",
          "Inline",
          "Stack",
          // Window expressions.
          "WindowExpression",
          "WindowSpecDefinition",
          "SpecifiedWindowFrame",
          "UnspecifiedFrame",
          // Struct, array, and map expressions.
          "CreateNamedStruct",
          "CreateArray",
          "CreateMap",
          "GetStructField",
          "GetArrayStructFields",
          "GetArrayItem",
          "GetMapValue",
          // Higher-order functions.
          "LambdaFunction",
          // Grouping.
          "Grouping",
          "GroupingID",
          // Unresolved expression types.
          "UnresolvedExtractValue",
          "UnresolvedRegex",
          "NamedArgumentExpression",
          // Non-deterministic (safe).
          "Rand",
          "Randn",
          "Uuid",
          "Shuffle",
          "RandStr",
          "Uniform",
          // Error handling.
          "RaiseError",
          "TryEval",
          "AssertTrue",
          // Utility.
          "Empty2Null",
          "TypeOf",
          // Bitwise operations.
          "BitwiseAnd",
          "BitwiseOr",
          "BitwiseXor",
          "BitwiseNot");

  // Expression types that are explicitly rejected because they enable unsafe operations.
  private static final Set<String> REJECTED_EXPRESSION_NAMES =
      Set.of(
          "CallMethodViaReflection",
          "TryReflect",
          "PythonUDF",
          "HiveSimpleUDF",
          "HiveGenericUDF",
          "HiveUDAF",
          "PrintToStderr",
          "StaticInvoke",
          "Invoke",
          "NewInstance");

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
   * Validates that the given SQL query is read-only and contains only allowed operations.
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
    walkPlan(plan);
  }

  /**
   * Recursively validates a logical plan node and all its children and expressions.
   *
   * @param plan the logical plan node to validate
   */
  private void walkPlan(@Nonnull final LogicalPlan plan) {
    validatePlanNode(plan);
    final List<Expression> expressions = CollectionConverters.asJava(plan.expressions());
    for (final Expression expr : expressions) {
      walkExpression(expr);
    }
    final List<LogicalPlan> children = CollectionConverters.asJava(plan.children());
    for (final LogicalPlan child : children) {
      walkPlan(child);
    }
  }

  /**
   * Recursively validates an expression and all its child expressions. If the expression is a
   * subquery expression, its inner plan is also validated.
   *
   * @param expr the expression to validate
   */
  private void walkExpression(@Nonnull final Expression expr) {
    validateExpression(expr);
    // Subquery expressions contain an inner logical plan that must also be validated.
    if (expr instanceof final SubqueryExpression subquery) {
      walkPlan(subquery.plan());
    }
    final List<Expression> children = CollectionConverters.asJava(expr.children());
    for (final Expression child : children) {
      walkExpression(child);
    }
  }

  /**
   * Validates that a plan node is in the allowed set.
   *
   * @param plan the plan node to validate
   * @throws InvalidRequestException if the plan node is not allowed
   */
  private void validatePlanNode(@Nonnull final LogicalPlan plan) {
    // All Command subclasses are rejected (DDL, DML, SHOW, DESCRIBE, etc.).
    if (plan instanceof Command) {
      throw new InvalidRequestException(
          "SQL contains a disallowed operation: " + plan.getClass().getSimpleName());
    }
    final String className = plan.getClass().getSimpleName();
    // Scala objects have a trailing '$' in their class name which must be stripped for matching.
    final String normalised =
        className.endsWith("$") ? className.substring(0, className.length() - 1) : className;
    if (!ALLOWED_PLAN_NODES.contains(normalised)) {
      throw new InvalidRequestException("SQL contains a disallowed plan node: " + className);
    }
  }

  /**
   * Validates that an expression is in the allowed set.
   *
   * @param expr the expression to validate
   * @throws InvalidRequestException if the expression is not allowed
   */
  private void validateExpression(@Nonnull final Expression expr) {
    final String className = expr.getClass().getSimpleName();

    // Check explicitly rejected types first.
    if (REJECTED_EXPRESSION_NAMES.contains(className)) {
      throw new InvalidRequestException("SQL contains a disallowed expression: " + className);
    }

    // Special handling for UnresolvedFunction: validate the function name.
    if (expr instanceof final UnresolvedFunction unresolvedFunc) {
      validateFunctionName(unresolvedFunc);
      return;
    }

    // Special handling for ScalaUDF: validate against UDF allowlist.
    if (expr instanceof final ScalaUDF scalaUdf) {
      validateUdf(scalaUdf);
      return;
    }

    // Check against the allowed expression names. Scala objects have a trailing '$' in their
    // class name which must be stripped for matching.
    final String normalised =
        className.endsWith("$") ? className.substring(0, className.length() - 1) : className;
    if (!ALLOWED_EXPRESSION_NAMES.contains(normalised)) {
      throw new InvalidRequestException("SQL contains a disallowed expression: " + className);
    }
  }

  /**
   * Validates that an unresolved function is not a known-dangerous function.
   *
   * @param func the unresolved function to validate
   * @throws InvalidRequestException if the function is dangerous
   */
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

  /**
   * Validates that a ScalaUDF is in the Pathling UDF allowlist.
   *
   * @param udf the ScalaUDF to validate
   * @throws InvalidRequestException if the UDF is not in the allowlist
   */
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
}
