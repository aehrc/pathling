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
import jakarta.annotation.Nullable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.spark.sql.catalyst.analysis.UnresolvedRelation;
import org.apache.spark.sql.catalyst.analysis.UnresolvedSubqueryColumnAliases;
import org.apache.spark.sql.catalyst.analysis.UnresolvedTableOrView;
import org.apache.spark.sql.catalyst.expressions.Expression;
import org.apache.spark.sql.catalyst.expressions.SubqueryExpression;
import org.apache.spark.sql.catalyst.parser.ParserInterface;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.catalyst.plans.logical.SubqueryAlias;
import org.apache.spark.sql.catalyst.plans.logical.UnresolvedWith;
import org.apache.spark.sql.catalyst.trees.Origin;
import org.apache.spark.sql.execution.command.DescribeQueryCommand;
import scala.Tuple2;
import scala.collection.immutable.Seq;
import scala.jdk.javaapi.CollectionConverters;

/**
 * Substitutes dependency table labels within a SQL query with the request-scoped temporary view
 * names the dependencies are materialised under. The substitution is parse-guided: the SQL is
 * parsed and only the character spans of relation references are rewritten, so column references,
 * aliases, function names, string literals and comments cannot be touched.
 *
 * <p>Where a substituted relation reference carries no alias of its own, {@code AS <label>} is
 * injected after it. The rewritten query then behaves as if a real table existed under the label's
 * own name, so a column qualified by the label (for example {@code SELECT age.age FROM age})
 * continues to resolve. Where the alias goes, and whether the grammar admits one at all, is decided
 * from the query's tokens by {@code SqlSource.aliasInsertionPoint}. The target of a {@code
 * DESCRIBE} never takes one.
 *
 * <p>A reference is left alone when a common table expression of the same name is in scope: the
 * query author's definition shadows the label, per standard SQL scoping. Scope follows Spark's own
 * {@code CTESubstitution}, and the names are compared case-sensitively, exactly as the labels are.
 *
 * @author John Grimes
 */
public final class SqlLabelRewriter {

  private SqlLabelRewriter() {
    // This class is not intended to be instantiated.
  }

  /**
   * Rewrites the table references within the supplied SQL that name a dependency label, replacing
   * each with the temporary view name the dependency is registered under.
   *
   * @param parser the SQL parser used to locate relation references
   * @param sql the original SQL query
   * @param labelToViewName the mapping from dependency labels to temporary view names
   * @return the rewritten SQL query
   * @throws InvalidRequestException if the SQL cannot be parsed
   */
  @Nonnull
  public static String rewrite(
      @Nonnull final ParserInterface parser,
      @Nonnull final String sql,
      @Nonnull final Map<String, String> labelToViewName) {

    if (labelToViewName.isEmpty()) {
      return sql;
    }

    final LogicalPlan plan;
    try {
      plan = parser.parsePlan(sql);
    } catch (final Exception e) {
      // Static validation parses the same text first, so in the server pipeline this is
      // unreachable; direct callers still get the invalid-syntax error the validator would raise.
      throw new InvalidRequestException("Invalid SQL syntax: " + e.getMessage());
    }

    final SqlSource source = new SqlSource(sql);
    final List<SqlEdit> edits = new ArrayList<>();
    final Set<LogicalPlan> visited = Collections.newSetFromMap(new IdentityHashMap<>());
    collectEdits(source, plan, labelToViewName, Set.of(), false, visited, edits);
    return applyEdits(sql, edits);
  }

  /**
   * Walks the parsed plan, collecting one edit per relation reference that names a label.
   *
   * @param sql the original SQL query, and the token view over it
   * @param plan the plan node to visit
   * @param labelToViewName the mapping from dependency labels to temporary view names
   * @param cteScope the names of the common table expressions in scope at this node, each of which
   *     shadows a label of the same name
   * @param aliased whether an alias has already been seen between this node and the relation
   *     primary it belongs to
   * @param visited the plan nodes already visited, compared by identity because a subquery plan is
   *     reachable both as an inner child and through its subquery expression
   * @param edits the edits collected so far
   */
  private static void collectEdits(
      @Nonnull final SqlSource sql,
      @Nonnull final LogicalPlan plan,
      @Nonnull final Map<String, String> labelToViewName,
      @Nonnull final Set<String> cteScope,
      final boolean aliased,
      @Nonnull final Set<LogicalPlan> visited,
      @Nonnull final List<SqlEdit> edits) {

    if (!visited.add(plan)) {
      return;
    }

    if (plan instanceof final UnresolvedRelation relation) {
      collectRelationEdit(sql, relation, labelToViewName, cteScope, aliased, edits);
    } else if (plan instanceof final UnresolvedTableOrView target) {
      collectDescribeTargetEdit(target, labelToViewName, edits);
    }

    // A subquery expression holds its plan outside the tree's children.
    for (final Expression expression : CollectionConverters.asJava(plan.expressions())) {
      collectEditsInExpression(sql, expression, labelToViewName, cteScope, visited, edits);
    }

    // DESCRIBE QUERY holds the query it describes as a constructor argument, so neither the child
    // walk nor the inner-child walk below reaches it.
    if (plan instanceof final DescribeQueryCommand describeQuery) {
      collectEdits(sql, describeQuery.plan(), labelToViewName, cteScope, false, visited, edits);
    }

    // A WITH clause gives each of its subtrees a different scope, so it is walked explicitly
    // instead of through the generic inner-child and child walks below.
    if (plan instanceof final UnresolvedWith with) {
      collectEditsInWith(sql, with, labelToViewName, cteScope, visited, edits);
      return;
    }

    // Any plan a node holds outside its children is exposed as an inner child. The scope is
    // carried into the descent so that a plan first reached this way keeps it.
    for (final Object innerChild : CollectionConverters.asJava(plan.innerChildren())) {
      if (innerChild instanceof final LogicalPlan innerPlan) {
        collectEdits(sql, innerPlan, labelToViewName, cteScope, false, visited, edits);
      }
    }

    // The parser wraps an aliased relation in its alias wrapper before any sample clause, so the
    // wrapper is always the relation's immediate parent and the flag never has to survive another
    // node. Every other node ends the relation primary, so the flag resets.
    final boolean childAliased = isAliasWrapper(plan);
    for (final LogicalPlan child : CollectionConverters.asJava(plan.children())) {
      collectEdits(sql, child, labelToViewName, cteScope, childAliased, visited, edits);
    }
  }

  /** Recurses through expression trees to reach the plans held by subquery expressions. */
  private static void collectEditsInExpression(
      @Nonnull final SqlSource sql,
      @Nonnull final Expression expression,
      @Nonnull final Map<String, String> labelToViewName,
      @Nonnull final Set<String> cteScope,
      @Nonnull final Set<LogicalPlan> visited,
      @Nonnull final List<SqlEdit> edits) {

    if (expression instanceof final SubqueryExpression subquery) {
      collectEdits(sql, subquery.plan(), labelToViewName, cteScope, false, visited, edits);
    }
    for (final Expression child : CollectionConverters.asJava(expression.children())) {
      collectEditsInExpression(sql, child, labelToViewName, cteScope, visited, edits);
    }
  }

  /**
   * Walks the definitions and the main query of a {@code WITH} clause, mirroring the scoping of
   * Spark's own {@code CTESubstitution}: the i-th definition sees the names in scope around the
   * clause plus the definitions declared before it, and the main query sees every definition. A
   * definition referencing its own name therefore resolves outward to a label of that name, because
   * a {@code WITH} without {@code RECURSIVE} cannot refer to itself.
   *
   * <p>The definitions are taken from {@code cteRelations} rather than {@code innerChildren}
   * because only the former pairs each body with the name it is bound to. The scope is accumulated
   * in place, each descent completing before the next name is added.
   */
  private static void collectEditsInWith(
      @Nonnull final SqlSource sql,
      @Nonnull final UnresolvedWith with,
      @Nonnull final Map<String, String> labelToViewName,
      @Nonnull final Set<String> cteScope,
      @Nonnull final Set<LogicalPlan> visited,
      @Nonnull final List<SqlEdit> edits) {

    final List<Tuple2<String, SubqueryAlias>> ctes =
        CollectionConverters.asJava(with.cteRelations());
    final Set<String> scope = new HashSet<>(cteScope);
    for (final Tuple2<String, SubqueryAlias> cte : ctes) {
      if (with.allowRecursion()) {
        // A RECURSIVE clause puts a definition's own name in scope for its body, so a
        // self-reference there is the recursion rather than a label of the same name.
        scope.add(cte._1());
      }
      collectEdits(sql, cte._2(), labelToViewName, scope, false, visited, edits);
      scope.add(cte._1());
    }
    collectEdits(sql, with.child(), labelToViewName, scope, false, visited, edits);
  }

  /**
   * Collects the edit for a relation reference. A reference whose identifier is not a single-part
   * label, or which names a common table expression in scope, is left alone; static validation has
   * already rejected anything that could not have been a declared label.
   */
  private static void collectRelationEdit(
      @Nonnull final SqlSource sql,
      @Nonnull final UnresolvedRelation relation,
      @Nonnull final Map<String, String> labelToViewName,
      @Nonnull final Set<String> cteScope,
      final boolean aliased,
      @Nonnull final List<SqlEdit> edits) {

    final String label = singlePartLabel(relation.multipartIdentifier(), labelToViewName);
    if (label == null || cteScope.contains(label)) {
      return;
    }
    final String viewName = labelToViewName.get(label);
    final int start = spanStart(relation.origin(), label);
    final int stop = spanStop(relation.origin(), label);

    final int insertion = aliased ? SqlSource.NO_ALIAS : sql.aliasInsertionPoint(start, stop);
    if (insertion == SqlSource.NO_ALIAS) {
      // Only the identifier is replaced, because no alias can be added here. Either the reference
      // already carries one, in which case standard SQL scoping makes the original name unavailable
      // as a qualifier anyway, or the grammar offers no alias slot at all.
      edits.add(new SqlEdit(start, stop + 1, viewName));
      return;
    }
    // Any options and sample clauses between the identifier and the alias slot are carried over
    // unchanged, because the grammar orders the alias after them.
    edits.add(
        new SqlEdit(
            start, insertion, viewName + sql.substring(stop + 1, insertion) + " AS " + label));
  }

  /**
   * Collects the edit for the target of a {@code DESCRIBE [TABLE] <label>} statement. No alias is
   * injected: the grammar does not permit one there, and there is nothing to qualify.
   */
  private static void collectDescribeTargetEdit(
      @Nonnull final UnresolvedTableOrView target,
      @Nonnull final Map<String, String> labelToViewName,
      @Nonnull final List<SqlEdit> edits) {

    final String label = singlePartLabel(target.multipartIdentifier(), labelToViewName);
    if (label == null) {
      return;
    }
    edits.add(
        new SqlEdit(
            spanStart(target.origin(), label),
            spanStop(target.origin(), label) + 1,
            labelToViewName.get(label)));
  }

  /**
   * Returns the label named by a relation identifier that is a single part matching a declared
   * label, or null when the identifier is multi-part or names no label. Matching is case-sensitive,
   * as it is in the validator.
   */
  @Nullable
  private static String singlePartLabel(
      @Nonnull final Seq<String> multipartIdentifier,
      @Nonnull final Map<String, String> labelToViewName) {

    final List<String> parts = CollectionConverters.asJava(multipartIdentifier);
    if (parts.size() != 1 || !labelToViewName.containsKey(parts.get(0))) {
      return null;
    }
    return parts.get(0);
  }

  /** Returns true when the node is one of the wrappers the parser builds for a relation alias. */
  private static boolean isAliasWrapper(@Nonnull final LogicalPlan plan) {
    return plan instanceof SubqueryAlias || plan instanceof UnresolvedSubqueryColumnAliases;
  }

  /** Returns the offset of the first character of a matched node's span. */
  private static int spanStart(@Nonnull final Origin origin, @Nonnull final String label) {
    if (origin.startIndex().isEmpty()) {
      throw new IllegalStateException(
          "Parsed relation reference for label '" + label + "' carries no start position");
    }
    return (Integer) origin.startIndex().get();
  }

  /** Returns the offset of the last character of a matched node's span. */
  private static int spanStop(@Nonnull final Origin origin, @Nonnull final String label) {
    if (origin.stopIndex().isEmpty()) {
      throw new IllegalStateException(
          "Parsed relation reference for label '" + label + "' carries no end position");
    }
    return (Integer) origin.stopIndex().get();
  }

  /** Applies the collected edits to the original text in order, rejecting any overlap. */
  @Nonnull
  private static String applyEdits(@Nonnull final String sql, @Nonnull final List<SqlEdit> edits) {

    edits.sort(Comparator.comparingInt(SqlEdit::getStart));
    final StringBuilder out = new StringBuilder(sql.length());
    int cursor = 0;
    for (final SqlEdit edit : edits) {
      if (edit.getStart() < cursor) {
        // Relation references occupy disjoint spans of the query text, so an overlap here is an
        // internal invariant violation rather than anything a client could provoke.
        throw new IllegalStateException(
            "Overlapping label substitutions at offset " + edit.getStart());
      }
      out.append(sql, cursor, edit.getStart()).append(edit.getReplacement());
      cursor = edit.getEnd();
    }
    out.append(sql, cursor, sql.length());
    return out.toString();
  }
}
