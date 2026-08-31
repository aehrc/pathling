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
import org.apache.spark.sql.catalyst.plans.logical.Sample;
import org.apache.spark.sql.catalyst.plans.logical.SubqueryAlias;
import org.apache.spark.sql.catalyst.trees.Origin;
import org.apache.spark.sql.execution.command.DescribeQueryCommand;
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
 * continues to resolve.
 *
 * @author John Grimes
 */
public final class SqlLabelRewriter {

  /** Sentinel meaning that no relation-primary wrapper is in effect. */
  private static final int NO_WRAPPER = -1;

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

    final List<SqlEdit> edits = new ArrayList<>();
    final Set<LogicalPlan> visited = Collections.newSetFromMap(new IdentityHashMap<>());
    collectEdits(sql, plan, labelToViewName, false, NO_WRAPPER, visited, edits);
    return applyEdits(sql, edits);
  }

  /**
   * Walks the parsed plan, collecting one edit per relation reference that names a label.
   *
   * @param sql the original SQL query
   * @param plan the plan node to visit
   * @param labelToViewName the mapping from dependency labels to temporary view names
   * @param aliased whether an alias has already been seen between this node and the relation
   *     primary it belongs to
   * @param wrapperStop the last character offset of the outermost relation-primary wrapper seen so
   *     far, or {@link #NO_WRAPPER} when there is none
   * @param visited the plan nodes already visited, compared by identity because a subquery plan is
   *     reachable both as an inner child and through its subquery expression
   * @param edits the edits collected so far
   */
  private static void collectEdits(
      @Nonnull final String sql,
      @Nonnull final LogicalPlan plan,
      @Nonnull final Map<String, String> labelToViewName,
      final boolean aliased,
      final int wrapperStop,
      @Nonnull final Set<LogicalPlan> visited,
      @Nonnull final List<SqlEdit> edits) {

    if (!visited.add(plan)) {
      return;
    }

    if (plan instanceof final UnresolvedRelation relation) {
      collectRelationEdit(sql, relation, labelToViewName, aliased, wrapperStop, edits);
    } else if (plan instanceof final UnresolvedTableOrView target) {
      collectDescribeTargetEdit(target, labelToViewName, edits);
    }

    // A subquery expression holds its plan outside the tree's children.
    for (final Expression expression : CollectionConverters.asJava(plan.expressions())) {
      collectEditsInExpression(sql, expression, labelToViewName, visited, edits);
    }

    // DESCRIBE QUERY holds the query it describes as a constructor argument, so neither the child
    // walk nor the inner-child walk below reaches it.
    if (plan instanceof final DescribeQueryCommand describeQuery) {
      collectEdits(sql, describeQuery.plan(), labelToViewName, false, NO_WRAPPER, visited, edits);
    }

    // The definition bodies of a WITH clause are inner children rather than children, so the
    // generic child walk never reaches them.
    for (final Object innerChild : CollectionConverters.asJava(plan.innerChildren())) {
      if (innerChild instanceof final LogicalPlan innerPlan) {
        collectEdits(sql, innerPlan, labelToViewName, false, NO_WRAPPER, visited, edits);
      }
    }

    final boolean childAliased = isAliasWrapper(plan) || (plan instanceof Sample && aliased);
    final int childWrapperStop = childWrapperStop(plan, wrapperStop);
    for (final LogicalPlan child : CollectionConverters.asJava(plan.children())) {
      collectEdits(sql, child, labelToViewName, childAliased, childWrapperStop, visited, edits);
    }
  }

  /** Recurses through expression trees to reach the plans held by subquery expressions. */
  private static void collectEditsInExpression(
      @Nonnull final String sql,
      @Nonnull final Expression expression,
      @Nonnull final Map<String, String> labelToViewName,
      @Nonnull final Set<LogicalPlan> visited,
      @Nonnull final List<SqlEdit> edits) {

    if (expression instanceof final SubqueryExpression subquery) {
      collectEdits(sql, subquery.plan(), labelToViewName, false, NO_WRAPPER, visited, edits);
    }
    for (final Expression child : CollectionConverters.asJava(expression.children())) {
      collectEditsInExpression(sql, child, labelToViewName, visited, edits);
    }
  }

  /**
   * Collects the edit for a relation reference. A reference whose identifier is not a single-part
   * label is left alone; static validation has already rejected anything that could not have been a
   * declared label.
   */
  private static void collectRelationEdit(
      @Nonnull final String sql,
      @Nonnull final UnresolvedRelation relation,
      @Nonnull final Map<String, String> labelToViewName,
      final boolean aliased,
      final int wrapperStop,
      @Nonnull final List<SqlEdit> edits) {

    final String label = singlePartLabel(relation.multipartIdentifier(), labelToViewName);
    if (label == null) {
      return;
    }
    final String viewName = labelToViewName.get(label);
    final int start = spanStart(relation.origin(), label);
    final int stop = spanStop(relation.origin(), label);

    if (aliased) {
      // The reference already carries an alias, so only the identifier is replaced. Standard SQL
      // scoping makes the original name unavailable as a qualifier once an alias is present.
      edits.add(new SqlEdit(start, stop + 1, viewName));
      return;
    }
    // The alias must follow any relation-primary clause the grammar orders before it, which among
    // the constructs validation permits is the TABLESAMPLE clause.
    final int end = Math.max(stop, wrapperStop);
    edits.add(
        new SqlEdit(start, end + 1, viewName + sql.substring(stop + 1, end + 1) + " AS " + label));
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

  /**
   * Returns the wrapper extent to pass to a node's children. A {@code Sample} node carries the span
   * of the TABLESAMPLE clause, which an injected alias must follow; any other node ends the
   * relation primary, so the extent resets.
   */
  private static int childWrapperStop(@Nonnull final LogicalPlan plan, final int wrapperStop) {
    if (plan instanceof final Sample sample) {
      final Origin origin = sample.origin();
      if (origin.stopIndex().isDefined()) {
        return Math.max(wrapperStop, (Integer) origin.stopIndex().get());
      }
      return wrapperStop;
    }
    if (isAliasWrapper(plan)) {
      return wrapperStop;
    }
    return NO_WRAPPER;
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
