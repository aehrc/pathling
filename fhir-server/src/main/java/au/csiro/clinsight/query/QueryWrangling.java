/*
 * Copyright © Australian e-Health Research Centre, CSIRO. All rights reserved.
 */

package au.csiro.clinsight.query;

import static au.csiro.clinsight.query.parsing.Join.JoinType.LATERAL_VIEW;
import static au.csiro.clinsight.query.parsing.Join.JoinType.MEMBERSHIP_JOIN;
import static au.csiro.clinsight.query.parsing.Join.JoinType.TABLE_JOIN;
import static au.csiro.clinsight.query.parsing.Join.JoinType.WRAPPED_LATERAL_VIEWS;

import au.csiro.clinsight.query.parsing.Join;
import java.util.Collections;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.stream.Collectors;

/**
 * @author John Grimes
 */
public abstract class QueryWrangling {

  /**
   * Spark SQL does not currently allow a table join to follow a LATERAL VIEW join within a query -
   * the LATERAL VIEW statement must first be wrapped within a subquery. This method takes a set of
   * joins and wraps each of the LATERAL VIEW joins in a subquery, then returns a new set of joins.
   * The `targetAlias` argument is the target of the first join in the set.
   */
  public static SortedSet<Join> convertUpstreamLateralViewsToInlineQueries(SortedSet<Join> joins,
      String targetAlias) {
    if (joins.isEmpty()) {
      return joins;
    }
    // We start with the final join and move backwards.
    int cursorIndex = joins.size() - 1;
    @SuppressWarnings("ConstantConditions") Join cursor =
        cursorIndex >= 0 ? (Join) joins.toArray()[cursorIndex] : null;
    Join downstreamTableJoin = null;
    SortedSet<Join> lateralViewsToConvert = new TreeSet<>();
    // We stop when we reach the end of the dependencies and there are no lateral views queued up
    // for conversion.
    while (cursor != null || !lateralViewsToConvert.isEmpty()) {
      if (cursor == null) {
        // Upon reaching the start of the joins, we bundle up any lateral views queued up for
        // conversion.
        assert downstreamTableJoin != null;
        joins = replaceLateralViews(joins, downstreamTableJoin, lateralViewsToConvert, targetAlias);
      } else {
        if (cursor.getJoinType() == TABLE_JOIN || cursor.getJoinType() == MEMBERSHIP_JOIN) {
          if (downstreamTableJoin == null) {
            // We mark a table join that depends upon a lateral view (or set of lateral views) as
            // the "dependent table join". It stays marked until we reach the end of the lateral
            // views that it depends upon and finish converting them.
            downstreamTableJoin = cursor;
          } else {
            // If we reach a new table join that is not the "dependent table join", then that must
            // mean that we have reached the end of this contiguous set of lateral views. This is
            // the trigger to take these and convert them into an inline query.
            if (!lateralViewsToConvert.isEmpty()) {
              joins = replaceLateralViews(joins, downstreamTableJoin, lateralViewsToConvert,
                  targetAlias);
              downstreamTableJoin = null;
              continue;
            }
          }
        } else if (cursor.getJoinType() == LATERAL_VIEW && downstreamTableJoin != null) {
          // If we find a new lateral view, we add that to the set of lateral views that are queued
          // for conversion.
          lateralViewsToConvert.add(cursor);
        }
        // Each iteration of the loop, we move to the previous join in the sorted set.
        cursorIndex--;
        cursor = cursorIndex >= 0 ? (Join) joins.toArray()[cursorIndex] : null;
      }
    }
    return joins;
  }

  /**
   * This method takes a set of LATERAL VIEW joins and converts them into a single join with an
   * inline query.
   */
  public static SortedSet<Join> replaceLateralViews(SortedSet<Join> joins, Join dependentTableJoin,
      SortedSet<Join> lateralViewsToConvert, String targetAlias) {
    // First we search for invocations made against the alias of the last join in the group. This
    // tells us which columns will need to be selected within the subquery.
    Join firstLateralView = lateralViewsToConvert.first();
    Join lastLateralView = lateralViewsToConvert.last();
    String finalTableAlias = lastLateralView.getTableAlias();

    // Create a new set of joins that includes any dependencies of the lateral views.
    SortedSet<Join> includingDependencies = new TreeSet<>(lateralViewsToConvert);
    for (Join join : lateralViewsToConvert) {
      if (join.getDependsUpon() != null) {
        includingDependencies.add(join.getDependsUpon());
      }
    }

    // Rewrite the expressions of the joins to take account of aliases.
    for (Join join : includingDependencies) {
      String newSql = rewriteJoinWithJoinAliases(join, includingDependencies);
      join.setSql(newSql);
    }

    // Build the join expression.
    String joinAlias = lastLateralView.getTableAlias();
    String joinExpression =
        "LEFT JOIN (SELECT " + targetAlias + ".*, " + joinAlias + ".* FROM " + targetAlias + " ";
    joinExpression += includingDependencies.stream()
        .map(Join::getSql)
        .collect(Collectors.joining(" "));
    joinExpression += ") " + joinAlias + " ON " + targetAlias + ".id = " + joinAlias + ".id";

    // Build a new Join object to replace the group of lateral views.
    Join newJoin = new Join();
    newJoin.setSql(joinExpression);
    newJoin.setJoinType(WRAPPED_LATERAL_VIEWS);
    newJoin.setTableAlias(joinAlias);
    newJoin.setAliasTarget(lastLateralView.getAliasTarget());
    newJoin.setTargetElement(lastLateralView.getTargetElement());
    newJoin.setDependsUpon(firstLateralView.getDependsUpon());
    dependentTableJoin.setDependsUpon(newJoin);

    // Change the expression within the dependent table join to point to the alias of the new join.
    String transformedExpression = dependentTableJoin.getSql()
        .replaceAll("(?<=(ON|AND)\\s)" + lastLateralView.getTableAlias() + "\\.",
            newJoin.getTableAlias() + "." + finalTableAlias + ".");
    dependentTableJoin.setSql(transformedExpression);

    // This piece of code exists due to some strange behaviour in the removal of items from a set
    // that is not yet understood. Ideally this would be as simple as
    // `joins.removeAll(lateralViewsToConvert)`, but this does not seem to remove all of the lateral
    // views in all cases (see the `anyReferenceTraversal` test case).
    SortedSet<Join> newJoins = new TreeSet<>();
    for (Join join : joins) {
      boolean inLateralViewsToConvert = false;
      for (Join toConvert : lateralViewsToConvert) {
        if (join.equals(toConvert)) {
          inLateralViewsToConvert = true;
        }
      }
      if (!inLateralViewsToConvert) {
        newJoins.add(join);
      }
    }
    newJoins.add(newJoin);
    lateralViewsToConvert.clear();
    return newJoins;
  }

  public static String rewriteSqlWithJoinAliases(String sql, SortedSet<Join> joins) {
    if (joins.isEmpty()) {
      return sql;
    }
    String newSql = sql;

    // Go through the list and replace the alias target string within the expression with the alias,
    // for each join.
    SortedSet<Join> joinsReversed = new TreeSet<>(Collections.reverseOrder());
    joinsReversed.addAll(joins);
    for (Join currentJoin : joinsReversed) {
      newSql = newSql.replaceAll(currentJoin.getAliasTarget(), currentJoin.getTableAlias());
    }

    return newSql;
  }

  public static String rewriteJoinWithJoinAliases(Join join, SortedSet<Join> joins) {
    String newSql = join.getSql();

    // Don't replace aliases in join expressions which contain inline queries (i.e. LEFT JOIN).
    if (newSql.contains("LEFT JOIN")) {
      return newSql;
    }

    // Go through the list and replace the alias target string within the expression with the alias,
    // for each join.
    SortedSet<Join> joinsReversed = new TreeSet<>(Collections.reverseOrder());
    joinsReversed.addAll(joins);
    for (Join currentJoin : joinsReversed) {
      if (!join.equals(currentJoin)) {
        // Match the alias target, unless it is inside an inline query.
        newSql = newSql.replaceAll(currentJoin.getAliasTarget(), currentJoin.getTableAlias());
      }
    }

    return newSql;
  }

}
