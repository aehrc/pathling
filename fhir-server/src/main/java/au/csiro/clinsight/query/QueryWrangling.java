/*
 * Copyright © Australian e-Health Research Centre, CSIRO. All rights reserved.
 */

package au.csiro.clinsight.query;

import static au.csiro.clinsight.query.parsing.Join.JoinType.EXISTS_JOIN;
import static au.csiro.clinsight.query.parsing.Join.JoinType.INLINE_QUERY;
import static au.csiro.clinsight.query.parsing.Join.JoinType.LATERAL_VIEW;
import static au.csiro.clinsight.query.parsing.Join.JoinType.TABLE_JOIN;
import static au.csiro.clinsight.utilities.Strings.tokenizePath;

import au.csiro.clinsight.query.parsing.Join;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/**
 * @author John Grimes
 */
public abstract class QueryWrangling {

  /**
   * Spark SQL does not currently allow a table join to follow a LATERAL VIEW join within a query -
   * the LATERAL VIEW statement must first be wrapped within a subquery. This method takes a set of
   * joins and wraps each of the LATERAL VIEW joins in a subquery, then returns a new set of joins.
   */
  public static SortedSet<Join> convertUpstreamLateralViewsToInlineQueries(SortedSet<Join> joins) {
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
        joins = replaceLateralViews(joins, downstreamTableJoin, lateralViewsToConvert);
      } else {
        if (cursor.getJoinType() == TABLE_JOIN || cursor.getJoinType() == EXISTS_JOIN) {
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
              joins = replaceLateralViews(joins, downstreamTableJoin, lateralViewsToConvert);
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
      SortedSet<Join> lateralViewsToConvert) {
    // First we search for invocations made against the alias of the last join in the group. This tells us which columns will need to be selected within the subquery.
    String finalTableAlias = lateralViewsToConvert.last().getTableAlias();
    Pattern tableAliasInvocationPattern = Pattern
        .compile("(?:ON|AND)\\s+" + finalTableAlias + "\\.(.*?)[\\s$]");
    Matcher tableAliasInvocationMatcher = tableAliasInvocationPattern
        .matcher(dependentTableJoin.getExpression());
    boolean found = tableAliasInvocationMatcher.find();

    // Build the join expression.
    String newTableAlias = finalTableAlias + "Exploded";
    Join firstLateralView = lateralViewsToConvert.first();
    Join upstreamJoin = firstLateralView.getDependsUpon();
    assert firstLateralView.getUdtfExpression() != null;
    String udtfExpression = firstLateralView.getRootExpression();
    firstLateralView.setExpression(firstLateralView.getExpression()
        .replace(firstLateralView.getUdtfExpression(), udtfExpression));
    firstLateralView.setUdtfExpression(udtfExpression);
    String table = tokenizePath(udtfExpression).getFirst();
    String joinConditionTarget = upstreamJoin == null ? table : upstreamJoin.getTableAlias();
    String joinExpression = "LEFT JOIN (SELECT * FROM " + table + " ";
    joinExpression += lateralViewsToConvert.stream()
        .map(Join::getExpression)
        .collect(Collectors.joining(" "));
    joinExpression +=
        ") " + newTableAlias + " ON " + joinConditionTarget + ".id = " + newTableAlias + ".id";
    String rootExpression = lateralViewsToConvert.last().getRootExpression();

    // Build a new Join object to replace the group of lateral views.
    Join inlineQuery = new Join(joinExpression, rootExpression, INLINE_QUERY,
        newTableAlias);
    inlineQuery.setDependsUpon(firstLateralView.getDependsUpon());
    dependentTableJoin.setDependsUpon(inlineQuery);
    // Change the expression within the dependent table join to point to the alias of the new join.
    String transformedExpression = dependentTableJoin.getExpression()
        .replaceAll("(?<=(ON|AND)\\s)" + lateralViewsToConvert.last().getTableAlias() + "\\.",
            inlineQuery.getTableAlias() + "." + finalTableAlias + ".");
    dependentTableJoin.setExpression(transformedExpression);

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
    newJoins.add(inlineQuery);
    lateralViewsToConvert.clear();
    return newJoins;
  }

}
