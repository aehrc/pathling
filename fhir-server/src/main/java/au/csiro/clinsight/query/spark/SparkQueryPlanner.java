/*
 * Copyright © Australian e-Health Research Centre, CSIRO. All rights reserved.
 */

package au.csiro.clinsight.query.spark;

import static au.csiro.clinsight.utilities.Strings.tokenizePath;

import au.csiro.clinsight.TerminologyClient;
import au.csiro.clinsight.fhir.ResolvedElement.ResolvedElementType;
import au.csiro.clinsight.query.AggregateQuery;
import au.csiro.clinsight.query.AggregateQuery.Aggregation;
import au.csiro.clinsight.query.AggregateQuery.Grouping;
import au.csiro.clinsight.query.spark.Join.JoinType;
import ca.uhn.fhir.rest.server.exceptions.InvalidRequestException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import org.apache.spark.sql.SparkSession;

/**
 * This class knows how to take an AggregateQuery and convert it into an object which contains all
 * the information needed to execute the query as SQL against a Spark data warehouse.
 *
 * @author John Grimes
 */
class SparkQueryPlanner {

  private final TerminologyClient terminologyClient;
  private final SparkSession spark;
  private final List<ParseResult> aggregationParseResults;
  private final List<ParseResult> groupingParseResults;

  SparkQueryPlanner(@Nonnull TerminologyClient terminologyClient,
      @Nonnull SparkSession spark, @Nonnull AggregateQuery query) {
    this.terminologyClient = terminologyClient;
    this.spark = spark;
    List<Aggregation> aggregations = query.getAggregations();
    List<Grouping> groupings = query.getGroupings();
    if (aggregations == null || aggregations.isEmpty()) {
      throw new InvalidRequestException("Missing aggregation component within query");
    }

    aggregationParseResults = parseAggregation(aggregations);
    groupingParseResults = parseGroupings(groupings);
  }

  private List<ParseResult> parseAggregation(@Nonnull List<Aggregation> aggregations) {
    return aggregations.stream()
        .map(aggregation -> {
          String aggExpression = aggregation.getExpression();
          if (aggExpression == null) {
            throw new InvalidRequestException("Aggregation component must have expression");
          }
          ExpressionParser aggregationParser = new ExpressionParser(terminologyClient, spark);
          return aggregationParser.parse(aggExpression);
        }).collect(Collectors.toList());
  }

  private List<ParseResult> parseGroupings(List<Grouping> groupings) {
    List<ParseResult> groupingParseResults = new ArrayList<>();
    if (groupings != null) {
      groupingParseResults = groupings.stream()
          .map(grouping -> {
            String groupingExpression = grouping.getExpression();
            if (groupingExpression == null) {
              throw new InvalidRequestException("Grouping component must have expression");
            }
            ExpressionParser groupingParser = new ExpressionParser(terminologyClient, spark);
            ParseResult result = groupingParser.parse(groupingExpression);
            if (result.getElementType() != ResolvedElementType.PRIMITIVE) {
              throw new InvalidRequestException(
                  "Grouping expression is not of primitive type: " + groupingExpression + " ("
                      + result.getElementTypeCode() + ")");
            }
            return result;
          }).collect(Collectors.toList());
    }
    return groupingParseResults;
  }

  QueryPlan buildQueryPlan() {
    QueryPlan queryPlan = new QueryPlan();

    // Get aggregation expressions from the parse results.
    List<String> aggregations = aggregationParseResults.stream()
        .map(ParseResult::getSqlExpression)
        .collect(Collectors.toList());
    queryPlan.setAggregations(aggregations);

    // Get aggregation data types from the parse results.
    List<String> aggregationTypes = aggregationParseResults.stream()
        .map(ParseResult::getElementTypeCode)
        .collect(Collectors.toList());
    queryPlan.setAggregationTypes(aggregationTypes);

    // Get grouping expressions from the parse results.
    List<String> groupings = groupingParseResults.stream()
        .map(ParseResult::getSqlExpression)
        .collect(Collectors.toList());
    queryPlan.setGroupings(groupings);

    // Get grouping data types from the parse results.
    List<String> groupingTypes = groupingParseResults.stream()
        .map(ParseResult::getElementTypeCode)
        .collect(Collectors.toList());
    queryPlan.setGroupingTypes(groupingTypes);

    // Get from tables from the results of parsing both aggregations and groupings, and compute the
    // union.
    Set<String> aggregationFromTables = new HashSet<>();
    aggregationParseResults
        .forEach(parseResult -> aggregationFromTables.addAll(parseResult.getFromTable()));
    Set<String> groupingFromTables = new HashSet<>();
    groupingParseResults
        .forEach(parseResult -> groupingFromTables.addAll(parseResult.getFromTable()));
    // Check for from tables within the groupings that were not referenced within at least one
    // aggregation expression.
    if (!aggregationFromTables.containsAll(groupingFromTables)) {
      Set<String> difference = new HashSet<>(groupingFromTables);
      difference.removeAll(aggregationFromTables);
      throw new InvalidRequestException(
          "Groupings contain one or more resources that are not the subject of an aggregation: "
              + String.join(", ", difference));
    }
    queryPlan.setFromTables(aggregationFromTables);

    // Get joins from the results of parsing both aggregations and groupings.
    SortedSet<Join> joins = new TreeSet<>();
    for (ParseResult parseResult : aggregationParseResults) {
      joins.addAll(parseResult.getJoins());
    }
    for (ParseResult parseResult : groupingParseResults) {
      joins.addAll(parseResult.getJoins());
    }
    SortedSet<Join> convertedJoins = convertUpstreamLateralViewsToInlineQueries(joins);
    queryPlan.setJoins(convertedJoins);

    return queryPlan;
  }

  private SortedSet<Join> convertUpstreamLateralViewsToInlineQueries(SortedSet<Join> joins) {
    if (joins.isEmpty()) {
      return joins;
    }
    Join cursor = joins.last();
    Join dependentTableJoin = null;
    SortedSet<Join> lateralViewsToConvert = new TreeSet<>();
    while (cursor != null || !lateralViewsToConvert.isEmpty()) {
      if (cursor == null) {
        joins = replaceLateralViews(joins, dependentTableJoin, lateralViewsToConvert);
      } else {
        if (cursor.getJoinType() == JoinType.TABLE_JOIN) {
          if (dependentTableJoin == null) {
            dependentTableJoin = cursor;
          } else {
            joins = replaceLateralViews(joins, dependentTableJoin, lateralViewsToConvert);
            dependentTableJoin = null;
            continue;
          }
        } else if (cursor.getJoinType() == JoinType.LATERAL_VIEW && dependentTableJoin != null) {
          lateralViewsToConvert.add(cursor);
        }
        cursor = cursor.getDependsUpon();
      }
    }
    return joins;
  }

  private SortedSet<Join> replaceLateralViews(SortedSet<Join> joins, Join dependentTableJoin,
      SortedSet<Join> lateralViewsToConvert) {
    // Package up lateral views into an inline query and replace them within the dependency tree.
    String finalTableAlias = lateralViewsToConvert.last().getTableAlias();
    Pattern tableAliasInvocationPattern = Pattern
        .compile("(?:ON|AND)\\s+" + finalTableAlias + "\\.(.*?)[\\s$]");
    Matcher tableAliasInvocationMatcher = tableAliasInvocationPattern
        .matcher(dependentTableJoin.getExpression());
    boolean found = tableAliasInvocationMatcher.find();
    List<String> tableAliasInvocations = new ArrayList<>();
    while (found) {
      tableAliasInvocations.add(tableAliasInvocationMatcher.group(1));
      found = tableAliasInvocationMatcher.find();
    }
    assert !tableAliasInvocations.isEmpty();
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
    String selectFields = tableAliasInvocations.stream()
        .map(tai -> finalTableAlias + "." + tai)
        .collect(Collectors.joining(", "));
    String joinExpression = "INNER JOIN (SELECT id, " + selectFields + " FROM " + table + " ";
    joinExpression += lateralViewsToConvert.stream()
        .map(Join::getExpression)
        .collect(Collectors.joining(" "));
    joinExpression +=
        ") " + newTableAlias + " ON " + joinConditionTarget + ".id = " + newTableAlias + ".id";
    String rootExpression = lateralViewsToConvert.last().getRootExpression();
    Join inlineQuery = new Join(joinExpression, rootExpression, JoinType.INLINE_QUERY,
        newTableAlias);
    inlineQuery.setDependsUpon(firstLateralView.getDependsUpon());
    dependentTableJoin.setDependsUpon(inlineQuery);
    String transformedExpression = dependentTableJoin.getExpression()
        .replaceAll("(?<=(ON|AND)\\s)" + lateralViewsToConvert.last().getTableAlias() + "\\.",
            inlineQuery.getTableAlias() + ".");
    dependentTableJoin.setExpression(transformedExpression);
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
