/*
 * Copyright © Australian e-Health Research Centre, CSIRO. All rights reserved.
 */

package au.csiro.clinsight.query;

import static au.csiro.clinsight.query.parsing.Join.JoinType.EXISTS_JOIN;
import static au.csiro.clinsight.query.parsing.Join.JoinType.INLINE_QUERY;
import static au.csiro.clinsight.query.parsing.Join.JoinType.LATERAL_VIEW;
import static au.csiro.clinsight.query.parsing.Join.JoinType.TABLE_JOIN;
import static au.csiro.clinsight.utilities.Strings.backTicks;
import static au.csiro.clinsight.utilities.Strings.tokenizePath;

import au.csiro.clinsight.TerminologyClient;
import au.csiro.clinsight.fhir.definitions.ResolvedElement.ResolvedElementType;
import au.csiro.clinsight.query.AggregateQuery.Aggregation;
import au.csiro.clinsight.query.AggregateQuery.Grouping;
import au.csiro.clinsight.query.parsing.ExpressionParser;
import au.csiro.clinsight.query.parsing.Join;
import au.csiro.clinsight.query.parsing.ParseResult;
import ca.uhn.fhir.rest.server.exceptions.InvalidRequestException;
import java.util.*;
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
class QueryPlanner {

  private final TerminologyClient terminologyClient;
  private final SparkSession spark;
  private final AggregateQuery query;
  private final List<ParseResult> aggregationParseResults;
  private final List<ParseResult> groupingParseResults;

  QueryPlanner(@Nonnull TerminologyClient terminologyClient,
      @Nonnull SparkSession spark, @Nonnull AggregateQuery query) {
    this.terminologyClient = terminologyClient;
    this.spark = spark;
    this.query = query;
    List<Aggregation> aggregations = query.getAggregations();
    List<Grouping> groupings = query.getGroupings();
    if (aggregations.isEmpty()) {
      throw new InvalidRequestException("Missing aggregation component within query");
    }

    aggregationParseResults = parseAggregation(aggregations);
    groupingParseResults = parseGroupings(groupings);
  }

  /**
   * Executes the ExpressionParser over each of the expressions within a list of aggregations, then
   * returns a list of ParseResults.
   */
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

  /**
   * Executes the ExpressionParser over each of the expressions within a list of groupings, then
   * returns a list of ParseResults.
   */
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
            // Validate that the return value of the expression is a primitive element reference,
            // this is a requirement for a grouping.
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

  /**
   * Builds a QueryPlan object from the results of parsing the query and the expressions within.
   */
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

    computeFromTables(queryPlan);

    computeJoins(queryPlan);

    return queryPlan;
  }

  /**
   * Get from tables from the results of parsing both aggregations and groupings, and compute the
   * union.
   */
  private void computeFromTables(QueryPlan queryPlan) {
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
  }

  /**
   * Get joins from the results of parsing both aggregations and groupings.
   */
  private void computeJoins(QueryPlan queryPlan) {
    SortedSet<Join> joins = new TreeSet<>();
    for (ParseResult parseResult : aggregationParseResults) {
      joins.addAll(parseResult.getJoins());
    }
    for (ParseResult parseResult : groupingParseResults) {
      joins.addAll(parseResult.getJoins());
    }
    SortedSet<Join> convertedJoins = convertUpstreamLateralViewsToInlineQueries(joins);
    queryPlan.setJoins(convertedJoins);
    convertExistsJoin(queryPlan);
  }

  private void convertExistsJoin(QueryPlan queryPlan) {
    if (queryPlan.getJoins().isEmpty()
        || queryPlan.getJoins().last().getJoinType() != EXISTS_JOIN) {
      return;
    }
    Join finalJoin = queryPlan.getJoins().last();

    List<String> selectExpressions = new ArrayList<>();
    for (int i = 0; i < queryPlan.getAggregations().size(); i++) {
      ParseResult aggregationParseResult = aggregationParseResults.get(i);
      String label = backTicks(query.getAggregations().get(i).getLabel());
      selectExpressions.add(aggregationParseResult.getPreAggregationExpression() + " AS " + label);
    }
    String innerAggregation = "MAX(" + finalJoin.getTableAlias() + ".code) AS code";
    selectExpressions.add(innerAggregation);

    LinkedList<String> groupByArgs = new LinkedList<>();
    for (int i = 0; i < queryPlan.getAggregations().size(); i++) {
      groupByArgs.add(Integer.toString(i + 1));
    }

    String selectClause = "SELECT " + String.join(", ", selectExpressions);
    String fromClause = "FROM " + String.join(", ", queryPlan.getFromTables());
    String joins = queryPlan.getJoins().stream().map(Join::getExpression).collect(
        Collectors.joining(" "));
    String groupByClause = "GROUP BY " + String.join(", ", groupByArgs);
    List<String> clauses = new LinkedList<>(Arrays.asList(selectClause, fromClause));
    if (!joins.isEmpty()) {
      clauses.add(joins);
    }
    if (queryPlan.getGroupings().size() > 0) {
      clauses.add(groupByClause);
    }
    String sql = String.join(" ", clauses);
    String tableAlias = finalJoin.getTableAlias() + "Aggregated";
    String fromTable = "(" + sql + ") " + tableAlias;

    queryPlan.getJoins().clear();
    queryPlan.getFromTables().clear();
    queryPlan.getFromTables().add(fromTable);

    for (int i = 0; i < queryPlan.getGroupings().size(); i++) {
      String grouping = queryPlan.getGroupings().get(i);
      String transformed = grouping.replaceAll(finalJoin.getTableAlias(), tableAlias);
      queryPlan.getGroupings().set(i, transformed);
    }
    for (int i = 0; i < queryPlan.getAggregations().size(); i++) {
      String aggregation = queryPlan.getAggregations().get(i);
      ParseResult aggregationParseResult = aggregationParseResults.get(i);
      if (aggregationParseResult.getPreAggregationExpression() != null) {
        String label = backTicks(query.getAggregations().get(i).getLabel());
        String transformed = aggregation
            .replaceAll(aggregationParseResult.getPreAggregationExpression(),
                tableAlias + "." + label);
        queryPlan.getAggregations().set(i, transformed);
      }
    }
  }

  /**
   * Spark SQL does not currently allow a table join to follow a LATERAL VIEW join within a query -
   * the LATERAL VIEW statement must first be wrapped within a subquery. This method takes a set of
   * joins and wraps each of the LATERAL VIEW joins in a subquery, then returns a new set of joins.
   */
  private SortedSet<Join> convertUpstreamLateralViewsToInlineQueries(SortedSet<Join> joins) {
    if (joins.isEmpty()) {
      return joins;
    }
    // We start with the final join and move upwards through the dependencies.
    Join cursor = joins.last();
    Join dependentTableJoin = null;
    SortedSet<Join> lateralViewsToConvert = new TreeSet<>();
    // We stop when we reach the end of the dependencies and there are no lateral views queued up
    // for conversion.
    while (cursor != null || !lateralViewsToConvert.isEmpty()) {
      if (cursor == null) {
        // Upon reaching the end of the dependencies, we bundle up any lateral views queued up for
        // conversion.
        assert dependentTableJoin != null;
        joins = replaceLateralViews(joins, dependentTableJoin, lateralViewsToConvert);
      } else {
        if (cursor.getJoinType() == TABLE_JOIN || cursor.getJoinType() == EXISTS_JOIN) {
          if (dependentTableJoin == null) {
            // We mark a table join that depends upon a lateral view (or set of lateral views) as
            // the "dependent table join". It stays marked until we reach the end of the lateral
            // views that it depends upon and finish converting them.
            dependentTableJoin = cursor;
          } else {
            // If we reach a new table join that is not the "dependent table join", then that must
            // mean that we have reached the end of this contiguous set of lateral views. This is
            // the trigger to take these and convert them into an inline query.
            joins = replaceLateralViews(joins, dependentTableJoin, lateralViewsToConvert);
            dependentTableJoin = null;
            continue;
          }
        } else if (cursor.getJoinType() == LATERAL_VIEW && dependentTableJoin != null) {
          // If we find a new lateral view, we add that to the set of lateral views that are queued
          // for conversion.
          lateralViewsToConvert.add(cursor);
        }
        // Each iteration of the loop, we move to a new join based upon the depends upon
        // relationship.
        cursor = cursor.getDependsUpon();
      }
    }
    return joins;
  }

  /**
   * This method takes a set of LATERAL VIEW joins and converts them into a single join with an
   * inline query.
   */
  private SortedSet<Join> replaceLateralViews(SortedSet<Join> joins, Join dependentTableJoin,
      SortedSet<Join> lateralViewsToConvert) {
    // First we search for invocations made against the alias of the last join in the group. This tells us which columns will need to be selected within the subquery.
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
    String selectFields = tableAliasInvocations.stream()
        .map(tai -> finalTableAlias + "." + tai)
        .collect(Collectors.joining(", "));
    String joinExpression = "LEFT JOIN (SELECT id, " + selectFields + " FROM " + table + " ";
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
            inlineQuery.getTableAlias() + ".");
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
