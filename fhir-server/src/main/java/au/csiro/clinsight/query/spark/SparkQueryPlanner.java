/*
 * Copyright © Australian e-Health Research Centre, CSIRO. All rights reserved.
 */

package au.csiro.clinsight.query.spark;

import au.csiro.clinsight.resources.AggregateQuery;
import au.csiro.clinsight.resources.AggregateQuery.AggregationComponent;
import au.csiro.clinsight.resources.AggregateQuery.GroupingComponent;
import ca.uhn.fhir.rest.server.exceptions.InvalidRequestException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.stream.Collectors;

/**
 * This class knows how to take an AggregateQuery and convert it into an object which contains all
 * the information needed to execute the query as SQL against a Spark data warehouse.
 *
 * @author John Grimes
 */
class SparkQueryPlanner {

  private final List<ParseResult> aggregationParseResults;
  private final List<ParseResult> groupingParseResults;

  SparkQueryPlanner(AggregateQuery query) {
    List<AggregationComponent> aggregations = query.getAggregation();
    List<GroupingComponent> groupings = query.getGrouping();
    if (aggregations == null || aggregations.isEmpty()) {
      throw new InvalidRequestException("Missing aggregation component within query");
    }

    aggregationParseResults = parseAggregation(aggregations);
    groupingParseResults = parseGroupings(groupings);
  }

  private List<ParseResult> parseAggregation(List<AggregationComponent> aggregations) {
    return aggregations.stream()
        .map(aggregation -> {
          // TODO: Support references to pre-defined aggregations.
          String aggExpression = aggregation.getExpression().asStringValue();
          if (aggExpression == null) {
            throw new InvalidRequestException("Aggregation component must have expression");
          }
          AggregationParser aggregationParser = new AggregationParser();
          return aggregationParser.parse(aggExpression);
        }).collect(Collectors.toList());
  }

  private List<ParseResult> parseGroupings(List<GroupingComponent> groupings) {
    List<ParseResult> groupingParseResults = new ArrayList<>();
    if (groupings != null) {
      groupingParseResults = groupings.stream()
          .map(grouping -> {
            // TODO: Support references to pre-defined dimensions.
            String groupingExpression = grouping.getExpression().asStringValue();
            if (groupingExpression == null) {
              throw new InvalidRequestException("Grouping component must have expression");
            }
            GroupingParser groupingParser = new GroupingParser();
            return groupingParser.parse(groupingExpression);
          }).collect(Collectors.toList());
    }
    return groupingParseResults;
  }

  QueryPlan buildQueryPlan() {
    QueryPlan queryPlan = new QueryPlan();

    // Get aggregation expressions from the parse results.
    List<String> aggregations = aggregationParseResults.stream()
        .map(ParseResult::getExpression)
        .collect(Collectors.toList());
    queryPlan.setAggregations(aggregations);

    // Get aggregation data types from the parse results.
    List<String> aggregationTypes = aggregationParseResults.stream()
        .map(ParseResult::getResultType)
        .collect(Collectors.toList());
    queryPlan.setAggregationTypes(aggregationTypes);

    // Get grouping expressions from the parse results.
    List<String> groupings = groupingParseResults.stream()
        .map(ParseResult::getExpression)
        .collect(Collectors.toList());
    queryPlan.setGroupings(groupings);

    // Get grouping data types from the parse results.
    List<String> groupingTypes = groupingParseResults.stream()
        .map(ParseResult::getResultType)
        .collect(Collectors.toList());
    queryPlan.setGroupingTypes(groupingTypes);

    // Get from tables from the results of parsing both aggregations and groupings, and compute the
    // union.
    Set<String> aggregationFromTables = aggregationParseResults.stream()
        .map(ParseResult::getFromTable)
        .collect(Collectors.toSet());
    Set<String> groupingFromTables = groupingParseResults.stream()
        .map(ParseResult::getFromTable)
        .collect(Collectors.toSet());
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
    queryPlan.setJoins(joins);

    return queryPlan;
  }

}
