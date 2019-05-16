/*
 * Copyright © Australian e-Health Research Centre, CSIRO. All rights reserved.
 */

package au.csiro.clinsight.query;

import static au.csiro.clinsight.fhir.definitions.ResolvedElement.ResolvedElementType.PRIMITIVE;
import static au.csiro.clinsight.query.QueryWrangling.convertUpstreamLateralViewsToInlineQueries;
import static au.csiro.clinsight.query.parsing.ParseResult.ParseResultType.BOOLEAN;
import static au.csiro.clinsight.query.parsing.ParseResult.ParseResultType.COLLECTION;

import au.csiro.clinsight.TerminologyClient;
import au.csiro.clinsight.query.AggregateQuery.Aggregation;
import au.csiro.clinsight.query.AggregateQuery.Grouping;
import au.csiro.clinsight.query.parsing.ExpressionParser;
import au.csiro.clinsight.query.parsing.Join;
import au.csiro.clinsight.query.parsing.ParseResult;
import ca.uhn.fhir.rest.server.exceptions.InvalidRequestException;
import java.util.*;
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

  private final List<ParseResult> aggregationParseResults;
  private final List<ParseResult> groupingParseResults;
  private final List<ParseResult> filterParseResults;
  private final ExpressionParser expressionParser;

  QueryPlanner(@Nonnull TerminologyClient terminologyClient,
      @Nonnull SparkSession spark, @Nonnull AggregateQuery query) {
    List<Aggregation> aggregations = query.getAggregations();
    List<Grouping> groupings = query.getGroupings();
    List<String> filters = query.getFilters();
    if (aggregations.isEmpty()) {
      throw new InvalidRequestException("Missing aggregation component within query");
    }

    expressionParser = new ExpressionParser(terminologyClient, spark);
    aggregationParseResults = parseAggregation(aggregations);
    groupingParseResults = parseGroupings(groupings);
    filterParseResults = parseFilters(filters);
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
          return expressionParser.parse(aggExpression);
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
            ParseResult result = expressionParser.parse(groupingExpression);
            // Validate that the return value of the expression is a collection of primitive types,
            // this is a requirement for a grouping.
            if (result.getResultType() == COLLECTION && result.getElementType() == PRIMITIVE) {
              return result;
            } else {
              throw new InvalidRequestException(
                  "Grouping expression is not of primitive type: " + groupingExpression + " ("
                      + result.getElementTypeCode() + ")");
            }
          }).collect(Collectors.toList());
    }
    return groupingParseResults;
  }

  private List<ParseResult> parseFilters(List<String> filters) {
    return filters.stream().map(expression -> {
      ParseResult result = expressionParser.parse(expression);
      if (result.getResultType() != BOOLEAN) {
        throw new InvalidRequestException(
            "Filter expression is not of boolean type: " + expression);
      }
      return result;
    })
        .collect(Collectors.toList());
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

    // Get filter expressions from the parse results.
    List<String> filters = filterParseResults.stream()
        .map(ParseResult::getSqlExpression)
        .collect(Collectors.toList());
    queryPlan.setFilters(filters);

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
        .forEach(parseResult -> aggregationFromTables.addAll(parseResult.getFromTables()));
    Set<String> groupingFromTables = new HashSet<>();
    groupingParseResults
        .forEach(parseResult -> groupingFromTables.addAll(parseResult.getFromTables()));
    Set<String> filterFromTables = new HashSet<>();
    filterParseResults
        .forEach(parseResult -> filterFromTables.addAll(parseResult.getFromTables()));
    // Check for from tables within the groupings that were not referenced within at least one
    // aggregation expression.
    if (!aggregationFromTables.containsAll(groupingFromTables)) {
      Set<String> difference = new HashSet<>(groupingFromTables);
      difference.removeAll(aggregationFromTables);
      throw new InvalidRequestException(
          "Groupings contain one or more resources that are not the subject of an aggregation: "
              + String.join(", ", difference));
    }
    // Check for from tables within the filters that were not referenced within at least one
    // aggregation expression.
    if (!aggregationFromTables.containsAll(filterFromTables)) {
      Set<String> difference = new HashSet<>(filterFromTables);
      difference.removeAll(aggregationFromTables);
      throw new InvalidRequestException(
          "Filters contain one or more resources that are not the subject of an aggregation: "
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
    for (ParseResult parseResult : filterParseResults) {
      joins.addAll(parseResult.getJoins());
    }
    SortedSet<Join> convertedJoins = convertUpstreamLateralViewsToInlineQueries(joins);
    queryPlan.setJoins(convertedJoins);
  }


}
