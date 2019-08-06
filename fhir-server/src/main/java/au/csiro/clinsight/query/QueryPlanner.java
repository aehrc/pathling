/*
 * Copyright © Australian e-Health Research Centre, CSIRO. All rights reserved.
 */

package au.csiro.clinsight.query;

import static au.csiro.clinsight.fhir.definitions.ResourceDefinitions.BASE_RESOURCE_URL_PREFIX;
import static au.csiro.clinsight.query.parsing.Join.rewriteSqlWithJoinAliases;
import static au.csiro.clinsight.query.parsing.ParseResult.FhirPathType.BOOLEAN;

import au.csiro.clinsight.TerminologyClient;
import au.csiro.clinsight.fhir.definitions.PathResolver;
import au.csiro.clinsight.fhir.definitions.PathTraversal;
import au.csiro.clinsight.fhir.definitions.exceptions.ResourceNotKnownException;
import au.csiro.clinsight.query.AggregateQuery.Aggregation;
import au.csiro.clinsight.query.AggregateQuery.Grouping;
import au.csiro.clinsight.query.parsing.*;
import ca.uhn.fhir.rest.server.exceptions.InvalidRequestException;
import java.util.ArrayList;
import java.util.List;
import java.util.SortedSet;
import java.util.TreeSet;
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

  private final String fromTable;
  private final List<ParseResult> aggregationParseResults;
  private final List<ParseResult> groupingParseResults;
  private final List<ParseResult> filterParseResults;
  private final ExpressionParser expressionParser;

  QueryPlanner(@Nonnull TerminologyClient terminologyClient, @Nonnull SparkSession spark,
      @Nonnull SqlRunner sqlRunner, String databaseName, @Nonnull AggregateQuery query) {
    List<Aggregation> aggregations = query.getAggregations();
    List<Grouping> groupings = query.getGroupings();
    List<String> filters = query.getFilters();
    if (aggregations.isEmpty()) {
      throw new InvalidRequestException("Missing aggregation component within query");
    }

    // Build a ParseResult to represent the subject resource.
    String resourceName = query.getSubjectResource().replace(BASE_RESOURCE_URL_PREFIX, "");
    fromTable = resourceName.toLowerCase();

    // Resolve the subject resource, throwing an error if it is not known.
    PathTraversal resource;
    try {
      resource = PathResolver.resolvePath(resourceName);
    } catch (ResourceNotKnownException e) {
      throw new InvalidRequestException(
          "Subject resource not known: " + query.getSubjectResource());
    }

    ParseResult subjectResource = new ParseResult();
    subjectResource.setFhirPath(resourceName);
    subjectResource.setSql(fromTable);
    subjectResource.setPathTraversal(resource);
    subjectResource.setSingular(true);

    // Gather dependencies for the execution of the expression parser.
    ExpressionParserContext context = new ExpressionParserContext();
    context.setTerminologyClient(terminologyClient);
    context.setSparkSession(spark);
    context.setDatabaseName(databaseName);
    context.setSubjectResource(subjectResource);
    context.setFromTable(fromTable);
    context.setAliasGenerator(new AliasGenerator());
    context.setSqlRunner(sqlRunner);

    // Build a new expression parser, and parse all of the expressions within the query.
    expressionParser = new ExpressionParser(context);
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
            if (!result.isPrimitive()) {
              throw new InvalidRequestException(
                  "Grouping expression not of primitive type: " + groupingExpression);
            }
            return result;
          }).collect(Collectors.toList());
    }
    return groupingParseResults;
  }

  private List<ParseResult> parseFilters(List<String> filters) {
    return filters.stream().map(expression -> {
      ParseResult result = expressionParser.parse(expression);
      if (result.getFhirPathType() != BOOLEAN) {
        throw new InvalidRequestException(
            "Filter expression is not of boolean type: " + expression);
      }
      return result;
    }).collect(Collectors.toList());
  }

  /**
   * Builds a QueryPlan object from the results of parsing the query and the expressions within.
   */
  QueryPlan buildQueryPlan() {
    QueryPlan queryPlan = new QueryPlan();
    queryPlan.setAggregations(aggregationParseResults);
    queryPlan.setGroupings(groupingParseResults);
    queryPlan.setFilters(filterParseResults);
    queryPlan.setFromTable(fromTable);
    addAllJoins(queryPlan);
    rewriteExpressions(queryPlan);
    return queryPlan;
  }

  /**
   * Get joins from the results of parsing both aggregations and groupings.
   */
  private void addAllJoins(QueryPlan queryPlan) {
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
    queryPlan.setJoins(joins);
  }

  /**
   * Rewrite expressions to use aliases within the joins.
   */
  private void rewriteExpressions(QueryPlan queryPlan) {
    for (ParseResult aggregation : queryPlan.getAggregations()) {
      String newSql = rewriteSqlWithJoinAliases(aggregation.getSql(), queryPlan.getJoins());
      aggregation.setSql(newSql);
    }
    for (ParseResult grouping : queryPlan.getGroupings()) {
      String newSql = rewriteSqlWithJoinAliases(grouping.getSql(), queryPlan.getJoins());
      grouping.setSql(newSql);
    }
    for (ParseResult filter : queryPlan.getFilters()) {
      String newSql = rewriteSqlWithJoinAliases(filter.getSql(), queryPlan.getJoins());
      filter.setSql(newSql);
    }
  }

}
