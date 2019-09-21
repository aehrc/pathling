/*
 * Copyright © Australian e-Health Research Centre, CSIRO. All rights reserved.
 */

package au.csiro.clinsight.query;

import static au.csiro.clinsight.fhir.definitions.ResourceDefinitions.BASE_RESOURCE_URL_PREFIX;
import static au.csiro.clinsight.fhir.definitions.ResourceDefinitions.ensureInitialized;
import static au.csiro.clinsight.query.parsing.ParsedExpression.FhirPathType.BOOLEAN;
import static au.csiro.clinsight.utilities.Strings.md5Short;

import au.csiro.clinsight.fhir.TerminologyClient;
import au.csiro.clinsight.fhir.definitions.ResourceDefinitions;
import au.csiro.clinsight.query.QueryRequest.Aggregation;
import au.csiro.clinsight.query.QueryRequest.Grouping;
import au.csiro.clinsight.query.parsing.ExpressionParser;
import au.csiro.clinsight.query.parsing.ExpressionParserContext;
import au.csiro.clinsight.query.parsing.ParsedExpression;
import ca.uhn.fhir.rest.server.exceptions.BaseServerResponseException;
import ca.uhn.fhir.rest.server.exceptions.InternalErrorException;
import ca.uhn.fhir.rest.server.exceptions.InvalidRequestException;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import org.apache.spark.sql.*;
import org.hl7.fhir.r4.model.Type;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class knows how to take an AggregateQuery and execute it against an Apache Spark data
 * warehouse, returning the result as an AggregateQueryResult resource.
 *
 * @author John Grimes
 */
public class QueryExecutor {

  private static final Logger logger = LoggerFactory.getLogger(QueryExecutor.class);

  private final SparkSession spark;
  private final ResourceReader resourceReader;
  private TerminologyClient terminologyClient;

  public QueryExecutor(@Nonnull QueryExecutorConfiguration configuration) {
    logger.info("Creating new QueryExecutor: " + configuration);
    this.spark = configuration.getSparkSession();
    this.terminologyClient = configuration.getTerminologyClient();
    this.resourceReader = new ResourceReader(configuration.getSparkSession(),
        configuration.getWarehouseUrl(), configuration.getDatabaseName());
    initialiseResourceDefinitions();
  }

  private void initialiseResourceDefinitions() {
    ensureInitialized(terminologyClient);
  }

  /**
   * Check that the query executor is ready to execute a query by checking the readiness of all
   * dependencies.
   */
  public boolean isReady() {
    if (spark == null || terminologyClient == null) {
      return false;
    }
    try {
      ResourceDefinitions.checkInitialised();
      terminologyClient.getServerMetadata();
    } catch (Exception e) {
      logger.error("Readiness failure", e);
      return false;
    }
    return true;
  }

  public QueryResponse execute(QueryRequest query) throws InvalidRequestException {
    try {
      // Set up the subject resource dataset.
      if (!query.getSubjectResource().startsWith(BASE_RESOURCE_URL_PREFIX)) {
        throw new InvalidRequestException(
            "Non-base resources not currently supported within subjectResource parameter: " + query
                .getSubjectResource());
      }
      String resourceName = query.getSubjectResource().replaceFirst(BASE_RESOURCE_URL_PREFIX, "");
      String hash = md5Short(resourceName);
      Dataset<Row> subject = resourceReader.read(query.getSubjectResource());
      subject = subject.withColumnRenamed("id", hash + "_id");

      // Create an expression for the subject resource.
      ParsedExpression subjectResource = new ParsedExpression();
      subjectResource.setDataset(subject);
      subjectResource.setDatasetColumn(hash);
      subjectResource.setResource(true);
      subjectResource.setResourceDefinition(query.getSubjectResource());

      // Gather dependencies for the execution of the expression parser.
      ExpressionParserContext context = new ExpressionParserContext();
      context.setTerminologyClient(terminologyClient);
      context.setSparkSession(spark);
      context.setResourceReader(resourceReader);
      context.setSubjectContext(subjectResource);

      // Build a new expression parser, and parse all of the filter and grouping expressions within
      // the query.
      ExpressionParser expressionParser = new ExpressionParser(context);
      List<ParsedExpression> parsedFilters = parseFilters(expressionParser,
          query.getFilters());
      List<ParsedExpression> parsedGroupings = parseGroupings(expressionParser,
          query.getGroupings());

      // Add information about the groupings to the parser context before parsing the aggregations.
      context.getGroupings().addAll(parsedGroupings);
      List<ParsedExpression> parsedAggregations = parseAggregation(expressionParser,
          query.getAggregations());

      // Build a dataset representing the final result of the query.
      Dataset<Row> result = buildResult(query, parsedAggregations, parsedGroupings,
          parsedFilters);

      // Translate the result into a response object to be passed back to the user.
      return buildResponse(result, parsedAggregations, parsedGroupings
      );

    } catch (BaseServerResponseException e) {
      // Errors relating to invalid input are re-raised, to be dealt with by HAPI.
      logger.warn("Invalid request", e);
      throw e;
    } catch (Exception | AssertionError e) {
      // All unexpected exceptions get logged and wrapped in a 500 for presenting back to the user.
      logger.error("Exception occurred while executing query", e);
      throw new InternalErrorException("Unexpected error occurred while executing query");
    }
  }

  /**
   * Executes the ExpressionParser over each of the expressions within a list of aggregations, then
   * returns a list of ParsedExpressions.
   */
  private List<ParsedExpression> parseAggregation(@Nonnull ExpressionParser expressionParser,
      @Nonnull List<Aggregation> aggregations) {
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
   * returns a list of ParsedExpressions.
   */
  private List<ParsedExpression> parseGroupings(@Nonnull ExpressionParser expressionParser,
      @Nonnull List<Grouping> groupings) {
    List<ParsedExpression> groupingParsedExpressions;
    groupingParsedExpressions = groupings.stream()
        .map(grouping -> {
          String groupingExpression = grouping.getExpression();
          if (groupingExpression == null) {
            throw new InvalidRequestException("Grouping component must have expression");
          }
          ParsedExpression result = expressionParser.parse(groupingExpression);
          // Validate that the return value of the expression is a collection of primitive types,
          // this is a requirement for a grouping.
          if (!result.isPrimitive()) {
            throw new InvalidRequestException(
                "Grouping expression not of primitive type: " + groupingExpression);
          }
          return result;
        }).collect(Collectors.toList());
    return groupingParsedExpressions;
  }

  private List<ParsedExpression> parseFilters(@Nonnull ExpressionParser expressionParser,
      @Nonnull List<String> filters) {
    return filters.stream().map(expression -> {
      ParsedExpression result = expressionParser.parse(expression);
      if (result.getFhirPathType() != BOOLEAN) {
        throw new InvalidRequestException(
            "Filter expression is not of boolean type: " + expression);
      }
      return result;
    }).collect(Collectors.toList());
  }

  private Dataset<Row> buildResult(@Nonnull QueryRequest query,
      @Nonnull List<ParsedExpression> parsedAggregations,
      @Nonnull List<ParsedExpression> parsedGroupings,
      @Nonnull List<ParsedExpression> parsedFilters) {
    Dataset<Row> result = null;
    // Create a dataset which represents only the resource IDs for which the filter expressions
    // evaluate to true.
    if (!parsedFilters.isEmpty()) {
      // Get the first filter dataset and filter it based upon its values.
      ParsedExpression firstFilter = parsedFilters.get(0);
      Column currentId = firstFilter.getDataset().col(firstFilter.getDatasetColumn() + "_id");
      Column resourceId = currentId.alias("resource_id");
      Column currentValue = firstFilter.getDataset().col(firstFilter.getDatasetColumn());
      result = firstFilter.getDataset();
      result = result.filter(currentValue);
      result = result.select(resourceId);
      // Go through each of the remaining filters and perform an inner join from the previous
      // dataset, conditional on the value column being true.
      for (int i = 1; i < parsedFilters.size(); i++) {
        ParsedExpression currentFilter = parsedFilters.get(i);
        currentId = currentFilter.getDataset().col(currentFilter.getDatasetColumn() + "_id");
        currentValue = currentFilter.getDataset().col(currentFilter.getDatasetColumn());
        result = result
            .join(currentFilter.getDataset(), resourceId.equalTo(currentId).and(currentValue),
                "inner");
        result = result.select(resourceId);
      }
    }

    // Join each of the grouping datasets to the result, and retain each as a column. These are all
    // left outer joins.
    if (!parsedGroupings.isEmpty()) {
      // Get the first grouping, and either join it to the filters or make it the start of the
      // result.
      ParsedExpression firstGrouping = parsedGroupings.get(0);
      Column firstId = firstGrouping.getDataset().col(firstGrouping.getDatasetColumn() + "_id");
      if (result == null) {
        // If there were no filters, the first grouping is our starting point.
        result = firstGrouping.getDataset();
      } else {
        // If there were filters, we join the last filter to the first grouping.
        ParsedExpression lastFilter = parsedFilters.get(parsedFilters.size() - 1);
        Column lastFilterId = result.col(lastFilter.getDatasetColumn() + "_id")
            .alias("resource_id");
        String firstGroupingLabel = query.getGroupings().get(0).getLabel();
        Column firstGroupingValue = lastFilterId.equalTo(firstId).alias(firstGroupingLabel);
        result = result
            .join(firstGrouping.getDataset(), firstGroupingValue, "left_outer");
      }
      // Go through each of the remaining groupings and perform a left outer join from the previous
      // dataset.
      for (int i = 1; i < parsedGroupings.size(); i++) {
        ParsedExpression prevGrouping = parsedGroupings.get(i - 1);
        Column prevId = prevGrouping.getDataset()
            .col(prevGrouping.getDatasetColumn() + "_id");
        ParsedExpression currentGrouping = parsedGroupings.get(i);
        String groupingLabel = query.getGroupings().get(i).getLabel();
        Column currentId = currentGrouping.getDataset()
            .col(currentGrouping.getDatasetColumn() + "_id");
        result = result.alias("prev_result");
        result = result
            .join(currentGrouping.getDataset(), prevId.equalTo(currentId), "left_outer");
        result = result.select("prev_result.*", groupingLabel);
      }
    }

    // Get the first aggregation, and either join it to the groupings or use it as the start of
    // the result.
    ParsedExpression firstAggregation = parsedAggregations.get(0);
    Column firstId = firstAggregation.getDataset().col(firstAggregation.getDatasetColumn() + "_id");
    if (result == null) {
      // If there were no groupings, the first aggregation is our starting point.
      result = firstAggregation.getDataset();
    } else {
      // If there were groupings, we join the last grouping to the first aggregation.
      ParsedExpression lastGrouping = parsedGroupings.get(parsedGroupings.size() - 1);
      Column lastGroupingId = result.col(lastGrouping.getDatasetColumn() + "_id")
          .alias("resource_id");
      String firstAggregationLabel = query.getAggregations().get(0).getLabel();
      Column firstAggregationValue = lastGroupingId.equalTo(firstId).alias(firstAggregationLabel);
      result = result
          .join(firstAggregation.getDataset(), firstAggregationValue, "left_outer");
    }
    // Go through each of the remaining aggregations and perform a left outer join from the previous
    // dataset.
    for (int i = 1; i < parsedAggregations.size(); i++) {
      ParsedExpression prevAggregation = parsedAggregations.get(i - 1);
      Column prevId = prevAggregation.getDataset()
          .col(prevAggregation.getDatasetColumn() + "_id");
      ParsedExpression currentAggregation = parsedAggregations.get(i);
      String aggregationLabel = query.getAggregations().get(i).getLabel();
      Column currentId = currentAggregation.getDataset()
          .col(currentAggregation.getDatasetColumn() + "_id");
      result = result.alias("prev_result");
      result = result
          .join(currentAggregation.getDataset(), prevId.equalTo(currentId), "left_outer");
      result = result.select("prev_result.*", aggregationLabel);
    }
    // Group by each of the grouping value columns.
    Column[] groupingColumns = (Column[]) parsedGroupings.stream()
        .map(grouping -> grouping.getDataset().col(grouping.getDatasetColumn()))
        .toArray();
    RelationalGroupedDataset groupedResult = result.groupBy(groupingColumns);
    // Apply the aggregations.
    Column firstAggregationColumn = firstAggregation.getDataset()
        .col(firstAggregation.getDatasetColumn());
    Column[] remainingAggregationColumns = (Column[]) parsedAggregations.stream()
        .skip(1)
        .map(ParsedExpression::getAggregation)
        .toArray();
    return groupedResult.agg(firstAggregationColumn, remainingAggregationColumns);
  }

  /**
   * Build an AggregateQueryResult resource from the supplied Dataset, embedding the original
   * AggregateQuery and honouring the hints within the QueryPlan.
   */
  private QueryResponse buildResponse(@Nonnull Dataset<Row> dataset,
      @Nonnull List<ParsedExpression> parsedAggregations,
      @Nonnull List<ParsedExpression> parsedGroupings) {
    List<Row> rows = dataset.collectAsList();

    QueryResponse queryResult = new QueryResponse();

    List<QueryResponse.Grouping> groupings = rows.stream()
        .map(mapRowToGrouping(parsedAggregations, parsedGroupings))
        .collect(Collectors.toList());
    queryResult.getGroupings().addAll(groupings);

    return queryResult;
  }

  /**
   * Translate a Dataset Row into a grouping component for inclusion within an
   * AggregateQueryResult.
   */
  private Function<Row, QueryResponse.Grouping> mapRowToGrouping(
      @Nonnull List<ParsedExpression> parsedAggregations,
      @Nonnull List<ParsedExpression> parsedGroupings) {
    return row -> {
      QueryResponse.Grouping grouping = new QueryResponse.Grouping();

      for (int i = 0; i < parsedGroupings.size(); i++) {
        ParsedExpression groupingResult = parsedGroupings.get(i);
        Type value = getValueFromRow(row, i, groupingResult);
        grouping.getLabels().add(value);
      }

      for (int i = 0; i < parsedAggregations.size(); i++) {
        ParsedExpression aggregationResult = parsedAggregations.get(i);
        Type value = getValueFromRow(row, i + parsedGroupings.size(), aggregationResult);
        grouping.getResults().add(value);
      }

      return grouping;
    };
  }

  /**
   * Extract a value from the specified column within a row, using the supplied data type. Note that
   * this method may return null where the value is actually null within the Dataset.
   */
  private Type getValueFromRow(Row row, int columnNumber, ParsedExpression parsedExpression) {
    try {
      assert
          parsedExpression.getFhirType() != null :
          "Parse result encountered with missing FHIR type: " + parsedExpression;
      Class hapiClass = parsedExpression.getFhirType().getHapiClass();
      Class javaClass = parsedExpression.getFhirType().getJavaClass();
      @SuppressWarnings("unchecked") Constructor constructor = hapiClass.getConstructor(javaClass);
      Object value = row.get(columnNumber);
      return value == null ? null : (Type) constructor.newInstance(value);
    } catch (NoSuchMethodException | IllegalAccessException | InvocationTargetException | InstantiationException e) {
      logger.error("Failed to access value from row: " + e.getMessage());
      return null;
    }
  }

}
