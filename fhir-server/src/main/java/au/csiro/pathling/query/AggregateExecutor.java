/*
 * Copyright © Australian e-Health Research Centre, CSIRO. All rights reserved.
 */

package au.csiro.pathling.query;

import static au.csiro.pathling.query.parsing.ParsedExpression.FhirPathType.BOOLEAN;

import au.csiro.pathling.fhir.TerminologyClient;
import au.csiro.pathling.fhir.TerminologyClientFactory;
import au.csiro.pathling.query.AggregateRequest.Aggregation;
import au.csiro.pathling.query.AggregateRequest.Grouping;
import au.csiro.pathling.query.parsing.Joinable;
import au.csiro.pathling.query.parsing.ParsedExpression;
import au.csiro.pathling.query.parsing.parser.ExpressionParser;
import au.csiro.pathling.query.parsing.parser.ExpressionParserContext;
import ca.uhn.fhir.context.FhirContext;
import ca.uhn.fhir.rest.server.exceptions.BaseServerResponseException;
import ca.uhn.fhir.rest.server.exceptions.InternalErrorException;
import ca.uhn.fhir.rest.server.exceptions.InvalidRequestException;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import org.apache.spark.sql.*;
import org.hl7.fhir.r4.model.CodeType;
import org.hl7.fhir.r4.model.Enumeration;
import org.hl7.fhir.r4.model.Enumerations.ResourceType;
import org.hl7.fhir.r4.model.Type;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class knows how to take an AggregateQuery and execute it against an Apache Spark data
 * warehouse, returning the result as an AggregateQueryResult resource.
 *
 * @author John Grimes
 */
public class AggregateExecutor {

  private static final Logger logger = LoggerFactory.getLogger(AggregateExecutor.class);

  private final FhirContext fhirContext;
  private final TerminologyClientFactory terminologyClientFactory;
  private final SparkSession spark;
  private final ResourceReader resourceReader;
  private final TerminologyClient terminologyClient;
  private final boolean explainQueries;

  public AggregateExecutor(@Nonnull AggregateExecutorConfiguration configuration) {
    logger.info("Creating new AggregateExecutor: " + configuration);
    spark = configuration.getSparkSession();
    fhirContext = configuration.getFhirContext();
    terminologyClientFactory = configuration.getTerminologyClientFactory();
    terminologyClient = configuration.getTerminologyClient();
    resourceReader = configuration.getResourceReader();
    explainQueries = configuration.isExplainQueries();
  }

  /**
   * Check that the aggregate executor is ready to execute a query by checking the readiness of all
   * dependencies.
   */
  public boolean isReady() {
    if (spark == null || terminologyClient == null) {
      return false;
    }
    try {
      logger.info("Getting terminology service capability statement to check service health");
      terminologyClient.getServerMetadata();
    } catch (Exception e) {
      logger.error("Readiness failure", e);
      return false;
    }
    return true;
  }

  public ResourceReader getResourceReader() {
    return resourceReader;
  }

  public Set<ResourceType> getAvailableResourceTypes() {
    return resourceReader.getAvailableResourceTypes();
  }

  public AggregateResponse execute(AggregateRequest query) throws InvalidRequestException {
    try {
      // Log query.
      logger.info("Received $aggregate request: aggregations=[" + query.getAggregations().stream()
          .map(Aggregation::getExpression).collect(
              Collectors.joining(",")) + "] groupings=[" + query.getGroupings().stream()
          .map(Grouping::getExpression).collect(
              Collectors.joining(",")) + "] filters=[" + String.join(",", query.getFilters())
          + "]");

      if (1 == 1) {
        throw new RuntimeException("bar");
      }

      // Set up the subject resource dataset.
      ResourceType resourceType = query.getSubjectResource();
      String resourceCode = resourceType.toCode();
      Dataset<Row> subject = resourceReader.read(resourceType);
      String firstColumn = subject.columns()[0];
      String[] remainingColumns = Arrays
          .copyOfRange(subject.columns(), 1, subject.columns().length);
      Column idColumn = subject.col("id");
      subject = subject.withColumn("resource",
          org.apache.spark.sql.functions.struct(firstColumn, remainingColumns));
      Column valueColumn = subject.col("resource");
      subject = subject.select(idColumn, valueColumn);

      // Create an expression for the subject resource.
      ParsedExpression subjectResource = new ParsedExpression();
      subjectResource.setFhirPath(resourceCode);
      subjectResource.setDataset(subject);
      subjectResource.setResource(true);
      subjectResource.setResourceType(resourceType);
      subjectResource.setSingular(true);
      subjectResource.setOrigin(subjectResource);
      subjectResource.setHashedValue(idColumn, valueColumn);

      // Gather dependencies for the execution of the expression parser.
      ExpressionParserContext context = new ExpressionParserContext();
      context.setFhirContext(fhirContext);
      context.setTerminologyClientFactory(terminologyClientFactory);
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

      // Gather all expressions into a single list.
      Dataset<Row> result = subjectResource.getDataset();
      List<Joinable> allExpressions = new ArrayList<>();
      allExpressions.add(subjectResource);
      allExpressions.addAll(parsedFilters);
      allExpressions.addAll(parsedGroupings);
      allExpressions
          .addAll(parsedAggregations.stream().map(ParsedExpression::getAggregationJoinable)
              .collect(Collectors.toList()));
      Set<Dataset<Row>> joinedDatasets = new HashSet<>();
      joinedDatasets.add(subjectResource.getDataset());

      // Join all datasets together, omitting any duplicates.
      Joinable previous = subjectResource;
      for (int i = 0; i < allExpressions.size(); i++) {
        Joinable current = allExpressions.get(i);
        if (i > 0 && !joinedDatasets.contains(current.getDataset())) {
          result = result.join(current.getDataset(),
              previous.getIdColumn().equalTo(current.getIdColumn()));
          previous = current;
          joinedDatasets.add(current.getDataset());
        }
      }

      // Apply filters.
      Optional<Column> filterCondition = parsedFilters.stream()
          .map(ParsedExpression::getValueColumn)
          .reduce(Column::and);
      if (filterCondition.isPresent()) {
        result = result.filter(filterCondition.get());
      }

      // Apply groupings.
      Column[] groupingCols = parsedGroupings.stream()
          .map(ParsedExpression::getValueColumn)
          .toArray(Column[]::new);
      RelationalGroupedDataset groupedResult = null;
      if (groupingCols.length > 0) {
        groupedResult = result.groupBy(groupingCols);
      }

      // Apply aggregations.
      Column firstAggregation = parsedAggregations.get(0).getAggregationColumn();
      Column[] remainingAggregations = parsedAggregations.stream()
          .skip(1)
          .map(ParsedExpression::getAggregationColumn)
          .toArray(Column[]::new);
      result = groupingCols.length > 0
          ? groupedResult.agg(firstAggregation, remainingAggregations)
          : result.agg(firstAggregation, remainingAggregations);

      // Translate the result into a response object to be passed back to the user.
      return buildResponse(result, parsedAggregations, parsedGroupings);

    } catch (BaseServerResponseException e) {
      // Errors relating to invalid input are re-raised, to be dealt with by HAPI.
      logger.warn("Invalid request", e);
      throw e;
    } catch (Exception | AssertionError e) {
      // All unexpected exceptions get wrapped in a 500 for presenting back to the user.
      throw new InternalErrorException("Unexpected error occurred while executing query", e);
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

  /**
   * Build an AggregateQueryResult resource from the supplied Dataset, embedding the original
   * AggregateQuery and honouring the hints within the QueryPlan.
   */
  private AggregateResponse buildResponse(@Nonnull Dataset<Row> dataset,
      @Nonnull List<ParsedExpression> parsedAggregations,
      @Nonnull List<ParsedExpression> parsedGroupings) {
    if (explainQueries) {
      logger.info("$aggregate query plan:");
      dataset.explain(true);
    }
    List<Row> rows = dataset.collectAsList();

    AggregateResponse queryResult = new AggregateResponse();

    List<AggregateResponse.Grouping> groupings = rows.stream()
        .map(mapRowToGrouping(parsedAggregations, parsedGroupings))
        .collect(Collectors.toList());
    queryResult.getGroupings().addAll(groupings);

    return queryResult;
  }

  /**
   * Translate a Dataset Row into a grouping component for inclusion within an
   * AggregateQueryResult.
   */
  private Function<Row, AggregateResponse.Grouping> mapRowToGrouping(
      @Nonnull List<ParsedExpression> parsedAggregations,
      @Nonnull List<ParsedExpression> parsedGroupings) {
    return row -> {
      AggregateResponse.Grouping grouping = new AggregateResponse.Grouping();

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
      Class hapiClass = parsedExpression.getImplementingClass(fhirContext);
      if (hapiClass == Enumeration.class) {
        hapiClass = CodeType.class;
      }
      Class javaClass = parsedExpression.getJavaClass();
      @SuppressWarnings("unchecked") Constructor constructor = hapiClass.getConstructor(javaClass);
      Object value = row.get(columnNumber);
      return value == null ? null : (Type) constructor.newInstance(value);
    } catch (NoSuchMethodException | IllegalAccessException | InvocationTargetException | InstantiationException e) {
      logger.error("Failed to access value from row: " + e.getMessage());
      return null;
    }
  }

}
