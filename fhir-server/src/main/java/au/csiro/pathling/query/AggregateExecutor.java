/*
 * Copyright © 2018-2020, Commonwealth Scientific and Industrial Research
 * Organisation (CSIRO) ABN 41 687 119 230. Licensed under the CSIRO Open Source
 * Software Licence Agreement.
 */

package au.csiro.pathling.query;

import static au.csiro.pathling.query.parsing.ParsedExpression.FhirPathType.BOOLEAN;

import au.csiro.pathling.query.AggregateRequest.Aggregation;
import au.csiro.pathling.query.AggregateRequest.Grouping;
import au.csiro.pathling.query.parsing.Joinable;
import au.csiro.pathling.query.parsing.LiteralComposer;
import au.csiro.pathling.query.parsing.ParsedExpression;
import au.csiro.pathling.query.parsing.parser.ExpressionParser;
import au.csiro.pathling.query.parsing.parser.ExpressionParserContext;
import ca.uhn.fhir.rest.server.exceptions.BaseServerResponseException;
import ca.uhn.fhir.rest.server.exceptions.InternalErrorException;
import ca.uhn.fhir.rest.server.exceptions.InvalidRequestException;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.RelationalGroupedDataset;
import org.apache.spark.sql.Row;
import org.hl7.fhir.r4.model.CodeType;
import org.hl7.fhir.r4.model.Enumeration;
import org.hl7.fhir.r4.model.Enumerations.ResourceType;
import org.hl7.fhir.r4.model.StringType;
import org.hl7.fhir.r4.model.Type;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class knows how to take an AggregateQuery and execute it against an Apache Spark data
 * warehouse, returning the result as an AggregateQueryResult resource.
 *
 * @author John Grimes
 */
public class AggregateExecutor extends QueryExecutor {

  private static final Logger logger = LoggerFactory.getLogger(AggregateExecutor.class);

  public AggregateExecutor(@Nonnull ExecutorConfiguration configuration) {
    super(configuration);
  }

  /**
   * Check that the aggregate executor is ready to execute a query by checking the readiness of all
   * dependencies.
   * <p>
   * TODO: Remove this in favour of a more general readiness check within the capability statement.
   */
  public boolean isReady() {
    try {
      logger.info("Getting terminology service capability statement to check service health");
      configuration.getTerminologyClient().getServerMetadata();
    } catch (Exception e) {
      logger.error("Readiness failure", e);
      return false;
    }
    return true;
  }

  public ResourceReader getResourceReader() {
    return configuration.getResourceReader();
  }

  public Set<ResourceType> getAvailableResourceTypes() {
    return configuration.getResourceReader().getAvailableResourceTypes();
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

      // Build a new expression parser, and parse all of the filter and grouping expressions within
      // the query.
      ExpressionParserContext context = buildParserContext(query.getSubjectResource());
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
      ParsedExpression subjectResource = context.getSubjectContext();
      List<Joinable> allExpressions = new ArrayList<>();
      allExpressions.add(subjectResource);
      allExpressions.addAll(parsedFilters);
      allExpressions.addAll(parsedGroupings.stream().map(ParsedExpression::getGroupingJoinable)
          .collect(Collectors.toList()));
      allExpressions
          .addAll(parsedAggregations.stream().map(ParsedExpression::getAggregationJoinable)
              .collect(Collectors.toList()));
      Dataset<Row> result = joinExpressions(allExpressions);

      // Apply filters.
      result = applyFilters(result, parsedFilters);

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
      return buildResponse(result, parsedAggregations, parsedGroupings, parsedFilters);
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
      @Nonnull List<ParsedExpression> parsedGroupings,
      @Nonnull List<ParsedExpression> parsedFilters
  ) {
    if (configuration.isExplainQueries()) {
      logger.info("$aggregate query plan:");
      dataset.explain(true);
    }
    List<Row> rows = dataset.collectAsList();

    AggregateResponse queryResult = new AggregateResponse();

    List<AggregateResponse.Grouping> groupings = rows.stream()
        .map(mapRowToGrouping(parsedAggregations, parsedGroupings, parsedFilters))
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
      @Nonnull List<ParsedExpression> parsedGroupings,
      List<ParsedExpression> parsedFilters) {
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

      String drillDown = buildDrillDown(parsedGroupings, parsedFilters, grouping);
      grouping.setDrillDown(new StringType(drillDown));

      return grouping;
    };
  }

  /**
   * Extract a value from the specified column within a row, using the supplied data type. Note that
   * this method may return null where the value is actually null within the Dataset.
   */
  @SuppressWarnings("rawtypes")
  private Type getValueFromRow(Row row, int columnNumber, ParsedExpression parsedExpression) {
    try {
      assert
          parsedExpression.getFhirType() != null :
          "Parse result encountered with missing FHIR type: " + parsedExpression;
      Class hapiClass = parsedExpression.getImplementingClass(configuration.getFhirContext());
      if (hapiClass == Enumeration.class) {
        hapiClass = CodeType.class;
      }
      Class javaClass = parsedExpression.getJavaClass();
      @SuppressWarnings("unchecked") Constructor constructor = hapiClass.getConstructor(javaClass);
      Object value = row.get(columnNumber);
      return value == null
             ? null
             : (Type) constructor.newInstance(value);
    } catch (NoSuchMethodException | IllegalAccessException | InvocationTargetException | InstantiationException e) {
      logger.error("Failed to access value from row: " + e.getMessage());
      return null;
    }
  }

  /**
   * Builds a URL for drilling down from an aggregate grouping to a list of resources.
   */
  private static String buildDrillDown(@Nonnull List<ParsedExpression> parsedGroupings,
      List<ParsedExpression> parsedFilters, AggregateResponse.Grouping grouping) {
    // We use a Set here to avoid situations where we needlessly have the same condition in the
    // expression more than once.
    Set<String> fhirPaths = new HashSet<>();

    // Add each of the grouping expressions, along with either equality or contains against the
    // group value to convert it in to a Boolean expression.
    for (int i = 0; i < parsedGroupings.size(); i++) {
      ParsedExpression parsedGrouping = parsedGroupings.get(i);
      Type label = grouping.getLabels().get(i);
      if (label == null) {
        fhirPaths.add(parsedGrouping.getFhirPath() + ".empty()");
      } else {
        String literal = LiteralComposer.getFhirPathForType(label);
        String equality = parsedGrouping.isSingular()
                          ? " = "
                          : " contains ";
        fhirPaths.add(parsedGrouping.getFhirPath() + equality + literal);
      }
    }

    // Add each of the filter expressions.
    List<String> filterFhirPaths = parsedFilters.stream()
        .map(ParsedExpression::getFhirPath)
        .collect(Collectors.toList());
    fhirPaths.addAll(filterFhirPaths);

    // If there is more than one expression, wrap each expression in parentheses before joining
    // together with Boolean AND operators.
    if (fhirPaths.size() > 1) {
      fhirPaths = fhirPaths.stream()
          .map(fhirPath -> "(" + fhirPath + ")")
          .collect(Collectors.toSet());
    }
    return String.join(" and ", fhirPaths);
  }
}
