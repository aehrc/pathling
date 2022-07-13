/*
 * Copyright © 2018-2022, Commonwealth Scientific and Industrial Research
 * Organisation (CSIRO) ABN 41 687 119 230. Licensed under the CSIRO Open Source
 * Software Licence Agreement.
 */

package au.csiro.pathling.aggregate;

import static au.csiro.pathling.QueryHelpers.createColumns;
import static au.csiro.pathling.utilities.Preconditions.checkUserInput;

import au.csiro.pathling.Configuration;
import au.csiro.pathling.QueryExecutor;
import au.csiro.pathling.QueryHelpers.DatasetWithColumnMap;
import au.csiro.pathling.fhir.TerminologyServiceFactory;
import au.csiro.pathling.fhirpath.FhirPath;
import au.csiro.pathling.fhirpath.Materializable;
import au.csiro.pathling.fhirpath.ResourcePath;
import au.csiro.pathling.fhirpath.element.BooleanPath;
import au.csiro.pathling.fhirpath.parser.Parser;
import au.csiro.pathling.fhirpath.parser.ParserContext;
import au.csiro.pathling.io.Database;
import au.csiro.pathling.sql.PathlingFunctions;
import ca.uhn.fhir.context.FhirContext;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import lombok.Value;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.hl7.fhir.r4.model.Type;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Component;

/**
 * This class knows how to take an {@link AggregateRequest} and execute it, returning the result as
 * an {@link AggregateResponse}.
 *
 * @author John Grimes
 */
@Component
@Profile("core")
@Slf4j
public class AggregateExecutor extends QueryExecutor {

  /**
   * @param configuration A {@link Configuration} object to control the behaviour of the executor
   * @param fhirContext A {@link FhirContext} for doing FHIR stuff
   * @param sparkSession A {@link SparkSession} for resolving Spark queries
   * @param database A {@link Database} for retrieving resources
   * @param terminologyClientFactory A {@link TerminologyServiceFactory} for resolving terminology
   */
  public AggregateExecutor(@Nonnull final Configuration configuration,
      @Nonnull final FhirContext fhirContext, @Nonnull final SparkSession sparkSession,
      @Nonnull final Database database,
      @Nonnull final Optional<TerminologyServiceFactory> terminologyClientFactory) {
    super(configuration, fhirContext, sparkSession, database,
        terminologyClientFactory);
  }

  /**
   * @param query an {@link AggregateRequest}
   * @return the resulting {@link AggregateResponse}
   */
  @Nonnull
  public AggregateResponse execute(@Nonnull final AggregateRequest query) {
    final ResultWithExpressions resultWithExpressions = buildQuery(
        query);

    // Translate the result into a response object to be passed back to the user.
    return buildResponse(resultWithExpressions);
  }

  /**
   * @param query an {@link AggregateRequest}
   * @return a {@link ResultWithExpressions}, which includes the uncollected {@link Dataset}
   */
  @SuppressWarnings("WeakerAccess")
  @Nonnull
  public ResultWithExpressions buildQuery(@Nonnull final AggregateRequest query) {
    log.info("Executing request: {}", query);

    // Build a new expression parser, and parse all of the filter and grouping expressions within
    // the query.
    final ResourcePath inputContext = ResourcePath
        .build(getFhirContext(), getDatabase(), query.getSubjectResource(),
            query.getSubjectResource().toCode(), true);

    ParserContext filterContext = buildParserContext(inputContext,
        Collections.singletonList(inputContext.getIdColumn()));
    Parser parser = new Parser(filterContext);

    final List<FhirPath> filters = new ArrayList<>();

    for (String filterExpression : query.getFilters()) {

      final FhirPath result = parser.parse(filterExpression);
      // Each filter expression must evaluate to a singular Boolean value, or a user error will be
      // thrown.
      checkUserInput(result instanceof BooleanPath,
          "Filter expression is not a non-literal boolean: " + filterExpression);
      checkUserInput(result.isSingular(),
          "Filter expression must represent a singular value: " + filterExpression);

      filters.add(result);

      filterContext = buildParserContext(inputContext.adoptDataset(result.getDataset()),
          Collections.singletonList(inputContext.getIdColumn()));
      parser = new Parser(filterContext);
    }

    // Join all filter and grouping expressions together.
    final Column idColumn = inputContext.getIdColumn();
    //Dataset<Row> filterDS = applyFilters(filterContext.getInputContext().getDataset(), filters);

    // not we need to join this to to input context
    FhirPath groupingContextPath = filters.isEmpty()
                                   ? inputContext
                                   : inputContext.adoptDataset(
                                       applyFilters(filterContext.getInputContext().getDataset(),
                                           filters).cache()
                                   );

    final ParserContext groupingContext = buildParserContext(groupingContextPath,
        Collections.singletonList(inputContext.getIdColumn()));

    final List<FhirPathAndContext> groupingParseResult = parseMaterializableExpressions(
        groupingContext, query.getGroupings(), "Grouping");
    final List<FhirPath> groupings = groupingParseResult.stream()
        .map(FhirPathAndContext::getFhirPath)
        .collect(Collectors.toList());

    // Join all filter and grouping expressions together.
    Dataset<Row> groupingsAndFilters = joinExpressionsAndFilters(groupingContextPath, groupings,
        Collections.emptyList(),
        idColumn);
    // Apply filters.
    //groupingsAndFilters = applyFilters(groupingsAndFilters, filters);

    // Remove synthetic fields from struct values (such as _fid) before grouping.
    final DatasetWithColumnMap datasetWithNormalizedGroupings = createColumns(
        groupingsAndFilters, groupings.stream().map(FhirPath::getValueColumn)
            .map(PathlingFunctions::pruneSyntheticFields).toArray(Column[]::new));

    groupingsAndFilters = datasetWithNormalizedGroupings.getDataset();

    final List<Column> groupingColumns = new ArrayList<>(
        datasetWithNormalizedGroupings.getColumnMap().values());

    // The input context will be identical to that used for the groupings and filters, except that
    // it will use the dataset that resulted from the parsing of the groupings and filters,
    // instead of just the raw resource. This is so that any aggregations that are performed
    // during the parse can use these columns for grouping, rather than the identity of each
    // resource.
    final ResourcePath aggregationContext = inputContext
        .copy(inputContext.getExpression(), groupingsAndFilters, idColumn,
            inputContext.getEidColumn(), inputContext.getValueColumn(), inputContext.isSingular(),
            Optional.empty());

    //aggregationContext.getDataset().cache();

    final ParserContext aggregationParserContext = buildParserContext(aggregationContext,
        groupingColumns);
    final Parser aggregationParser = new Parser(aggregationParserContext);

    // Parse the aggregations, and grab the updated grouping columns. When aggregations are
    // performed during an aggregation parse, the grouping columns need to be updated, as any
    // aggregation operation erases the previous columns that were built up within the dataset.
    final List<FhirPath> aggregations = parseAggregations(aggregationParser,
        query.getAggregations());

    // Join the aggregations together, using equality of the grouping column values as the join
    // condition.
    final List<Column> aggregationColumns = aggregations.stream()
        .map(FhirPath::getValueColumn)
        .collect(Collectors.toList());
    Dataset<Row> joinedAggregations = joinExpressionsByColumns(aggregations, groupingColumns);
    if (groupingColumns.isEmpty()) {
      joinedAggregations = joinedAggregations.limit(1);
    }

    // The final column selection will be the grouping columns, followed by the aggregation
    // columns.
    final List<Column> finalSelection = new ArrayList<>(groupingColumns);
    finalSelection.addAll(aggregationColumns);
    final Dataset<Row> finalDataset = joinedAggregations
        .select(finalSelection.toArray(new Column[0]))
        // This is needed to cater for the scenario where a literal value is used within an
        // aggregation expression.
        .distinct();
    return new ResultWithExpressions(finalDataset, aggregations, groupings, filters);
  }

  // /**
  //  * @param query an {@link AggregateRequest}
  //  * @return a {@link ResultWithExpressions}, which includes the uncollected {@link Dataset}
  //  */
  // @SuppressWarnings("WeakerAccess")
  // @Nonnull
  // public ResultWithExpressions buildQuery(@Nonnull final AggregateRequest query) {
  //   log.info("Executing request: {}", query);
  //
  //   // Build a new expression parser, and parse all of the filter and grouping expressions within
  //   // the query.
  //   final ResourcePath inputContext = ResourcePath
  //       .build(getFhirContext(), getDatabase(), query.getSubjectResource(),
  //           query.getSubjectResource().toCode(), true);
  //   final ParserContext groupingAndFilterContext = buildParserContext(inputContext,
  //       Collections.singletonList(inputContext.getIdColumn()));
  //   final Parser parser = new Parser(groupingAndFilterContext);
  //   final List<FhirPath> filters = parseFilters(parser, query.getFilters());
  //   final List<FhirPathAndContext> groupingParseResult = parseMaterializableExpressions(
  //       groupingAndFilterContext, query.getGroupings(), "Grouping");
  //   final List<FhirPath> groupings = groupingParseResult.stream()
  //       .map(FhirPathAndContext::getFhirPath)
  //       .collect(Collectors.toList());
  //
  //   // Join all filter and grouping expressions together.
  //   final Column idColumn = inputContext.getIdColumn();
  //   Dataset<Row> groupingsAndFilters = joinExpressionsAndFilters(inputContext, groupings, filters,
  //       idColumn);
  //   // Apply filters.
  //   groupingsAndFilters = applyFilters(groupingsAndFilters, filters);
  //
  //   // Remove synthetic fields from struct values (such as _fid) before grouping.
  //   final DatasetWithColumnMap datasetWithNormalizedGroupings = createColumns(
  //       groupingsAndFilters, groupings.stream().map(FhirPath::getValueColumn)
  //           .map(PathlingFunctions::pruneSyntheticFields).toArray(Column[]::new));
  //
  //   groupingsAndFilters = datasetWithNormalizedGroupings.getDataset();
  //
  //   final List<Column> groupingColumns = new ArrayList<>(
  //       datasetWithNormalizedGroupings.getColumnMap().values());
  //
  //   // The input context will be identical to that used for the groupings and filters, except that
  //   // it will use the dataset that resulted from the parsing of the groupings and filters,
  //   // instead of just the raw resource. This is so that any aggregations that are performed
  //   // during the parse can use these columns for grouping, rather than the identity of each
  //   // resource.
  //   final ResourcePath aggregationContext = inputContext
  //       .copy(inputContext.getExpression(), groupingsAndFilters, idColumn,
  //           inputContext.getEidColumn(), inputContext.getValueColumn(), inputContext.isSingular(),
  //           Optional.empty());
  //
  //   //aggregationContext.getDataset().cache();
  //
  //   final ParserContext aggregationParserContext = buildParserContext(aggregationContext,
  //       groupingColumns);
  //   final Parser aggregationParser = new Parser(aggregationParserContext);
  //
  //   // Parse the aggregations, and grab the updated grouping columns. When aggregations are
  //   // performed during an aggregation parse, the grouping columns need to be updated, as any
  //   // aggregation operation erases the previous columns that were built up within the dataset.
  //   final List<FhirPath> aggregations = parseAggregations(aggregationParser,
  //       query.getAggregations());
  //
  //   // Join the aggregations together, using equality of the grouping column values as the join
  //   // condition.
  //   final List<Column> aggregationColumns = aggregations.stream()
  //       .map(FhirPath::getValueColumn)
  //       .collect(Collectors.toList());
  //   Dataset<Row> joinedAggregations = joinExpressionsByColumns(aggregations, groupingColumns);
  //   if (groupingColumns.isEmpty()) {
  //     joinedAggregations = joinedAggregations.limit(1);
  //   }
  //
  //   // The final column selection will be the grouping columns, followed by the aggregation
  //   // columns.
  //   final List<Column> finalSelection = new ArrayList<>(groupingColumns);
  //   finalSelection.addAll(aggregationColumns);
  //   final Dataset<Row> finalDataset = joinedAggregations
  //       .select(finalSelection.toArray(new Column[0]))
  //       // This is needed to cater for the scenario where a literal value is used within an
  //       // aggregation expression.
  //       .distinct();
  //   return new ResultWithExpressions(finalDataset, aggregations, groupings, filters);
  // }

  @Nonnull
  private List<FhirPath> parseAggregations(@Nonnull final Parser parser,
      @Nonnull final Collection<String> aggregations) {
    return aggregations.stream().map(aggregation -> {
      final FhirPath result = parser.parse(aggregation);
      // Aggregation expressions must evaluate to a singular, Materializable path, or a user error
      // will be returned.
      checkUserInput(result instanceof Materializable,
          "Aggregation expression is not of a supported type: " + aggregation);
      checkUserInput(result.isSingular(),
          "Aggregation expression does not evaluate to a singular value: " + aggregation);
      return result;
    }).collect(Collectors.toList());
  }

  @Nonnull
  AggregateResponse buildResponse(
      @Nonnull final ResultWithExpressions resultWithExpressions) {
    // If explain queries is on, print out a query plan to the log.
    if (getConfiguration().getSpark().getExplainQueries()) {
      log.debug("$aggregate query plan:");
      resultWithExpressions.getDataset().explain(true);
    }

    // Execute the query.
    final List<Row> rows = resultWithExpressions.getDataset().collectAsList();

    // Map each of the rows in the result to a grouping in the response object.
    final List<AggregateResponse.Grouping> groupings = rows.stream()
        .map(mapRowToGrouping(resultWithExpressions.getParsedAggregations(),
            resultWithExpressions.getParsedGroupings(),
            resultWithExpressions.getParsedFilters()))
        .collect(Collectors.toList());

    return new AggregateResponse(groupings);
  }

  @Nonnull
  @SuppressWarnings("unchecked")
  private Function<Row, AggregateResponse.Grouping> mapRowToGrouping(
      @Nonnull final List<FhirPath> aggregations, @Nonnull final List<FhirPath> groupings,
      @Nonnull final Collection<FhirPath> filters) {
    return row -> {
      final List<Optional<Type>> labels = new ArrayList<>();
      final List<Optional<Type>> results = new ArrayList<>();

      for (int i = 0; i < groupings.size(); i++) {
        final Materializable<Type> grouping = (Materializable<Type>) groupings.get(i);
        // Delegate to the `getValueFromRow` method within each Materializable path class to extract 
        // the Type value from the Row in the appropriate way.
        final Optional<Type> label = grouping.getValueFromRow(row, i);
        labels.add(label);
      }

      for (int i = 0; i < aggregations.size(); i++) {
        //noinspection rawtypes
        final Materializable aggregation = (Materializable<Type>) aggregations.get(i);
        // Delegate to the `getValueFromRow` method within each Materializable path class to extract 
        // the Type value from the Row in the appropriate way.
        final Optional<Type> result = aggregation.getValueFromRow(row, i + groupings.size());
        results.add(result);
      }

      // Build a drill-down FHIRPath expression for inclusion with the returned grouping.
      final Optional<String> drillDown = new DrillDownBuilder(labels, groupings, filters).build();

      return new AggregateResponse.Grouping(labels, results, drillDown);
    };
  }

  @Value
  static class ResultWithExpressions {

    @Nonnull
    Dataset<Row> dataset;

    @Nonnull
    List<FhirPath> parsedAggregations;

    @Nonnull
    List<FhirPath> parsedGroupings;

    @Nonnull
    Collection<FhirPath> parsedFilters;

  }

}
