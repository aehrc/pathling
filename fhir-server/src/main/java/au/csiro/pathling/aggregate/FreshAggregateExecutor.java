/*
 * Copyright © 2018-2020, Commonwealth Scientific and Industrial Research
 * Organisation (CSIRO) ABN 41 687 119 230. Licensed under the CSIRO Open Source
 * Software Licence Agreement.
 */

package au.csiro.pathling.aggregate;

import static au.csiro.pathling.QueryHelpers.join;
import static au.csiro.pathling.utilities.Preconditions.checkUserInput;

import au.csiro.pathling.Configuration;
import au.csiro.pathling.QueryExecutor;
import au.csiro.pathling.QueryHelpers.JoinType;
import au.csiro.pathling.aggregate.AggregateRequest.Aggregation;
import au.csiro.pathling.aggregate.AggregateRequest.Grouping;
import au.csiro.pathling.fhir.TerminologyClient;
import au.csiro.pathling.fhir.TerminologyClientFactory;
import au.csiro.pathling.fhirpath.FhirPath;
import au.csiro.pathling.fhirpath.Materializable;
import au.csiro.pathling.fhirpath.ResourcePath;
import au.csiro.pathling.fhirpath.element.BooleanPath;
import au.csiro.pathling.fhirpath.parser.Parser;
import au.csiro.pathling.fhirpath.parser.ParserContext;
import au.csiro.pathling.io.ResourceReader;
import ca.uhn.fhir.context.FhirContext;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.hl7.fhir.r4.model.Type;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;

/**
 * This class knows how to take an {@link AggregateRequest} and execute it, returning the result as
 * an {@link AggregateResponse}.
 *
 * @author John Grimes
 */
@Component
@ConditionalOnProperty(prefix = "pathling", value = "caching.enabled", havingValue = "false")
@Slf4j
public class FreshAggregateExecutor extends QueryExecutor implements AggregateExecutor {

  /**
   * @param configuration A {@link Configuration} object to control the behaviour of the executor
   * @param fhirContext A {@link FhirContext} for doing FHIR stuff
   * @param sparkSession A {@link SparkSession} for resolving Spark queries
   * @param resourceReader A {@link ResourceReader} for retrieving resources
   * @param terminologyClient A {@link TerminologyClient} for resolving terminology queries
   * @param terminologyClientFactory A {@link TerminologyClientFactory} for resolving terminology
   */
  public FreshAggregateExecutor(@Nonnull final Configuration configuration,
      @Nonnull final FhirContext fhirContext, @Nonnull final SparkSession sparkSession,
      @Nonnull final ResourceReader resourceReader,
      @Nonnull final Optional<TerminologyClient> terminologyClient,
      @Nonnull final Optional<TerminologyClientFactory> terminologyClientFactory) {
    super(configuration, fhirContext, sparkSession, resourceReader, terminologyClient,
        terminologyClientFactory);
  }

  @Nonnull
  @Override
  public AggregateResponse execute(@Nonnull final AggregateRequest query) {
    log.info("Executing request: {}", query);

    // Build a new expression parser, and parse all of the filter and grouping expressions within
    // the query.
    final ResourcePath inputContext = ResourcePath
        .build(getFhirContext(), getResourceReader(), query.getSubjectResource(),
            query.getSubjectResource().toCode(), true);
    final ParserContext groupingAndFilterContext = buildParserContext(inputContext);
    final Parser parser = new Parser(groupingAndFilterContext);
    final List<FhirPath> filters = parseFilters(parser, query.getFilters());
    final List<FhirPath> groupings = parseGroupings(parser, query.getGroupings());

    // Join all filter and grouping expressions together.
    final Column idColumn = inputContext.getIdColumn();
    Dataset<Row> groupingsAndFiltersDataset = filters.size() + groupings.size() > 0
                                              ? joinGroupingsAndFilters(groupings, filters,
        idColumn)
                                              : inputContext.getDataset();

    // Apply filters.
    groupingsAndFiltersDataset = applyFilters(groupingsAndFiltersDataset, filters);

    // Create a new parser context for aggregation that includes the groupings.
    final Optional<List<Column>> groupingColumns = Optional.of(groupings.stream()
        .map(FhirPath::getValueColumn)
        .collect(Collectors.toList()));

    // The input context will be identical to that used for the groupings and filters, except that
    // it will use the dataset that resulted from the parsing of the groupings and filters,
    // instead of just the raw resource. This is so that any aggregations that are performed
    // during the parse can use these columns for grouping, rather than the identity of each
    // resource.
    final ResourcePath aggregationContext = inputContext
        .copy(inputContext.getExpression(), groupingsAndFiltersDataset, idColumn,
            inputContext.getEidColumn(), inputContext.getValueColumn(), inputContext.isSingular(),
            Optional.empty());
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
    final Dataset<Row> joinedAggregations = joinExpressions(aggregations);

    // The final column selection will be the grouping columns, followed by the aggregation
    // columns.
    final List<Column> finalSelection = new ArrayList<>();
    groupingColumns.ifPresent(finalSelection::addAll);
    finalSelection.addAll(aggregationColumns);
    final Dataset<Row> finalDataset = joinedAggregations
        .select(finalSelection.toArray(new Column[0]));

    // Translate the result into a response object to be passed back to the user.
    return buildResponse(finalDataset, aggregations, groupings, filters);
  }

  @Nonnull
  private Dataset<Row> joinGroupingsAndFilters(@Nonnull final Collection<FhirPath> groupings,
      @Nonnull final Collection<FhirPath> filters, @Nonnull final Column idColumn) {
    final List<Dataset<Row>> datasets = groupings.stream()
        .map(grouping -> {
          // We need to remove any trailing null values from non-empty collections, so that aggregations
          // do not count non-empty collections in the empty collection grouping. We do this by joining
          // the distinct set of resource IDs to the dataset using an outer join, where the argument value
          // is true.
          final Dataset<Row> distinctIds = grouping.getDataset().select(idColumn).distinct();
          return join(grouping.getDataset(), idColumn, distinctIds,
              idColumn, grouping.getValueColumn().isNotNull(), JoinType.RIGHT_OUTER);
        }).collect(Collectors.toList());
    datasets.addAll(filters.stream()
        .map(FhirPath::getDataset)
        .collect(Collectors.toList()));

    Dataset<Row> result = datasets.get(0);

    for (int i = 1; i < datasets.size(); i++) {
      final Dataset<Row> current = datasets.get(i);
      result = join(result, idColumn, current, idColumn, JoinType.LEFT_OUTER);
    }

    return result;
  }

  @Nonnull
  private List<FhirPath> parseGroupings(@Nonnull final Parser parser,
      @Nonnull final Collection<Grouping> groupings) {
    return groupings.stream()
        .map(grouping -> {
          final String expression = grouping.getExpression();
          final FhirPath result = parser.parse(expression);
          // Each grouping expression must evaluate to a Materializable path, or a user error will
          // be thrown. There is no requirement for it to be singular, multiple values will result
          // in the resource being counted within multiple different groupings.
          checkUserInput(result instanceof Materializable,
              "Grouping expression is not of a supported type: " + expression);
          return result;
        }).collect(Collectors.toList());
  }

  @Nonnull
  private List<FhirPath> parseFilters(@Nonnull final Parser parser,
      @Nonnull final Collection<String> filters) {
    return filters.stream().map(expression -> {
      final FhirPath result = parser.parse(expression);
      // Each filter expression must evaluate to a singular Boolean value, or a user error will be
      // thrown.
      checkUserInput(result instanceof BooleanPath,
          "Filter expression is not a non-literal boolean: " + expression);
      checkUserInput(result.isSingular(),
          "Filter expression must represent a singular value: " + expression);
      return result;
    }).collect(Collectors.toList());
  }

  @Nonnull
  private List<FhirPath> parseAggregations(@Nonnull final Parser parser,
      @Nonnull final Collection<Aggregation> aggregations) {
    return aggregations.stream().map(aggregation -> {
      final String expression = aggregation.getExpression();
      final FhirPath result = parser.parse(expression);
      // Aggregation expressions must evaluate to a singular, Materializable path, or a user error
      // will be returned.
      checkUserInput(result instanceof Materializable,
          "Aggregation expression is not of a supported type: " + expression);
      checkUserInput(result.isSingular(),
          "Aggregation expression does not evaluate to a singular value: " + expression);
      return result;
    }).collect(Collectors.toList());
  }

  @Nonnull
  private AggregateResponse buildResponse(@Nonnull final Dataset<Row> dataset,
      @Nonnull final List<FhirPath> parsedAggregations,
      @Nonnull final List<FhirPath> parsedGroupings,
      @Nonnull final Collection<FhirPath> parsedFilters) {
    // If explain queries is on, print out a query plan to the log.
    if (getConfiguration().getSpark().getExplainQueries()) {
      log.info("$aggregate query plan:");
      dataset.explain(true);
    }

    // Execute the query.
    final List<Row> rows = dataset.collectAsList();

    // Map each of the rows in the result to a grouping in the response object.
    final List<AggregateResponse.Grouping> groupings = rows.stream()
        .map(mapRowToGrouping(parsedAggregations, parsedGroupings, parsedFilters))
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
        final Materializable aggregation = (Materializable<Type>) aggregations.get(i);
        // Delegate to the `getValueFromRow` method within each Materializable path class to extract 
        // the Type value from the Row in the appropriate way.
        final Optional<Type> result = aggregation.getValueFromRow(row, i + groupings.size());
        results.add(result);
      }

      // Build a drill-down FHIRPath expression for inclusion with the returned grouping.
      final String drillDown = new DrillDownBuilder(labels, groupings, filters)
          .build();

      return new AggregateResponse.Grouping(labels, results, drillDown);
    };
  }

}
