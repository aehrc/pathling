/*
 * Copyright © 2018-2020, Commonwealth Scientific and Industrial Research
 * Organisation (CSIRO) ABN 41 687 119 230. Licensed under the CSIRO Open Source
 * Software Licence Agreement.
 */

package au.csiro.pathling.aggregate;

import static au.csiro.pathling.QueryHelpers.firstNColumns;
import static au.csiro.pathling.QueryHelpers.joinOnColumns;
import static au.csiro.pathling.utilities.Preconditions.check;
import static au.csiro.pathling.utilities.Preconditions.checkNotNull;
import static au.csiro.pathling.utilities.Preconditions.checkUserInput;

import au.csiro.pathling.Configuration;
import au.csiro.pathling.QueryExecutor;
import au.csiro.pathling.QueryHelpers.DatasetWithColumns;
import au.csiro.pathling.QueryHelpers.JoinType;
import au.csiro.pathling.aggregate.AggregateRequest.Aggregation;
import au.csiro.pathling.aggregate.AggregateRequest.Grouping;
import au.csiro.pathling.fhir.TerminologyClient;
import au.csiro.pathling.fhir.TerminologyClientFactory;
import au.csiro.pathling.fhirpath.FhirPath;
import au.csiro.pathling.fhirpath.Materializable;
import au.csiro.pathling.fhirpath.ResourceDefinition;
import au.csiro.pathling.fhirpath.ResourcePath;
import au.csiro.pathling.fhirpath.element.BooleanPath;
import au.csiro.pathling.fhirpath.parser.AggregationParserContext;
import au.csiro.pathling.fhirpath.parser.Parser;
import au.csiro.pathling.fhirpath.parser.ParserContext;
import au.csiro.pathling.io.ResourceReader;
import ca.uhn.fhir.context.FhirContext;
import ca.uhn.fhir.context.RuntimeResourceDefinition;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.Getter;
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
    log.info("Executing request");

    // Build a new expression parser, and parse all of the filter and grouping expressions within
    // the query.
    final ParserContext groupingAndFilterContext = buildParserContext(query.getSubjectResource());
    final Parser parser = new Parser(groupingAndFilterContext);
    final List<FhirPath> filters = parseFilters(parser, query.getFilters());
    final List<FhirPath> groupings = parseGroupings(parser, query.getGroupings());

    // Join all filter and grouping expressions together.
    final FhirPath inputContext = groupingAndFilterContext.getInputContext();
    final List<FhirPath> groupingsAndFilters = new ArrayList<>();
    groupingsAndFilters.add(inputContext);
    groupingsAndFilters.addAll(filters);
    groupingsAndFilters.addAll(groupings);
    Dataset<Row> groupingsAndFiltersDataset = joinExpressions(groupingsAndFilters);

    // Apply filters.
    groupingsAndFiltersDataset = applyFilters(groupingsAndFiltersDataset, filters);

    // Create a new parser context for aggregation that includes the groupings.
    final List<Column> groupingColumns = groupings.stream()
        .map(FhirPath::getValueColumn)
        .collect(Collectors.toList());

    // The input context will be identical to that used for the groupings and filters, except that 
    // it will use the dataset that resulted from the parsing of the groupings and filters, 
    // instead of just the raw resource. This is so that any aggregations that are performed 
    // during the parse can use these columns for grouping, rather than the identity of each 
    // resource.
    check(inputContext instanceof ResourcePath);
    final ResourceDefinition definition = ((ResourcePath) inputContext).getDefinition();
    final ResourcePath aggregationInputContext = new ResourcePath(inputContext.getExpression(),
        groupingsAndFiltersDataset, inputContext.getIdColumn(), inputContext.getValueColumn(),
        inputContext.isSingular(), definition);

    // Parse the aggregations, and grab the updated grouping columns. When aggregations are 
    // performed during an aggregation parse, the grouping columns need to be updated, as any 
    // aggregation operation erases the previous columns that were built up within the dataset.
    final AggregationParseResult aggregationParseResult = parseAggregations(
        query.getAggregations(), aggregationInputContext, groupingsAndFiltersDataset,
        groupingColumns);
    final List<FhirPath> aggregations = aggregationParseResult.getAggregations();
    final List<Column> updatedGroupingColumns = aggregationParseResult.getGroupingColumns();

    // Join the aggregations together, using equality of the grouping column values as the join 
    // condition.
    final List<Column> aggregationColumns = aggregations.stream()
        .map(FhirPath::getValueColumn)
        .collect(Collectors.toList());
    final DatasetWithColumns finalDatasetWithColumns = joinAggregations(aggregations,
        updatedGroupingColumns);

    // The final column selection will be the grouping columns, followed by the aggregation 
    // columns.
    final List<Column> finalSelection = new ArrayList<>();
    finalSelection.addAll(finalDatasetWithColumns.getColumns());
    finalSelection.addAll(aggregationColumns);
    final Dataset<Row> finalDataset = finalDatasetWithColumns.getDataset()
        .select(finalSelection.toArray(new Column[]{}));

    // Translate the result into a response object to be passed back to the user.
    return buildResponse(finalDataset, aggregations, groupings, filters);
  }

  @Nonnull
  private AggregationParserContext buildAggregationParserContext(
      @Nonnull final ResourcePath inputContext, @Nonnull final Dataset<Row> dataset,
      @Nonnull final List<Column> groupingColumns) {
    final ResourcePath newInputContext = buildAggregationInputContext(inputContext, dataset);
    return new AggregationParserContext(newInputContext, Optional.empty(), getFhirContext(),
        getSparkSession(), getResourceReader(), getTerminologyClient(),
        getTerminologyClientFactory(), groupingColumns);
  }

  @Nonnull
  private ResourcePath buildAggregationInputContext(@Nonnull final ResourcePath inputContext,
      @Nonnull final Dataset<Row> dataset) {
    final String resourceCode = inputContext.getResourceType().toCode();
    final RuntimeResourceDefinition hapiDefinition = getFhirContext()
        .getResourceDefinition(resourceCode);
    final ResourceDefinition definition = new ResourceDefinition(inputContext.getResourceType(),
        hapiDefinition);

    // We use an empty expression here, as we are referring to the input context implicitly. The 
    // expression is not singular in the case of an aggregation, as the input context is the set of 
    // resources within the grouping that the aggregation is being applied to.
    return new ResourcePath("", dataset, inputContext.getIdColumn(), inputContext.getValueColumn(),
        false, definition);
  }

  @Nonnull
  private AggregationParseResult parseAggregations(
      @Nonnull final Iterable<Aggregation> aggregations, @Nonnull final ResourcePath inputContext,
      @Nonnull final Dataset<Row> groupingsAndFiltersDataset,
      @Nonnull final List<Column> groupingColumns) {
    final List<FhirPath> parsedAggregations = new ArrayList<>();
    @Nullable List<Column> updatedGroupingColumns = null;

    for (final Aggregation aggregation : aggregations) {
      // We need to create a new parser context and parser for each aggregation, as the grouping
      // columns within the context are mutated by aggregations during the parse.
      final AggregationParserContext aggregationContext = buildAggregationParserContext(
          inputContext, groupingsAndFiltersDataset, groupingColumns);
      final Parser parser = new Parser(aggregationContext);

      // Aggregation expressions must evaluate to a singular, Materializable path, or a user error
      // will be returned.
      final String expression = aggregation.getExpression();
      final FhirPath result = parser.parse(expression);
      checkUserInput(result instanceof Materializable,
          "Aggregation expression is not of a supported type: " + expression);
      checkUserInput(result.isSingular(),
          "Aggregation expression does not evaluate to a singular value: " + expression);

      parsedAggregations.add(result);
      updatedGroupingColumns = aggregationContext.getGroupingColumns();
    }

    checkNotNull(updatedGroupingColumns);
    return new AggregationParseResult(parsedAggregations, updatedGroupingColumns);
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
  private static DatasetWithColumns joinAggregations(@Nonnull final List<FhirPath> expressions,
      @Nonnull final List<Column> groupings) {
    check(!expressions.isEmpty());

    // If there is only one aggregation, we don't need to do any joining.
    if (expressions.size() == 1) {
      final FhirPath aggregation = expressions.get(0);
      return new DatasetWithColumns(aggregation.getDataset(), groupings);
    }

    @Nullable Dataset<Row> result = null;
    @Nullable List<Column> columns = null;

    for (int i = 0; i < expressions.size(); i++) {
      if (i == 0) {
        result = expressions.get(i).getDataset();
      } else {
        // Take the grouping columns from each aggregation dataset, and join the datasets together
        // based on their equality.
        final Dataset<Row> current = expressions.get(i).getDataset();
        final List<Column> currentColumns = firstNColumns(current, groupings.size());
        final List<Column> previousColumns = firstNColumns(result, groupings.size());
        final DatasetWithColumns datasetWithColumns = joinOnColumns(result, previousColumns,
            current,
            currentColumns,
            JoinType.LEFT_OUTER);
        result = datasetWithColumns.getDataset();
        columns = datasetWithColumns.getColumns();
      }
    }

    checkNotNull(result);
    checkNotNull(columns);
    return new DatasetWithColumns(result, columns);
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

  @Getter
  private static class AggregationParseResult {

    @Nonnull
    private final List<FhirPath> aggregations;

    @Nonnull
    private final List<Column> groupingColumns;

    private AggregationParseResult(
        @Nonnull final List<FhirPath> aggregations,
        @Nonnull final List<Column> groupingColumns) {
      this.aggregations = aggregations;
      this.groupingColumns = groupingColumns;
    }

  }

}
