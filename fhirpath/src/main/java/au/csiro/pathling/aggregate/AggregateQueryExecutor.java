/*
 * Copyright 2023 Commonwealth Scientific and Industrial Research
 * Organisation (CSIRO) ABN 41 687 119 230.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package au.csiro.pathling.aggregate;

import au.csiro.pathling.QueryExecutor;
import au.csiro.pathling.config.QueryConfiguration;
import au.csiro.pathling.fhirpath.annotations.NotImplemented;
import au.csiro.pathling.fhirpath.collection.Collection;
import au.csiro.pathling.fhirpath.parser.Parser;
import au.csiro.pathling.io.Database;
import au.csiro.pathling.io.source.DataSource;
import au.csiro.pathling.query.QueryParser;
import au.csiro.pathling.terminology.TerminologyServiceFactory;
import au.csiro.pathling.view.AggregationView;
import au.csiro.pathling.view.ExecutionContext;
import ca.uhn.fhir.context.FhirContext;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import javax.annotation.Nonnull;
import lombok.Value;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

/**
 * Builds the overall query responsible for executing an aggregate request.
 *
 * @author John Grimes
 */
@Slf4j
@NotImplemented
public class AggregateQueryExecutor extends QueryExecutor {

  /**
   * Constructs a new {@link AggregateQueryExecutor}.
   *
   * @param configuration A {@link QueryConfiguration} object to control the behaviour of the
   * executor
   * @param fhirContext A {@link FhirContext} for doing FHIR stuff
   * @param sparkSession A {@link SparkSession} for resolving Spark queries
   * @param dataSource A {@link Database} for retrieving resources
   * @param terminologyServiceFactory A {@link TerminologyServiceFactory} for resolving terminology
   */
  public AggregateQueryExecutor(@Nonnull final QueryConfiguration configuration,
      @Nonnull final FhirContext fhirContext, @Nonnull final SparkSession sparkSession,
      @Nonnull final DataSource dataSource,
      @Nonnull final Optional<TerminologyServiceFactory> terminologyServiceFactory) {
    super(configuration, fhirContext, sparkSession, dataSource, terminologyServiceFactory);
  }

  /**
   * @param query an {@link AggregateRequest}
   * @return a {@link ResultWithExpressions}, which includes the uncollected {@link Dataset}
   */
  @SuppressWarnings("WeakerAccess")
  @Nonnull
  public ResultWithExpressions buildQuery(@Nonnull final AggregateRequest query) {
    log.info("Executing request: {}", query);
    final QueryParser queryParser = new QueryParser(new Parser());
    final AggregationView aggregationView = queryParser.toView(query);
    aggregationView.printTree();
    final Dataset<Row> resultDataset = aggregationView.evaluate(newContext());
    return new ResultWithExpressions(resultDataset, Collections.emptyList(),
        Collections.emptyList(), Collections.emptyList());
  }

  protected ExecutionContext newContext() {
    return new ExecutionContext(sparkSession, fhirContext, dataSource);
  }

  //
  // /// Set up the parser context for the grouping and filtering expressions.
  // final ResourcePath inputContext = ResourcePath
  //     .build(getFhirContext(), getDataSource(), query.getSubjectResource(),
  //         query.getSubjectResource().toCode(), true);
  // final ParserContext groupingFilteringContext = new ParserContext(inputContext, fhirContext,
  //     sparkSession, dataSource,
  //     terminologyServiceFactory, Collections.singletonList(inputContext.getIdColumn()));
  //
  // // Parse the filter expressions.
  // final Optional<Dataset<Row>> filteredDataset;
  // final List<FhirPath> filters;
  // if (query.getFilters().isEmpty()) {
  //   filteredDataset = Optional.empty();
  //   filters = Collections.emptyList();
  // } else {
  //   filters = parseExpressions(groupingFilteringContext, query.getFilters());
  //   validateFilters(filters);
  //   final Dataset<Row> filterDataset = filters.get(filters.size() - 1).getDataset();
  //   final Optional<Column> filterConstraint = filters.stream()
  //       .map(FhirPath::getValueColumn)
  //       .reduce(Column::and);
  //   filteredDataset = Optional.of(filterConstraint.map(filterDataset::filter)
  //       .orElse(filterDataset));
  // }
  //
  // // Parse the grouping expressions.
  // final Optional<Dataset<Row>> groupingFilteringDataset;
  // final List<FhirPath> groupings;
  // if (query.getGroupings().isEmpty()) {
  //   groupingFilteringDataset = filteredDataset;
  //   groupings = Collections.emptyList();
  // } else {
  //   groupings = parseExpressions(groupingFilteringContext, query.getGroupings(),
  //       filteredDataset);
  //   validateGroupings(groupings);
  //   groupingFilteringDataset = Optional.of(groupings.get(groupings.size() - 1).getDataset());
  // }
  //
  // // Remove synthetic fields from struct values (such as _fid) before grouping.
  // final Optional<Dataset<Row>> prunedDataset;
  // final List<Column> prunedGroupings;
  // if (groupingFilteringDataset.isPresent()) {
  //   final QueryHelpers.DatasetWithColumnMap datasetWithNormalizedGroupings = createColumns(
  //       groupingFilteringDataset.get(), groupings.stream().map(FhirPath::getValueColumn)
  //           .map(SqlExpressions::pruneSyntheticFields).toArray(Column[]::new));
  //   prunedDataset = Optional.of(datasetWithNormalizedGroupings.getDataset());
  //   prunedGroupings = new ArrayList<>(
  //       datasetWithNormalizedGroupings.getColumnMap().values());
  // } else {
  //   prunedDataset = Optional.empty();
  //   prunedGroupings = Collections.emptyList();
  // }
  //
  // // Parse the aggregation expressions.
  // final ParserContext aggregationContext = groupingFilteringContext.withGroupingColumns(
  //     prunedGroupings);
  // final List<FhirPath> aggregations = parseExpressions(aggregationContext,
  //     query.getAggregations(), prunedDataset);
  // validateAggregations(aggregations);
  // final List<Column> aggregationColumns = aggregations.stream()
  //     .map(FhirPath::getValueColumn)
  //     .collect(toList());
  // final FhirPath lastAggregation = aggregations.get(aggregations.size() - 1);
  // Dataset<Row> aggregationDataset = lastAggregation.getDataset();
  // // If the result of the aggregation expressions is not actually aggregated, we need to apply
  // // a final aggregation step that reduces it down to one row per group.
  // if (!aggregationContext.getNesting().isRootErased()) {
  //   aggregationDataset = applyAggregation(aggregationContext, aggregationColumns, lastAggregation,
  //       aggregationDataset);
  // }
  //
  // // The final column selection will be the grouping columns, followed by the aggregation
  // // columns.
  // final Column[] selection = Stream.concat(
  //         labelColumns(prunedGroupings.stream(), labelsAsStream(query.getGroupingsWithLabels())),
  //         labelColumns(aggregationColumns.stream(), labelsAsStream(query.getAggregationsWithLabels()))
  //     )
  //     .toArray(Column[]::new);
  // final Dataset<Row> finalDataset = aggregationDataset.select(selection);
  //
  // return new ResultWithExpressions(finalDataset, aggregations, groupings, filters);
  //   return null;
  // }

  // private void validateAggregations(@Nonnull final java.util.Collection<Collection> aggregations) {
  //   for (final Collection aggregation : aggregations) {
  //     // An aggregation expression must be able to be extracted into a FHIR value.
  //     checkUserInput(aggregation instanceof Materializable,
  //         "Aggregation expression is not of a supported type: " + aggregation.getExpression());
  //     // An aggregation expression must be singular, relative to its input context.
  //     checkUserInput(aggregation.isSingular(),
  //         "Aggregation expression does not evaluate to a singular value: "
  //             + aggregation.getExpression());
  //   }
  // }
  //
  // private void validateGroupings(@Nonnull final java.util.Collection<Collection> groupings) {
  //   for (final Collection grouping : groupings) {
  //     // A grouping expression must be able to be extracted into a FHIR value.
  //     checkUserInput(grouping instanceof Materializable,
  //         "Grouping expression is not of a supported type: " + grouping.getExpression());
  //   }
  // }
  //
  // @Nonnull
  // private static Dataset<Row> applyAggregation(@Nonnull final ParserContext context,
  //     @Nonnull final List<Column> aggregationColumns, @Nonnull final Collection lastAggregation,
  //     @Nonnull final Dataset<Row> disaggregated) {
  //   // Group the dataset by the grouping columns, and take the first row.
  //   final Column[] groupBy = context.getGroupingColumns().toArray(new Column[0]);
  //   final String aggregatedColumnName = randomAlias();
  //   final Dataset<Row> aggregated = disaggregated.groupBy(groupBy)
  //       .agg(first(lastAggregation.getValueColumn()).as(aggregatedColumnName));
  //   // Replace the last column with the aggregated column.
  //   aggregationColumns.remove(aggregationColumns.size() - 1);
  //   aggregationColumns.add(col(aggregatedColumnName));
  //   return aggregated;
  // }

  @Value
  public static class ResultWithExpressions {

    @Nonnull
    Dataset<Row> dataset;

    @Nonnull
    List<Collection> parsedAggregations;

    @Nonnull
    List<Collection> parsedGroupings;

    @Nonnull
    java.util.Collection<Collection> parsedFilters;

  }
}