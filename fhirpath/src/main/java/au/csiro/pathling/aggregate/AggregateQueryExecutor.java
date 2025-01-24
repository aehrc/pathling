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

import static au.csiro.pathling.utilities.Strings.randomAlias;
import static java.util.Objects.nonNull;
import static java.util.Objects.requireNonNull;

import au.csiro.pathling.QueryExecutor;
import au.csiro.pathling.config.QueryConfiguration;
import au.csiro.pathling.fhirpath.FhirPath;
import au.csiro.pathling.fhirpath.execution.EvaluatedPath;
import au.csiro.pathling.fhirpath.execution.FhirpathEvaluator;
import au.csiro.pathling.fhirpath.execution.JoinResolver;
import au.csiro.pathling.fhirpath.execution.MultiFhirpathEvaluator.ManyFactory;
import au.csiro.pathling.fhirpath.parser.Parser;
import au.csiro.pathling.io.Database;
import au.csiro.pathling.io.source.DataSource;
import au.csiro.pathling.query.ExpressionWithLabel;
import au.csiro.pathling.sql.SqlExpressions;
import au.csiro.pathling.terminology.TerminologyServiceFactory;
import ca.uhn.fhir.context.FhirContext;
import jakarta.annotation.Nonnull;
import java.util.List;
import java.util.Optional;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import lombok.Value;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.ArrayType;

/**
 * Builds the overall query responsible for executing an aggregate request.
 *
 * @author John Grimes
 */
@Slf4j
public class AggregateQueryExecutor extends QueryExecutor {

  @Nonnull
  private final Parser parser = new Parser();

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

    final List<FhirPath> grouppingPaths = query.getGroupings().stream()
        .map(parser::parse)
        .toList();

    final List<FhirPath> filterPaths = query.getFilters().stream()
        .map(parser::parse)
        .toList();

    final List<FhirPath> aggPaths = query.getAggregations().stream()
        .map(parser::parse)
        .toList();

    final List<FhirPath> contextPaths = Stream.of(grouppingPaths, filterPaths, aggPaths)
        .flatMap(List::stream)
        .toList();

    final FhirpathEvaluator fhirEvaluator = ManyFactory.fromPaths(
        query.getSubjectResource(),
        fhirContext, dataSource,
        contextPaths).create(query.getSubjectResource());

    final List<EvaluatedPath> evaluatedFilters = fhirEvaluator.evaluateWithPath(filterPaths);

    final Optional<Column> maybeFilter = evaluatedFilters.stream()
        .map(EvaluatedPath::getColumnValue)
        .reduce(Column::and);

    final Dataset<Row> filteredDataset = maybeFilter.map(
            fhirEvaluator.createInitialDataset()::filter)
        .orElse(fhirEvaluator.createInitialDataset());

    // compute aggregation bases 
    // TODO: for now assume that the last element on the path is the aggregation function

    final List<EvaluatedPath> evaluatedGoupings = fhirEvaluator.evaluateWithPath(grouppingPaths);

    // prepend the materialized grouping columns to the dataset
    final Dataset<Row> inputDataset = filteredDataset.select(
        Stream.concat(
            evaluatedGoupings.stream().map(c -> c.getColumnValue().alias(randomAlias())),
            Stream.of(filteredDataset.columns()).map(functions::col)
        ).toArray(Column[]::new)
    );

    // explode the grouping columns
    final Dataset<Row> expandeDataset = Stream.of(inputDataset.schema().fields())
        .limit(evaluatedGoupings.size())
        .reduce(inputDataset, (dataset, field) -> {
          if (field.dataType() instanceof ArrayType) {
            return dataset.withColumn(field.name(),
                functions.explode_outer(functions.col(field.name())));
          }
          return dataset;
        }, (dataset1, dataset2) -> dataset1);

    // normalize the grouping columns (prune the synthetic fields)
    final Column[] normalizedGroupingColumns = Stream.of(expandeDataset.columns())
        .limit(evaluatedGoupings.size())
        .map(c -> SqlExpressions.pruneSyntheticFields(functions.col(c)).alias(c))
        .toArray(Column[]::new);

    // compute the aggregation from the initial dataset
    final List<Column> dataColumns = Stream.of(filteredDataset.columns())
        // for external referenced use collect map otherwise use collect list
        .map(c -> c.contains("@")
                  ? JoinResolver.collect_map(functions.col(c)).alias(c)
                  : functions.collect_list(functions.col(c)).alias(c))
        .toList();

    //then we need to group by the grouping columns and collect the resource column to a list
    final Dataset<Row> grouppedAggSource = expandeDataset.groupBy(normalizedGroupingColumns)
        .agg(dataColumns.get(0), dataColumns.subList(1, dataColumns.size()).toArray(Column[]::new));

    final List<EvaluatedPath> evaluatedAggs = fhirEvaluator.evaluateWithPath(aggPaths);
    final Column[] aggColumns = evaluatedAggs.stream()
        .map(c -> c.getColumnValue().alias(randomAlias()))
        .toArray(Column[]::new);

    final Dataset<Row> resultDataset = grouppedAggSource
        .select(Stream.concat(
            alias(normalizedGroupingColumns, toLabels(query.getGroupingsWithLabels())),
            alias(aggColumns, toLabels(query.getAggregationsWithLabels()))
        ).toArray(Column[]::new));

    return new ResultWithExpressions(resultDataset,
        evaluatedAggs.stream().map(p -> p.bind(filteredDataset)).toList(),
        evaluatedGoupings.stream().map(p -> p.bind(filteredDataset)).toList(),
        evaluatedFilters
    );
  }

  @Nonnull
  private static List<String> toLabels(@Nonnull final List<ExpressionWithLabel> expressions) {
    return expressions.stream()
        .map(ExpressionWithLabel::getLabel).toList();
  }

  @Nonnull
  private static Stream<Column> alias(Column[] columns, List<String> labels) {
    return IntStream.range(0, columns.length)
        .mapToObj(i -> nonNull(labels.get(i))
                       ? columns[i].alias(requireNonNull(labels.get(i)))
                       : columns[i]
        );
  }

  @Value
  public static class ResultWithExpressions {

    @Nonnull
    Dataset<Row> dataset;

    @Nonnull
    List<EvaluatedPath> parsedAggregations;

    @Nonnull
    List<EvaluatedPath> parsedGroupings;

    @Nonnull
    List<EvaluatedPath> parsedFilters;

  }
}
